package mesosphere.marathon
package core.matcher.manager.impl

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import akka.event.LoggingReceive
import akka.pattern.pipe
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.matcher.base.OfferMatcher
import mesosphere.marathon.core.matcher.base.OfferMatcher.{ InstanceOpWithSource, MatchedInstanceOps }
import mesosphere.marathon.core.matcher.base.util.ActorOfferMatcher
import mesosphere.marathon.core.matcher.manager.OfferMatcherManagerConfig
import mesosphere.marathon.core.matcher.manager.impl.OfferMatcherManagerActor.{ MatchTimeout, OfferData }
import mesosphere.marathon.core.task.Task.LocalVolumeId
import mesosphere.marathon.metrics.Metrics.AtomicIntGauge
import mesosphere.marathon.metrics.{ MetricPrefixes, Metrics }
import mesosphere.marathon.state.{ PathId, Timestamp }
import mesosphere.marathon.stream._
import mesosphere.marathon.tasks.ResourceUtil
import org.apache.mesos.Protos.{ Offer, OfferID }
import rx.lang.scala.Observer

import scala.collection.immutable.Queue
import scala.util.Random
import scala.util.control.NonFatal

private[manager] class OfferMatcherManagerActorMetrics(metrics: Metrics) {
  private[manager] val launchTokenGauge: AtomicIntGauge =
    metrics.gauge(metrics.name(MetricPrefixes.SERVICE, getClass, "launchTokens"), new AtomicIntGauge)
  private[manager] val currentOffersGauge: AtomicIntGauge =
    metrics.gauge(metrics.name(MetricPrefixes.SERVICE, getClass, "currentOffers"), new AtomicIntGauge)
}

/**
  * This actor offers one interface to a dynamic collection of matchers
  * and includes logic for limiting the amount of launches.
  * 这个实例里，创建了两个case class对象：OfferData 和 MatchTimeout
  */
private[manager] object OfferMatcherManagerActor {
  def props(
    metrics: OfferMatcherManagerActorMetrics,
    random: Random, clock: Clock,
    offerMatcherConfig: OfferMatcherManagerConfig, offersWanted: Observer[Boolean]): Props = {
    Props(new OfferMatcherManagerActor(metrics, random, clock, offerMatcherConfig, offersWanted))
  }

  private case class OfferData(
      offer: Offer,
      deadline: Timestamp,
      sender: ActorRef,
      matcherQueue: Queue[OfferMatcher],
      ops: Seq[InstanceOpWithSource] = Seq.empty,
      matchPasses: Int = 0,
      resendThisOffer: Boolean = false) {

    def addMatcher(matcher: OfferMatcher): OfferData = copy(matcherQueue = matcherQueue.enqueue(matcher))
    def nextMatcherOpt: Option[(OfferMatcher, OfferData)] = {
      matcherQueue.dequeueOption map {
        case (nextMatcher, newQueue) => nextMatcher -> copy(matcherQueue = newQueue)
      }
    }

    def addInstances(added: Seq[InstanceOpWithSource]): OfferData = {
      val leftOverOffer = added.foldLeft(offer) { (offer, nextOp) => nextOp.op.applyToOffer(offer) }

      copy(
        offer = leftOverOffer,
        ops = added ++ ops
      )
    }
  }

  private case class MatchTimeout(offerId: OfferID)
}

private[impl] class OfferMatcherManagerActor private (
  metrics: OfferMatcherManagerActorMetrics,
  random: Random, clock: Clock, conf: OfferMatcherManagerConfig, offersWantedObserver: Observer[Boolean])
    extends Actor with ActorLogging {

  private[this] var launchTokens: Int = 0

  private[this] var matchers: Set[OfferMatcher] = Set.empty

  private[this] var offerQueues: Map[OfferID, OfferMatcherManagerActor.OfferData] = Map.empty

  override def receive: Receive = LoggingReceive {
    Seq[Receive](
      receiveSetLaunchTokens,
      receiveChangingMatchers,
      receiveProcessOffer,
      receiveMatchedInstances
    ).reduceLeft(_.orElse[Any, Unit](_))
  }

  private[this] def receiveSetLaunchTokens: Receive = {
    case OfferMatcherManagerDelegate.SetInstanceLaunchTokens(tokens) =>
      val tokensBeforeSet = launchTokens
      launchTokens = tokens
      metrics.launchTokenGauge.setValue(launchTokens)
      if (tokens > 0 && tokensBeforeSet <= 0)
        updateOffersWanted()
    case OfferMatcherManagerDelegate.AddInstanceLaunchTokens(tokens) =>
      launchTokens += tokens
      metrics.launchTokenGauge.setValue(launchTokens)
      if (tokens > 0 && launchTokens == tokens)
        updateOffersWanted()
  }

  private[this] def receiveChangingMatchers: Receive = {
    //添加或者更新Matcher
    case OfferMatcherManagerDelegate.AddOrUpdateMatcher(matcher) =>
      //首先，判断一下，ActorOfferMatcher 是否已经存在了, matcher 就是  ActorOfferMatcher
      if (!matchers(matcher)) {
        log.info("--->OfferMatcherManagerActor.scala<-----activating matcher {}.", matcher)
        offerQueues.mapValues(_.addMatcher(matcher))
        matchers += matcher
        updateOffersWanted()
      }

      sender() ! OfferMatcherManagerDelegate.MatcherAdded(matcher)

    // 移除 Matcher
    case OfferMatcherManagerDelegate.RemoveMatcher(matcher) =>
      if (matchers(matcher)) {
        //matcher 就是  ActorOfferMatcher
        log.info("----->OfferMatcherManagerActor.scala<------removing matcher {}", matcher)
        matchers -= matcher
        updateOffersWanted()
      }
      sender() ! OfferMatcherManagerDelegate.MatcherRemoved(matcher)
  }

  private[this] def offersWanted: Boolean = matchers.nonEmpty && launchTokens > 0
  private[this] def updateOffersWanted(): Unit = offersWantedObserver.onNext(offersWanted)

  private[impl] def offerMatchers(offer: Offer): Queue[OfferMatcher] = {
    //the persistence id of a volume encodes the app id
    //we use this information as filter criteria
    val appReservations: Set[PathId] = offer.getResourcesList.view
      .filter(r => r.hasDisk && r.getDisk.hasPersistence && r.getDisk.getPersistence.hasId)
      .map(_.getDisk.getPersistence.getId)
      .collect { case LocalVolumeId(volumeId) => volumeId.runSpecId }
      .toSet
    val (reserved, normal) = matchers.toSeq.partition(_.precedenceFor.exists(appReservations))
    //1 give the offer to the matcher waiting for a reservation
    //2 give the offer to anybody else
    //3 randomize both lists to be fair
    (random.shuffle(reserved) ++ random.shuffle(normal)).to[Queue]
  }

  private[this] def receiveProcessOffer: Receive = {
    case ActorOfferMatcher.MatchOffer(deadline, offer: Offer) if !offersWanted =>
      log.debug(s"Ignoring offer ${offer.getId.getValue}: No one interested.")
      sender() ! OfferMatcher.MatchedInstanceOps(offer.getId, resendThisOffer = false)

    case ActorOfferMatcher.MatchOffer(deadline, offer: Offer) =>
      log.debug(s"Start processing offer ${offer.getId.getValue}")

      // setup initial offer data
      val randomizedMatchers = offerMatchers(offer)
      val data = OfferMatcherManagerActor.OfferData(offer, deadline, sender(), randomizedMatchers)
      offerQueues += offer.getId -> data
      metrics.currentOffersGauge.setValue(offerQueues.size)

      // deal with the timeout
      import context.dispatcher
      context.system.scheduler.scheduleOnce(
        clock.now().until(deadline),
        self,
        MatchTimeout(offer.getId))

      // process offer for the first time
      scheduleNextMatcherOrFinish(data)
  }

  //接收到 匹配过的实例
  private[this] def receiveMatchedInstances: Receive = {
    case OfferMatcher.MatchedInstanceOps(offerId, addedOps, resendOffer) =>
     log.info("---->OfferMatcherManagerActor.scala<----offerID----:\t" + offerId)
     log.info("---->OfferMatcherManagerActor.scala<----addedOps----:\n" )
      addedOps.foreach(x => println("----> " + x.toString))
     log.info("---->OfferMatcherManagerActor.scala<----resendOffer----:\t" + resendOffer)

      //添加实例 进程
      def processAddedInstances(data: OfferData): OfferData = {
        val dataWithInstances = try {
          val (acceptedOps, rejectedOps) =
            addedOps.splitAt(Seq(launchTokens, addedOps.size, conf.maxInstancesPerOffer() - data.ops.size).min)

          rejectedOps.foreach(_.reject("not enough launch tokens OR already scheduled sufficient instances on offer"))

          val newData: OfferData = data.addInstances(acceptedOps)
          launchTokens -= acceptedOps.size
          metrics.launchTokenGauge.setValue(launchTokens)
          newData
        } catch {
          case NonFatal(e) =>
            log.error(s"unexpected error processing ops for ${offerId.getValue} from ${sender()}", e)
            data
        }

        dataWithInstances.nextMatcherOpt match {
          case Some((matcher, contData)) =>
            val contDataWithActiveMatcher =
              if (addedOps.nonEmpty) contData.addMatcher(matcher)
              else contData
            offerQueues += offerId -> contDataWithActiveMatcher
            contDataWithActiveMatcher
          case None =>
            log.warning(s"Got unexpected matched ops from ${sender()}: $addedOps")
            dataWithInstances
        }
      }

      //offerQueues 提供的资源队列
      offerQueues.get(offerId) match {
        case Some(data) =>
          val resend = data.resendThisOffer | resendOffer
          val nextData = processAddedInstances(data.copy(matchPasses = data.matchPasses + 1, resendThisOffer = resend))
          //调度下一个
          scheduleNextMatcherOrFinish(nextData)

        case None =>
          addedOps.foreach(_.reject(s"offer '${offerId.getValue}' already timed out"))
      }

    case MatchTimeout(offerId) =>
      // When the timeout is reached, we will answer with all matching instances we found until then.
      // Since we cannot be sure if we found all matching instances, we set resendThisOffer to true.
      offerQueues.get(offerId).foreach(sendMatchResult(_, resendThisOffer = true))
  }

  private[this] def scheduleNextMatcherOrFinish(data: OfferData): Unit = {
    val nextMatcherOpt = if (data.deadline < clock.now()) {
      log.warning(s"Deadline for ${data.offer.getId.getValue} overdue. Scheduled ${data.ops.size} ops so far.")
      None
    } else if (data.ops.size >= conf.maxInstancesPerOffer()) {
      log.info(
        s"Already scheduled the maximum number of ${data.ops.size} instances on this offer. " +
          s"Increase with --${conf.maxInstancesPerOfferFlag.name}.")
      None
    } else if (launchTokens <= 0) {
      log.info(
        s"No launch tokens left for ${data.offer.getId.getValue}. " +
          "Tune with --launch_tokens/launch_token_refresh_interval.")
      None
    } else {
      log.info("----->OfferMatcherManagerActor.scala<-------scheduleNextMatcherOrFinish---------\n" + data.nextMatcherOpt)
      data.nextMatcherOpt
    }

    nextMatcherOpt match {
      case Some((nextMatcher, newData)) =>
        import context.dispatcher
        log.debug(s"query next offer matcher $nextMatcher for offer id ${data.offer.getId.getValue}")
        nextMatcher
          .matchOffer(newData.deadline, newData.offer)
          .recover {
            case NonFatal(e) =>
              log.warning("Received error from {}", e)
              MatchedInstanceOps(data.offer.getId, resendThisOffer = true)
          }.pipeTo(self)
      case None => sendMatchResult(data, data.resendThisOffer)
    }
  }

  // 发送匹配结果
  private[this] def sendMatchResult(data: OfferData, resendThisOffer: Boolean): Unit = {
    log.info("----->OfferMatcherManagerActor.scala<----resendThisOffer--是否重复发送--:\t" + resendThisOffer)
    //data.sender这个actor 指的是--->OfferMatcherManagerActor
    data.sender ! OfferMatcher.MatchedInstanceOps(data.offer.getId, data.ops, resendThisOffer)

    offerQueues.foreach(x => println("id:\t" + x._1 + " ------> " + "value:\t" + x._2.nextMatcherOpt))

    offerQueues -= data.offer.getId
    log.info("-----<OfferMatcherManagerActor.scala>----提供资源队列大小----size----:\t" + offerQueues.size)
    metrics.currentOffersGauge.setValue(offerQueues.size)
    val maxRanges = if (log.isDebugEnabled) 1000 else 10
    log.info("----->OfferMatcherManagerActor.scala<--------maxRanges----:\t" + maxRanges)
    log.info(s"----->OfferMatcherManagerActor.scala<--------Finished processing ${data.offer.getId.getValue} from ${data.offer.getHostname}. " +
      s"Matched ${data.ops.size} ops after ${data.matchPasses} passes. " +
      s"${ResourceUtil.displayResources(data.offer.getResourcesList.to[Seq], maxRanges)} left.")
  }
}

