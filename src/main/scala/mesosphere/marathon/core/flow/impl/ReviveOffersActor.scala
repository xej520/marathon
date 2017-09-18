package mesosphere.marathon.core.flow.impl

import akka.actor.{Actor, ActorLogging, Cancellable, Props}
import akka.event.{EventStream, LoggingReceive}
import mesosphere.marathon.MarathonSchedulerDriverHolder
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.flow.ReviveOffersConfig
import mesosphere.marathon.core.flow.impl.ReviveOffersActor.OffersWanted
import mesosphere.marathon.core.event.{SchedulerRegisteredEvent, SchedulerReregisteredEvent}
import mesosphere.marathon.state.Timestamp
import rx.lang.scala.{Observable, Subscription}

import scala.annotation.tailrec
import scala.concurrent.duration._

private[flow] object ReviveOffersActor {
  def props(
    clock: Clock, conf: ReviveOffersConfig,
    marathonEventStream: EventStream,
    offersWanted: Observable[Boolean], driverHolder: MarathonSchedulerDriverHolder): Props = {
    Props(new ReviveOffersActor(clock, conf, marathonEventStream, offersWanted, driverHolder))
  }

  private[impl] case object TimedCheck
  private[impl] case class OffersWanted(wanted: Boolean)
}

/**
  * Revive offers whenever interest is signaled but maximally every 5 seconds.
  */
private[impl] class ReviveOffersActor(
    clock: Clock, conf: ReviveOffersConfig,
    marathonEventStream: EventStream,
    offersWanted: Observable[Boolean],
    driverHolder: MarathonSchedulerDriverHolder) extends Actor with ActorLogging {

  private[impl] var subscription: Subscription = _
  private[impl] var offersCurrentlyWanted: Boolean = false
  private[impl] var revivesNeeded: Int = 0
  private[impl] var lastRevive: Timestamp = Timestamp(0)
  private[impl] var nextReviveCancellableOpt: Option[Cancellable] = None

  override def preStart(): Unit = {
    subscription = offersWanted.map(OffersWanted).subscribe(self ! _)
    marathonEventStream.subscribe(self, classOf[SchedulerRegisteredEvent])
    marathonEventStream.subscribe(self, classOf[SchedulerReregisteredEvent])
  }

  override def postStop(): Unit = {
    subscription.unsubscribe()
    nextReviveCancellableOpt.foreach(_.cancel())
    nextReviveCancellableOpt = None
    marathonEventStream.unsubscribe(self)
  }

  @tailrec
  private[this] def reviveOffers(): Unit = {
    val now: Timestamp = clock.now()
    val nextRevive = lastRevive + conf.minReviveOffersInterval().milliseconds

    if (nextRevive <= now) {
      log.info("=> -------->ReviveOffersActor.scala>-------revive offers NOW, canceling any scheduled revives")
      //nextReviveCancellableOpt 容器，
      nextReviveCancellableOpt.foreach(_.cancel())
      //将nextReviveCancellableOpt 容器 注释掉
      nextReviveCancellableOpt = None


      driverHolder.driver.foreach(_.reviveOffers())
      lastRevive = now

      revivesNeeded -= 1
      if (revivesNeeded > 0) {
        log.info(
          "-------->ReviveOffersActor.scala>-------{} further revives still needed. Repeating reviveOffers according to --{} {}",
          revivesNeeded, conf.reviveOffersRepetitions.name, conf.reviveOffersRepetitions())
        reviveOffers()
      }
    } else {
      lazy val untilNextRevive = now until nextRevive
      if (nextReviveCancellableOpt.isEmpty) {
        log.info(
          "=> -------->ReviveOffersActor.scala>-------Schedule next revive at {} in {}, adhering to --{} {} (ms)",
          nextRevive, untilNextRevive, conf.minReviveOffersInterval.name, conf.minReviveOffersInterval())

        //下面这些，是启动marathon时的命令
        //Vector(--master, zk://master001:2181,master002:2181,master003:2181/mesos, --zk, zk://master001:2181,master002:2181,master003:2181/marathon,
        // --framework_name, marathon, --mesos_user, root, --mesos_role, marathon_role, --mesos_authentication_principal, marathon_user)
        log.info("-------->ReviveOffersActor.scala>-----conf-------------\n" + conf.args)

        nextReviveCancellableOpt = Some(schedulerCheck(untilNextRevive))
      } else if (log.isDebugEnabled) {
        log.info("=> Next revive already scheduled at {} not yet due for {}", nextRevive, untilNextRevive)
      }
    }
  }

  private[this] def suppressOffers(): Unit = {
    log.info("=> ----->ReviveOffersActor.scala<------Suppress offers NOW")
    driverHolder.driver.foreach(_.suppressOffers())
  }

  override def receive: Receive = LoggingReceive {
    Seq(
      receiveOffersWantedNotifications,
      receiveReviveOffersEvents
    ).reduceLeft[Receive](_.orElse[Any, Unit](_))
  }

  private[this] def receiveOffersWantedNotifications: Receive = {
    //创建task时，第一次会走这里的
    case OffersWanted(true) =>
      log.info("------>ReviveOffersActor.scala<---Received offers WANTED notification")
      offersCurrentlyWanted = true
      initiateNewSeriesOfRevives()

    case OffersWanted(false) =>
      log.info("------>ReviveOffersActor.scala<-----Received offers NOT WANTED notification, canceling {} revives", revivesNeeded)
      offersCurrentlyWanted = false
      revivesNeeded = 0
      nextReviveCancellableOpt.foreach(_.cancel())
      nextReviveCancellableOpt = None

      // When we don't want any more offers, we ask mesos to suppress
      // them. This alleviates load on the allocator, and acts as an
      // infinite duration filter for all agents until the next time
      // we call `Revive`.
      suppressOffers()
  }

  def initiateNewSeriesOfRevives(): Unit = {
    revivesNeeded = conf.reviveOffersRepetitions()
    reviveOffers()
  }

  private[this] def receiveReviveOffersEvents: Receive = {
    case msg @ (_: SchedulerRegisteredEvent | _: SchedulerReregisteredEvent | OfferReviverDelegate.ReviveOffers) =>

      if (offersCurrentlyWanted) {
        log.info(s"Received reviveOffers notification: ${msg.getClass.getSimpleName}")
        initiateNewSeriesOfRevives()
      } else {
        log.info(s"Ignoring ${msg.getClass.getSimpleName} because no one is currently interested in offers")
      }

    case ReviveOffersActor.TimedCheck =>
      log.info("Received TimedCheck")
      if (revivesNeeded > 0) {
        reviveOffers()
      } else {
        log.info("=> no revives needed right now")
      }
  }

  protected def schedulerCheck(duration: FiniteDuration): Cancellable = {
    import context.dispatcher
    context.system.scheduler.scheduleOnce(duration, self, ReviveOffersActor.TimedCheck)
  }
}
