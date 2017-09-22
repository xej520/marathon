package mesosphere.marathon.core.task.update.impl.steps

//scalastyle:off
import javax.inject.Named

import akka.Done
import akka.actor.ActorRef
import com.google.inject.{Inject, Provider}
import mesosphere.marathon.MarathonSchedulerActor.ScaleRunSpec
import mesosphere.marathon.core.condition.Condition
import mesosphere.marathon.core.instance.update.{InstanceChange, InstanceChangeHandler}
import org.slf4j.LoggerFactory

import scala.concurrent.Future
//scalastyle:on
/**
  * 如果task出现下面的情况，会重新缩容app
  * 1、某一个task死掉
  * 2、task 超时
  * Trigger rescale of affected app if a task died or a reserved task timed out.
  * reserved 保留，预留，过时
  */
class ScaleAppUpdateStepImpl @Inject()(
                                        @Named("schedulerActor") schedulerActorProvider: Provider[ActorRef]) extends InstanceChangeHandler {

  private[this] val log = LoggerFactory.getLogger(getClass)
  private[this] lazy val schedulerActor = schedulerActorProvider.get()

  private[this] def scalingWorthy: Condition => Boolean = {
    case Condition.Reserved | Condition.UnreachableInactive | _: Condition.Terminal => true
    case _ => false
  }

  override def name: String = "scaleApp"

  override def process(update: InstanceChange): Future[Done] = {
    // TODO(PODS): it should be up to a tbd TaskUnreachableBehavior how to handle Unreachable
    calcScaleEvent(update).foreach(event => schedulerActor ! event)
    Future.successful(Done)
  }

  // app 进行缩容时 会调用
  def calcScaleEvent(update: InstanceChange): Option[ScaleRunSpec] = {
    //forall判断集合里所有的元素，是否都满足给定的条件
    if (scalingWorthy(update.condition) && update.lastState.forall(lastState => !scalingWorthy(lastState.condition))) {
      val runSpecId = update.runSpecId
      val instanceId = update.id
      val state = update.condition
      //state 的值 包括以下几种情况 Reserved、Killed
      //runSpecId : 就是appId,如/ftp/lgy008c
      //instanceId: 就是 instance [ftp_lgy008c.marathon-92f839d4-9c4f-11e7-9ec8-000c2930224d]
      //state： 就是Killed
      log.info(s"----->ScaleAppUpdateStepImpl.scala<-----initiating a scale check for runSpec [$runSpecId] due to [$instanceId] $state")
      // TODO(PODS): we should rename the Message and make the SchedulerActor generic
      //ScaleRunSpec(runSpecId) 就是 ScaleRunSpec(/ftp/xej002)
      log.info("------->ScaleAppUpdateStepImpl.scala<-----ScaleRunSpec(runSpecId)----:\t" + ScaleRunSpec(runSpecId))
      Some(ScaleRunSpec(runSpecId))
    } else {
      None
    }
  }
}
