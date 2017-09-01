package mesosphere.marathon.core.task.update.impl

import com.typesafe.scalalogging.StrictLogging
import javax.inject.Inject

import akka.event.EventStream
import com.google.inject.name.Names
import mesosphere.marathon.MarathonSchedulerDriverHolder
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.condition.Condition
import mesosphere.marathon.core.event.UnknownInstanceTerminated
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.instance.update.InstanceUpdateOperation
import mesosphere.marathon.core.task.termination.{ KillReason, KillService }
import mesosphere.marathon.core.task.tracker.{ InstanceTracker, TaskStateOpProcessor }
import mesosphere.marathon.core.task.update.TaskStatusUpdateProcessor
import mesosphere.marathon.core.task.{ TaskCondition, Task }
import mesosphere.marathon.metrics.Metrics.Timer
import mesosphere.marathon.metrics.{ MetricPrefixes, Metrics }
import org.apache.mesos.{ Protos => MesosProtos }

import scala.concurrent.Future

/**
  * Executes the given TaskStatusUpdateSteps for every update.
  */
class TaskStatusUpdateProcessorImpl @Inject() (
    metrics: Metrics,
    clock: Clock,
    instanceTracker: InstanceTracker,
    stateOpProcessor: TaskStateOpProcessor,
    driverHolder: MarathonSchedulerDriverHolder,
    killService: KillService,
    eventStream: EventStream) extends TaskStatusUpdateProcessor with StrictLogging {
  import scala.concurrent.ExecutionContext.Implicits.global

  private[this] val publishTimer: Timer =
    metrics.timer(metrics.name(MetricPrefixes.SERVICE, getClass, "publishFuture"))

  private[this] val killUnknownTaskTimer: Timer =
    metrics.timer(metrics.name(MetricPrefixes.SERVICE, getClass, "killUnknownTask"))

  logger.info("Started status update processor")

  override def publish(status: MesosProtos.TaskStatus): Future[Unit] = publishTimer.timeFuture {
    logger.debug(s"Received status update\n${status}")
    logger.info(s"---->TaskStatusUpdateProcessorImpl.scala<------Received status update\n${status}")
    import TaskStatusUpdateProcessorImpl._

    // TODO: should be Timestamp.fromTaskStatus(status), but this breaks unit tests as there are invalid stubs
    val now = clock.now()
    val taskId = Task.Id(status.getTaskId)
    val taskCondition = TaskCondition(status)

    instanceTracker.instance(taskId.instanceId).flatMap {
      case Some(instance) =>
        // TODO(PODS): we might as well pass the taskCondition here
        val op = InstanceUpdateOperation.MesosUpdate(instance, status, now)
        stateOpProcessor.process(op).flatMap(_ => acknowledge(status))

      case None if terminalUnknown(taskCondition) =>
        logger.warn(s"Received terminal status update for unknown ${taskId}")
        eventStream.publish(UnknownInstanceTerminated(taskId.instanceId, taskId.runSpecId, taskCondition))
        acknowledge(status)

      case None if killWhenUnknown(taskCondition) =>
        killUnknownTaskTimer {
          logger.warn(s"Kill unknown ${taskId}")
          killService.killUnknownTask(taskId, KillReason.Unknown)
          acknowledge(status)
        }

      case maybeTask: Option[Instance] =>
        val taskStr = taskKnownOrNotStr(maybeTask)
        logger.info(s"Ignoring ${status.getState} update for $taskStr $taskId")
        acknowledge(status)
    }
  }

  private[this] def acknowledge(status: MesosProtos.TaskStatus): Future[Unit] = {
    //driverHoler.driver 就是  org.apache.mesos.MesosSchedulerDriver
    driverHolder.driver.foreach{ driver =>
      logger.info(s"------>TaskStatusUpdateProcessorImpl.scala<-----Acknowledge status update for task ${status.getTaskId.getValue}: ${status.getState} (${status.getMessage})")
      driver.acknowledgeStatusUpdate(status)
    }
    logger.info("----driverHolder.driver-----:\t" + driverHolder.driver.get)
    Future.successful(())
  }
}

object TaskStatusUpdateProcessorImpl {
  lazy val name = Names.named(getClass.getSimpleName)

  /** Matches all states that are considered terminal for an unknown task */
  def terminalUnknown(condition: Condition): Boolean = condition match {
    case t: Condition.Terminal => true
    case Condition.Unreachable => true
    case _ => false
  }

  // TODO(PODS): align this with similar extractors/functions
  private[this] val ignoreWhenUnknown = Set[Condition](
    Condition.Killed,
    Condition.Killing,
    Condition.Error,
    Condition.Failed,
    Condition.Finished,
    Condition.Unreachable,
    Condition.Gone,
    Condition.Dropped,
    Condition.Unknown
  )
  // It doesn't make sense to kill an unknown task if it is in a terminal or killing state
  // We'd only get another update for the same task
  private def killWhenUnknown(condition: Condition): Boolean = {
    !ignoreWhenUnknown.contains(condition)
  }

  private def taskKnownOrNotStr(maybeTask: Option[Instance]): String = if (maybeTask.isDefined) "known" else "unknown"
}
