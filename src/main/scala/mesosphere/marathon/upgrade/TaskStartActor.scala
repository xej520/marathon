
package mesosphere.marathon.upgrade

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import akka.event.EventStream
import mesosphere.marathon.core.event.DeploymentStatus
import mesosphere.marathon.core.launchqueue.LaunchQueue
import mesosphere.marathon.core.readiness.ReadinessCheckExecutor
import mesosphere.marathon.core.task.tracker.InstanceTracker
import mesosphere.marathon.state.RunSpec
import mesosphere.marathon.{ SchedulerActions, TaskUpgradeCanceledException }

import scala.concurrent.Promise

class TaskStartActor(
    val deploymentManager: ActorRef,
    val status: DeploymentStatus,
    val scheduler: SchedulerActions,
    val launchQueue: LaunchQueue,
    val instanceTracker: InstanceTracker,
    val eventBus: EventStream,
    val readinessCheckExecutor: ReadinessCheckExecutor,
    val runSpec: RunSpec,
    val scaleTo: Int,
    promise: Promise[Unit]) extends Actor with ActorLogging with StartingBehavior {

  val nrToStart: Int = Math.max(
    0,
    scaleTo - launchQueue.get(runSpec.id).map(_.finalInstanceCount)
      .getOrElse(instanceTracker.countLaunchedSpecInstancesSync(runSpec.id)))

  def initializeStart(): Unit = if (nrToStart > 0) launchQueue.add(runSpec, nrToStart)

  override def postStop(): Unit = {
    eventBus.unsubscribe(self)
    super.postStop()
  }

  // 成功
  override def success(): Unit = {
    log.info(s"----> TaskStartActor.scala<------Successfully started $nrToStart instances of ${runSpec.id}")
    promise.success(())
    context.stop(self)
  }

  override def shutdown(): Unit = {
    if (!promise.isCompleted)
      promise.tryFailure(
        new TaskUpgradeCanceledException(
          "The task upgrade has been cancelled"))
    context.stop(self)
  }
}

object TaskStartActor {
  @SuppressWarnings(Array("MaxParameters"))
  def props(
    deploymentManager: ActorRef,
    status: DeploymentStatus,
    scheduler: SchedulerActions,
    launchQueue: LaunchQueue,
    instanceTracker: InstanceTracker,
    eventBus: EventStream,
    readinessCheckExecutor: ReadinessCheckExecutor,
    runSpec: RunSpec,
    scaleTo: Int,
    promise: Promise[Unit]): Props = {
    Props(new TaskStartActor(deploymentManager, status, scheduler, launchQueue, instanceTracker,
      eventBus, readinessCheckExecutor, runSpec, scaleTo, promise)
    )
  }
}
