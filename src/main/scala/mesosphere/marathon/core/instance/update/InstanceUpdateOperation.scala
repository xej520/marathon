package mesosphere.marathon.core.instance.update

import mesosphere.marathon.core.condition.Condition
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.task.{ TaskCondition, Task }
import mesosphere.marathon.state.Timestamp
import org.apache.mesos

import scala.collection.immutable.Seq

sealed trait InstanceUpdateOperation {
  def instanceId: Instance.Id
  /**
    * The possible task state if processing the state op succeeds. If processing the
    * state op fails, this state will never be persisted, so be cautious when using it.
    */
  def possibleNewState: Option[Instance] = None
}

object InstanceUpdateOperation {
  /** Launch (aka create) an ephemeral task*/
  case class LaunchEphemeral(instance: Instance) extends InstanceUpdateOperation {
    override def instanceId: Instance.Id = instance.instanceId
    override def possibleNewState: Option[Instance] = Some(instance)
  }

  /** Revert a task to the given state. Used in case TaskOps are rejected. */
  case class Revert(instance: Instance) extends InstanceUpdateOperation {
    override def instanceId: Instance.Id = instance.instanceId
    override def possibleNewState: Option[Instance] = Some(instance)
  }

  // TODO(PODS): this should work on an instance, not a task
  case class Reserve(instance: Instance) extends InstanceUpdateOperation {
    override def instanceId: Instance.Id = instance.instanceId
    override def possibleNewState: Option[Instance] = Some(instance)
  }

  case class LaunchOnReservation(
    instanceId: Instance.Id,
    runSpecVersion: Timestamp,
    timestamp: Timestamp,
    status: Task.Status, // TODO(PODS): the taskStatus must be created for each task and not passed in here
    hostPorts: Seq[Int]) extends InstanceUpdateOperation

  /**
    * Describes an instance update.
    *
    * @param instance Instance that is updated
    * @param condition New Condition of instance
    * @param mesosStatus New Mesos status
    * @param now Time when update was received
    */
  case class MesosUpdate(
      instance: Instance, condition: Condition,
      mesosStatus: mesos.Protos.TaskStatus, now: Timestamp) extends InstanceUpdateOperation {

    override def instanceId: Instance.Id = instance.instanceId
  }

  object MesosUpdate {
    def apply(instance: Instance, mesosStatus: mesos.Protos.TaskStatus, now: Timestamp): MesosUpdate = {
      MesosUpdate(instance, TaskCondition(mesosStatus), mesosStatus, now)
    }
  }

  case class ReservationTimeout(instanceId: Instance.Id) extends InstanceUpdateOperation

  /** Expunge a task whose TaskOp was rejected */
  case class ForceExpunge(instanceId: Instance.Id) extends InstanceUpdateOperation
}
