package mesosphere.marathon.core.task.tracker

import mesosphere.marathon.core.instance.update.{ InstanceUpdateEffect, InstanceUpdateOperation }

import scala.concurrent.Future

/**
  * 处理InstanceUpdateOperations的进程，
  * Handles the processing of InstanceUpdateOperations. These might originate from
  * * Creating an instance//创建一个实例
  * * Updating an instance (due to a state change, a timeout, a mesos update)
  * * Expunging an instance//消除一个实例
  */
trait TaskStateOpProcessor {
  /** Process an InstanceUpdateOperation and propagate its result. */
  def process(stateOp: InstanceUpdateOperation): Future[InstanceUpdateEffect]
}
