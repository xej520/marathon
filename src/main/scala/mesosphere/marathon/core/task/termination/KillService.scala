package mesosphere.marathon
package core.task.termination

import akka.Done
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.task.Task

import scala.concurrent.Future

/**
  * 服务，处理删除掉task的服务
  *
  * 针对丢失的任务，需要额外的逻辑
  * A service that handles killing tasks. This will take care about extra logic for lost tasks,
  * 使用重试策略，从而减少向mesos发送的请求
  * apply a retry strategy and throttle kill requests to Mesos.
  */
trait KillService {
  /**
    * Kill the given tasks and return a future that is completed when all of the tasks
    * have been reported as terminal.
    *
    * @param tasks the tasks that shall be killed.
    * @param reason the reason why the task shall be killed.
    * @return a future that is completed when all tasks are killed.
    */
  def killInstances(tasks: Seq[Instance], reason: KillReason): Future[Done]

  /**
    * 将杀死的task，扔进一个队列里，
    * Kill the given task. The implementation should add the task onto
    * a queue that is processed short term and will eventually kill the task.
    *
    * @param task the task that shall be killed.
    * @param reason the reason why the task shall be killed.
    * @return a future that is completed when all tasks are killed.
    */
  def killInstance(task: Instance, reason: KillReason): Future[Done]

  /**
    * Kill the given unknown task by ID and do not try to fetch its state
    * upfront. Only use this when it is certain that the task is unknown.
    *
    * @param taskId the id of the task that shall be killed.
    * @param reason the reason why the task shall be killed.
    */
  def killUnknownTask(taskId: Task.Id, reason: KillReason): Unit
}
