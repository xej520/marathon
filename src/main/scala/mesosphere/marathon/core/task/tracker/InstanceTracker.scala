package mesosphere.marathon
package core.task.tracker

import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.state.PathId
import org.slf4j.LoggerFactory

import scala.concurrent.{ ExecutionContext, Future }

/**
  * task 跟踪器，暴露task最新的已知状态
  * The TaskTracker exposes the latest known state for every task.
  *
  * It is an read-only interface. For modification, see
  * * [[TaskStateOpProcessor]] for create, update, delete operations
  *
  * FIXME: To allow introducing the new asynchronous [[InstanceTracker]] without needing to
  * refactor //重构 a lot of code at once, synchronous[同步] methods are still available but should be
  * avoided in new code.
  */
trait InstanceTracker {

  def specInstancesLaunchedSync(pathId: PathId): Seq[Instance]
  def specInstancesSync(pathId: PathId): Seq[Instance]
  def specInstances(pathId: PathId)(implicit ec: ExecutionContext): Future[Seq[Instance]]

  def instance(instanceId: Instance.Id): Future[Option[Instance]]

  def instancesBySpecSync: InstanceTracker.InstancesBySpec
  def instancesBySpec()(implicit ec: ExecutionContext): Future[InstanceTracker.InstancesBySpec]

  def countLaunchedSpecInstancesSync(appId: PathId): Int
  def countLaunchedSpecInstancesSync(appId: PathId, filter: Instance => Boolean): Int
  def countSpecInstancesSync(appId: PathId): Int
  def countSpecInstancesSync(appId: PathId, filter: Instance => Boolean): Int
  def countSpecInstances(appId: PathId)(implicit ec: ExecutionContext): Future[Int]

  def hasSpecInstancesSync(appId: PathId): Boolean
  def hasSpecInstances(appId: PathId)(implicit ec: ExecutionContext): Future[Boolean]
}

object InstanceTracker {
  //这里面，定义了两个伴生对象啊
  //第一 个是InstancesBySpec
  //第2 个是
  /**
    * Contains all tasks grouped by app ID.
    * 根据传入的appId,来获取“这个组的”所有的tasks
    */
  case class InstancesBySpec private (instancesMap: Map[PathId, InstanceTracker.SpecInstances]) {
    import InstancesBySpec._

    def allSpecIdsWithInstances: Set[PathId] = instancesMap.keySet

    def hasSpecInstances(appId: PathId): Boolean = instancesMap.contains(appId)

    def specInstances(pathId: PathId): Seq[Instance] = {
      instancesMap.get(pathId).map(_.instances).getOrElse(Seq.empty)
    }

    def instance(instanceId: Instance.Id): Option[Instance] = for {
      runSpec <- instancesMap.get(instanceId.runSpecId)
      instance <- runSpec.instanceMap.get(instanceId)
    } yield instance

    // TODO(PODS): the instanceTracker should not expose a def for tasks
    def task(id: Task.Id): Option[Task] = {
      val instances: Option[Instance] = instance(id.instanceId)
      instances.flatMap(_.tasksMap.get(id))
    }

    def allInstances: Seq[Instance] = instancesMap.values.flatMap(_.instances)(collection.breakOut)

    private[tracker] def updateApp(appId: PathId)(
      update: InstanceTracker.SpecInstances => InstanceTracker.SpecInstances): InstancesBySpec = {
      val updated = update(instancesMap(appId))
      if (updated.isEmpty) {
        log.info(s"Removed app [$appId] from tracker")
        copy(instancesMap = instancesMap - appId)
      } else {
        log.debug(s"Updated app [$appId], currently ${updated.instanceMap.size} tasks in total.")
        copy(instancesMap = instancesMap + (appId -> updated))
      }
    }
  }

  object InstancesBySpec {
    private val log = LoggerFactory.getLogger(getClass)

    def of(specInstances: collection.immutable.Map[PathId, InstanceTracker.SpecInstances]): InstancesBySpec = {
      new InstancesBySpec(specInstances.withDefault(appId => InstanceTracker.SpecInstances(appId)))
    }

    def of(apps: InstanceTracker.SpecInstances*): InstancesBySpec = of(Map(apps.map(app => app.specId -> app): _*))

    def forInstances(tasks: Instance*): InstancesBySpec = of(
      tasks.groupBy(_.runSpecId).map { case (appId, appTasks) => appId -> SpecInstances.forInstances(appId, appTasks.to[Seq]) }
    )

    def empty: InstancesBySpec = of(collection.immutable.Map.empty[PathId, InstanceTracker.SpecInstances])
  }
  /**
    * Contains only the tasks of the app with the given app ID.
    * 只获取 某一个app的tasks，并不是整个组的哦
    * @param specId The id of the app.
    * @param instanceMap The tasks of this app by task ID. FIXME: change keys to Task.TaskID
    */
  case class SpecInstances(specId: PathId, instanceMap: Map[Instance.Id, Instance] = Map.empty) {
    println(s"----<InstanceTracker.scala>----specId:${specId},-----instanceMap:\t${instanceMap}")
    def isEmpty: Boolean = instanceMap.isEmpty
    def contains(taskId: Instance.Id): Boolean = instanceMap.contains(taskId)
    def instances: Seq[Instance] = instanceMap.values.to[Seq]

    private[tracker] def withInstance(instance: Instance): SpecInstances =
      copy(instanceMap = instanceMap + (instance.instanceId -> instance))

    private[tracker] def withoutInstance(instanceId: Instance.Id): SpecInstances =
      copy(instanceMap = instanceMap - instanceId)
  }

  object SpecInstances {
    def forInstances(pathId: PathId, instances: Seq[Instance]): SpecInstances =
      SpecInstances(pathId, instances.map(instance => instance.instanceId -> instance)(collection.breakOut))
  }
}
