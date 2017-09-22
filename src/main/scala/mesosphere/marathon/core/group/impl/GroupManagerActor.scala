package mesosphere.marathon
package core.group.impl

import java.net.URL
import javax.inject.Provider

import akka.actor.{ Actor, Props }
import akka.event.EventStream
import akka.pattern.pipe
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import mesosphere.marathon.api.v2.Validation._
import mesosphere.marathon.core.event.{ GroupChangeFailed, GroupChangeSuccess }
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.pod.PodDefinition
import mesosphere.marathon.io.PathFun
import mesosphere.marathon.io.storage.StorageProvider
import mesosphere.marathon.state.{ AppDefinition, PortDefinition, _ }
import mesosphere.marathon.storage.repository.GroupRepository
import mesosphere.marathon.stream._
import mesosphere.marathon.upgrade.{ DeploymentPlan, GroupVersioningUtil, ResolveArtifacts }
import mesosphere.marathon.util.WorkQueue
import org.slf4j.LoggerFactory

import scala.collection.immutable.Seq
import scala.collection.mutable
import scala.concurrent.Future
import scala.util.{ Failure, Success }

private[group] object GroupManagerActor {
  sealed trait Request

  // Replies with Option[RunSpec]
  case class GetRunSpecWithId(id: PathId) extends Request

  // Replies with Option[AppDefinition]
  case class GetAppWithId(id: PathId) extends Request

  // Replies with Option[PodDefinition]
  case class GetPodWithId(id: PathId) extends Request

  // Replies with Option[Group]
  case class GetGroupWithId(id: PathId) extends Request

  // Replies with Option[Group]
  case class GetGroupWithVersion(id: PathId, version: Timestamp) extends Request

  // Replies with RootGroup
  case object GetRootGroup extends Request

  //回复 部署计划
  // Replies with DeploymentPlan
  case class GetUpgrade(
    gid: PathId,
    change: RootGroup => RootGroup,
    version: Timestamp = Timestamp.now(),
    force: Boolean = false,
    toKill: Map[PathId, Seq[Instance]] = Map.empty) extends Request

  // Replies with Seq[Timestamp]
  case class GetAllVersions(id: PathId) extends Request

  def props(
    serializeUpdates: WorkQueue,
    scheduler: Provider[DeploymentService],
    groupRepo: GroupRepository,
    storage: StorageProvider,
    config: MarathonConf,
    eventBus: EventStream)(implicit mat: Materializer): Props = {
    Props(new GroupManagerActor(
      serializeUpdates,
      scheduler,
      groupRepo,
      storage,
      config,
      eventBus))
  }
}

//private[impl] 此类GroupManagerActor只是在impl包下，可以用。
//其他包下，无法使用的
private[impl] class GroupManagerActor(
    serializeUpdates: WorkQueue,
    // a Provider has to be used to resolve a cyclic dependency between CoreModule and MarathonModule.
    // Once MarathonSchedulerService is in CoreModule, the Provider could be removed
    schedulerProvider: Provider[DeploymentService],
    groupRepo: GroupRepository,
    storage: StorageProvider,
    config: MarathonConf,
    eventBus: EventStream)(implicit mat: Materializer) extends Actor with PathFun {
  import GroupManagerActor._
  import context.dispatcher

  private[this] val log = LoggerFactory.getLogger(getClass.getName)
  private var scheduler: DeploymentService = _

  //
  override def preStart(): Unit = {
    super.preStart()
    log.info("-----------------GroupManagerActor-------preStart-------------")
    scheduler = schedulerProvider.get()
  }

  //actor自己，会不断运行这个
  override def receive: Receive = {
    case GetRunSpecWithId(id) => {
      log.info("---------<GroupManagerActor.scala>---GroupManagerActor-----GetRunSpecWithId(id):\t" + id)
      log.info("---------<GroupManagerActor.scala>---GroupManagerActor-----getRunSpec(id):\t" + getRunSpec(id))
      getRunSpec(id).pipeTo(sender())
    }
      //marathon 自己有定时器，大概每隔5秒钟，会执行下面的分支
    case GetAppWithId(id) => {
      log.info("------------------------------------------1----------------------------------------------")
      log.info("---------<GroupManagerActor.scala>---GroupManagerActor-----GetAppWithId(id):\t" + id)
      log.info("---------<GroupManagerActor.scala>---GroupManagerActor-----getApp(id):\t" + getApp(id))
      log.info("------------------------------------------2----------------------------------------------")
      getApp(id).pipeTo(sender())
    }
    case GetPodWithId(id) =>{
      log.info("---------<GroupManagerActor.scala>---GroupManagerActor-----GetRunSpecWithId(id):\t" + id)
      getPod(id).pipeTo(sender())
    }
    case GetRootGroup => {
      log.info("---------<GroupManagerActor.scala>---GroupManagerActor-----GetRootGroup(id)")
      groupRepo.root().pipeTo(sender())
    }
    case GetGroupWithId(id) => {
      log.info("---------<GroupManagerActor.scala>---GroupManagerActor-----GetGroupWithId(id):\t" + id)
      getGroupWithId(id).pipeTo(sender())
    }
    case GetGroupWithVersion(id, version) => {
      log.info("---------<GroupManagerActor.scala>---GroupManagerActor-----GetGroupWithVersion(id):\t" + id)
      getGroupWithVersion(id, version).pipeTo(sender())
    }
      //gid: 就是组ID号如/ftp, appID号: /ftp/lgy001, 那么gid就是/ftp
      //version 就是当前时间new date
      //force boolean类型 false
      //toKill是一个map集合，Map[pathID, Seq] 其实，pathID 就是appID, /ftp/lgy001,  如 Map(/ftp/xej002 -> List())
    case GetUpgrade(gid, change, version, force, toKill) => {
        log.info(s"---------<GroupManagerActor.scala>---gid:\t{}\n---change: {}\n----version:{}\n----force:{}\n----toKill:{}\n", gid, change,version,force,toKill)
        log.info("---------<GroupManagerActor.scala>---sender()----:\t" + sender().getClass)
        getUpgrade(gid, change, version, force, toKill).pipeTo(sender())
      }
    case GetAllVersions(id) => {
      log.info("---------<GroupManagerActor.scala>---GroupManagerActor-----GetAllVersions(id):\t" + id)
      getVersions(id).pipeTo(sender())
    }
  }

  private[this] def getRunSpec(id: PathId): Future[Option[RunSpec]] = {
    groupRepo.root().map { root =>
      root.app(id).orElse(root.pod(id))
    }
  }

  private[this] def getApp(id: PathId): Future[Option[AppDefinition]] = {
    groupRepo.root().map(_.app(id))
  }

  private[this] def getPod(id: PathId): Future[Option[PodDefinition]] = {
    groupRepo.root().map(_.pod(id))
  }

  private[this] def getGroupWithId(id: PathId): Future[Option[Group]] = {
    groupRepo.root().map(_.group(id))
  }

  private[this] def getGroupWithVersion(id: PathId, version: Timestamp): Future[Option[Group]] = {
    groupRepo.rootVersion(version.toOffsetDateTime).map {
      _.flatMap(_.group(id))
    }
  }

  private[this] def getUpgrade(
    gid: PathId,
    change: RootGroup => RootGroup,
    version: Timestamp,
    force: Boolean,
    toKill: Map[PathId, Seq[Instance]]): Future[DeploymentPlan] = {
    serializeUpdates {
      log.info(s"---------<GroupManagerActor.scala>-----Upgrade root group version:$version with force:$force")
      //声明一个变量
      val deployment = for {
        from <- groupRepo.root()
        (toUnversioned, resolve) <- resolveStoreUrls(assignDynamicServicePorts(from, change(from)))
        to = GroupVersioningUtil.updateVersionInfoForChangedApps(version, from, toUnversioned)
        _ = validateOrThrow(to)(RootGroup.valid(config.availableFeatures))
      //创建一个DeploymentPlan对象，也就是实例
      //plan包括：DeploymentID号，step1, step2
        plan = DeploymentPlan(from, to, resolve, version, toKill)
        _ = validateOrThrow(plan)(DeploymentPlan.deploymentPlanValidator(config))
        _ = log.info(s"------->GroupManagerActor.scala<-------Computed new deployment plan:\n$plan")
        _ <- groupRepo.storeRootVersion(plan.target, plan.createdOrUpdatedApps, plan.createdOrUpdatedPods)
        _ <- scheduler.deploy(plan, force)
        _ <- groupRepo.storeRoot(plan.target, plan.createdOrUpdatedApps,
          plan.deletedApps, plan.createdOrUpdatedPods, plan.deletedPods)
      //Deployment阶段后，才开始，执行下面的语句 1
        _ = log.info(s"------->GroupManagerActor.scala<-------Updated groups/apps/pods according to deployment plan ${plan.id}")
      } yield plan

      deployment.onComplete {
        case Success(plan) =>
          //Deployment阶段后，才开始，执行下面的语句 2
          log.info(s"------->GroupManagerActor.scala<------Deployment acknowledged. Waiting to get processed:\n$plan")
          eventBus.publish(GroupChangeSuccess(gid, version.toString))
        case Failure(ex: AccessDeniedException) =>
        // If the request was not authorized, we should not publish an event
        case Failure(ex) =>
          log.warn(s"Deployment failed for change: $version", ex)
          eventBus.publish(GroupChangeFailed(gid, version.toString, ex.getMessage))
      }
      deployment
    }
  }

  private[this] def getVersions(id: PathId): Future[Seq[Timestamp]] = {
    groupRepo.rootVersions().runWith(Sink.seq).flatMap { versions =>
      Future.sequence(versions.map(groupRepo.rootVersion)).map {
        _.collect {
          case Some(group) if group.group(id).isDefined => group.version
        }
      }
    }
  }

  private[this] def resolveStoreUrls(rootGroup: RootGroup): Future[(RootGroup, Seq[ResolveArtifacts])] = {
    def url2Path(url: String): Future[(String, String)] = contentPath(new URL(url)).map(url -> _)
    Future.sequence(rootGroup.transitiveApps.flatMap(_.storeUrls).map(url2Path))
      .map(_.toMap)
      .map { paths =>
        //Filter out all items with already existing path.
        //Since the path is derived from the content itself,
        //it will only change, if the content changes.
        val downloads = mutable.Map(paths.filterNotAs { case (url, path) => storage.item(path).exists }(collection.breakOut): _*)
        val actions = Seq.newBuilder[ResolveArtifacts]
        rootGroup.updateTransitiveApps(
          PathId.empty,
          app =>
            if (app.storeUrls.isEmpty) app
            else {
              val storageUrls = app.storeUrls.map(paths).map(storage.item(_).url)
              val resolved = app.copy(fetch = app.fetch ++ storageUrls.map(FetchUri.apply(_)), storeUrls = Seq.empty)
              val appDownloads: Map[URL, String] =
                app.storeUrls
                  .flatMap { url => downloads.remove(url).map { path => new URL(url) -> path } }(collection.breakOut)
              if (appDownloads.nonEmpty) actions += ResolveArtifacts(resolved, appDownloads)
              resolved
            }, rootGroup.version) -> actions.result()
      }
  }

  private[impl] def assignDynamicServicePorts(from: RootGroup, to: RootGroup): RootGroup = {
    val portRange = Range(config.localPortMin(), config.localPortMax())
    var taken = from.transitiveApps.flatMap(_.servicePorts) ++ to.transitiveApps.flatMap(_.servicePorts)

    def nextGlobalFreePort: Int = {
      val port = portRange.find(!taken.contains(_))
        .getOrElse(throw new PortRangeExhaustedException(
          config.localPortMin(),
          config.localPortMax()
        ))
      log.info(s"Take next configured free port: $port")
      taken += port
      port
    }

    def mergeServicePortsAndPortDefinitions(
      portDefinitions: Seq[PortDefinition],
      servicePorts: Seq[Int]): Seq[PortDefinition] =
      if (portDefinitions.nonEmpty)
        portDefinitions.zipAll(servicePorts, AppDefinition.RandomPortDefinition, AppDefinition.RandomPortValue).map {
          case (portDefinition, servicePort) => portDefinition.copy(port = servicePort)
        }
      else Seq.empty

    def assignPorts(app: AppDefinition): AppDefinition = {
      //all ports that are already assigned in old app definition, but not used in the new definition
      //if the app uses dynamic ports (0), it will get always the same ports assigned
      val assignedAndAvailable = mutable.Queue(
        from.app(app.id)
          .map(_.servicePorts.filter(p => portRange.contains(p) && !app.servicePorts.contains(p)))
          .getOrElse(Nil): _*
      )

      def nextFreeServicePort: Int =
        if (assignedAndAvailable.nonEmpty) assignedAndAvailable.dequeue()
        else nextGlobalFreePort

      val servicePorts: Seq[Int] = app.servicePorts.map { port =>
        if (port == 0) nextFreeServicePort else port
      }

      // TODO(portMappings) this should apply for multiple container types
      // defined only if there are port mappings
      val newContainer = app.container.flatMap { container =>
        container.docker.map { docker =>
          val newMappings = docker.portMappings.zip(servicePorts).map {
            case (portMapping, servicePort) => portMapping.copy(servicePort = servicePort)
          }
          docker.copy(portMappings = newMappings)
        }
      }

      app.copy(
        portDefinitions = mergeServicePortsAndPortDefinitions(app.portDefinitions, servicePorts),
        container = newContainer.orElse(app.container)
      )
    }

    val dynamicApps: Set[AppDefinition] =
      to.transitiveApps.map {
        // assign values for service ports that the user has left "blank" (set to zero)
        case app: AppDefinition if app.hasDynamicServicePorts => assignPorts(app)
        case app: AppDefinition =>
          // Always set the ports to service ports, even if we do not have dynamic ports in our port mappings
          app.copy(
            portDefinitions = mergeServicePortsAndPortDefinitions(app.portDefinitions, app.servicePorts)
          )
      }

    dynamicApps.foldLeft(to) { (rootGroup, app) =>
      rootGroup.updateApp(app.id, _ => app, app.version)
    }
  }
}
