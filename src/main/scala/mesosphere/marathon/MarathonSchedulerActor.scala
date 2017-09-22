package mesosphere.marathon

import akka.Done
import akka.actor._
import akka.event.{EventStream, LoggingReceive}
import akka.stream.Materializer
import mesosphere.marathon.MarathonSchedulerActor.ScaleRunSpec
import mesosphere.marathon.core.election.{ElectionService, LocalLeadershipEvent}
import mesosphere.marathon.core.event.{AppTerminatedEvent, DeploymentFailed, DeploymentSuccess}
import mesosphere.marathon.core.health.HealthCheckManager
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.instance.Instance.AgentInfo
import mesosphere.marathon.core.launchqueue.LaunchQueue
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.core.task.termination.{KillReason, KillService}
import mesosphere.marathon.core.task.tracker.InstanceTracker
import mesosphere.marathon.state.{PathId, RunSpec}
import mesosphere.marathon.storage.repository.GroupRepository
import mesosphere.marathon.stream._
import mesosphere.marathon.upgrade.DeploymentManager._
import mesosphere.marathon.upgrade.{DeploymentManager, DeploymentPlan, ScalingProposition}
import mesosphere.marathon.util._
import mesosphere.mesos.Constraints
import org.apache.mesos
import org.apache.mesos.Protos.{Status, TaskState}
import org.apache.mesos.SchedulerDriver
import org.slf4j.LoggerFactory

import scala.async.Async.{async, await}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

class LockingFailedException(msg: String) extends Exception(msg)

class MarathonSchedulerActor private(
                                      createSchedulerActions: ActorRef => SchedulerActions,
                                      deploymentManagerProps: SchedulerActions => Props,
                                      historyActorProps: Props,
                                      healthCheckManager: HealthCheckManager,
                                      killService: KillService,
                                      launchQueue: LaunchQueue,
                                      marathonSchedulerDriverHolder: MarathonSchedulerDriverHolder,
                                      electionService: ElectionService,
                                      eventBus: EventStream)(implicit val mat: Materializer) extends Actor
  with ActorLogging with Stash {

  import context.dispatcher
  import mesosphere.marathon.MarathonSchedulerActor._

  /**
    * About locks:
    * - a lock is acquired if deployment is started
    * - a lock is acquired if a kill operation is executed
    * - a lock is acquired if a scale operation is executed
    *
    * 如果deployment 处于运行状态，不允许有kill/scale操作的
    * This basically means:
    * - a kill/scale operation should not be performed, while a deployment is in progress
    * - a deployment should not be started, if a scale/kill operation is in progress
    * Since multiple conflicting deployment can be handled at the same time lockedRunSpecs saves
    * the lock count for each affected PathId. Lock is removed if lock count == 0.
    */
  // TODO (AD): DeploymentManager has already all the information about running deployments.
  // MarathonSchedulerActor should only save the locks resulting from scale and kill operations,
  // asking DeploymentManager for deployment locks.
  val lockedRunSpecs = collection.mutable.Map[PathId, Int]().withDefaultValue(0)
  var schedulerActions: SchedulerActions = _
  var deploymentManager: ActorRef = _
  var historyActor: ActorRef = _
  var activeReconciliation: Option[Future[Status]] = None

  // actor启动时，会先执行这个
  // 作用应该是，进行注册，选举之类的
  override def preStart(): Unit = {
    schedulerActions = createSchedulerActions(self)
    deploymentManager = context.actorOf(deploymentManagerProps(schedulerActions), "DeploymentManager")
    historyActor = context.actorOf(historyActorProps, "HistoryActor")

    electionService.subscribe(self)
  }

  override def postStop(): Unit = {
    electionService.unsubscribe(self)
  }

  def receive: Receive = suspended

  def suspended: Receive = LoggingReceive.withLabel("suspended") {
    case LocalLeadershipEvent.ElectedAsLeader =>
      log.info("------------------Starting scheduler actor------------------")
      deploymentManager ! LoadDeploymentsOnLeaderElection

    case LoadedDeploymentsOnLeaderElection(deployments) =>
      deployments.foreach { plan =>
        log.info(s"------------------Recovering deployment----------------------:\n$plan")
        deploy(context.system.deadLetters, Deploy(plan, force = false))
      }

      log.info("Scheduler actor ready")
      unstashAll()
      context.become(started)
      self ! ReconcileHealthChecks

    case LocalLeadershipEvent.Standby =>
    // ignored
    // FIXME: When we get this while recovering deployments, we become active anyway
    // and drop this message.

    case _ => stash()
  }

  // 统计一下，started一共有多少中情况，也就是说，一共有多少case情况哈
  //LocalLeadershipEvent.Standby
  //LocalLeadershipEvent.ElectedAsLeader
  //ReconcileTasks
  //ReconcileFinished
  //ReconcileHealthChecks
  //ScaleRunSpecs
  //CancelDeployment
  //Deploy(plan, force)
  //KillTasks(runSpecId, tasks)
  //DeploymentFinished(plan)
  //DeploymentManager.DeploymentFailed(plan, reason)
  //RunSpecScaled(id)
  //TasksKilled(runSpecId, _)
  //RetrieveRunningDeployments
  def started: Receive = LoggingReceive.withLabel("started") {
    case LocalLeadershipEvent.Standby =>
      log.info("-----------Suspending scheduler actor------------------")
      healthCheckManager.removeAll()
      deploymentManager ! ShutdownDeployments
      lockedRunSpecs.clear()
      context.become(suspended)

    case LocalLeadershipEvent.ElectedAsLeader => // ignore

    case ReconcileTasks =>
      import akka.pattern.pipe
      import context.dispatcher
      log.info("-----------ReconcileTasks------------------")
      val reconcileFuture = activeReconciliation match {
        //创建task时，activeReconciliation 初始值就是None,因此，会走case这个分支的
        case None =>
          //initiate 启动，开始
          //reconciliation 和解，调停; 一致; 服从，顺从; 和谐
          log.info("-----------initiate task reconciliation-----------")
          val newFuture = schedulerActions.reconcileTasks(driver)
          //重新赋值activeReconciliation
          activeReconciliation = Some(newFuture)
          newFuture.onFailure {
            case NonFatal(e) => log.error(e, "-----------error while reconciling tasks-----------")
          }

          newFuture
            // the self notification MUST happen before informing the initiator
            // if we want to ensure that we trigger a new reconciliation for
            // the first call after the last ReconcileTasks.answer has been received.
            .andThen { case _ => self ! ReconcileFinished }
        case Some(active) =>
          log.info("-------------------task reconciliation still active, reusing result------------")
          active
      }
      reconcileFuture.map(_ => ReconcileTasks.answer).pipeTo(sender)

    case ReconcileFinished =>
      log.info("task reconciliation has finished")
      activeReconciliation = None

    case ReconcileHealthChecks =>
      schedulerActions.reconcileHealthChecks()

    case ScaleRunSpecs => schedulerActions.scaleRunSpec()

    case cmd@ScaleRunSpec(runSpecId) =>
      log.debug("Receive scale run spec for {}", runSpecId)
      val origSender = sender()

      @SuppressWarnings(Array("all")) /* async/await */
      def scaleAndAnswer(): Done = {
        val res: Future[Done] = async {
          await(schedulerActions.scale(runSpecId))
          self ! cmd.answer
          Done
        }

        if (origSender != context.system.deadLetters) {
          res.asTry.onComplete {
            case Success(_) => origSender ! cmd.answer
            case Failure(t) => origSender ! CommandFailed(cmd, t)
          }
        }
        Done
      }

      withLockFor(runSpecId) {
        scaleAndAnswer()
      }

    case cmd: CancelDeployment =>
      deploymentManager forward cmd

    case cmd@Deploy(plan, force) =>
      deploy(sender(), cmd)

    case cmd@KillTasks(runSpecId, tasks) =>
      val origSender = sender()

      @SuppressWarnings(Array("all")) /* async/await */
      def killTasks(): Done = {
        log.debug("Received kill tasks {} of run spec {}", tasks, runSpecId)
        val res: Future[Done] = async {
          await(killService.killInstances(tasks, KillReason.KillingTasksViaApi))
          await(schedulerActions.scale(runSpecId))
          self ! cmd.answer
          Done
        }

        res.asTry.onComplete {
          case Success(_) => origSender ! cmd.answer
          case Failure(t) => origSender ! CommandFailed(cmd, t)
        }
        Done
      }

      withLockFor(runSpecId) {
        killTasks()
      }

    case DeploymentFinished(plan) =>
      removeLocks(plan.affectedRunSpecIds)
      deploymentSuccess(plan)

    case DeploymentManager.DeploymentFailed(plan, reason) =>
      removeLocks(plan.affectedRunSpecIds)
      deploymentFailed(plan, reason)

    case RunSpecScaled(id) => removeLock(id)

    case TasksKilled(runSpecId, _) => removeLock(runSpecId)

    case RetrieveRunningDeployments =>
      deploymentManager forward RetrieveRunningDeployments
  }

  /**
    * Tries to acquire the lock for the given runSpecIds.
    * If it succeeds it executes the given function,
    * otherwise the result will contain an LockingFailedException.
    */
  def withLockFor[A](runSpecIds: Set[PathId])(f: => A): Try[A] = {
    // there's no need for synchronization here, because this is being
    // executed inside an actor, i.e. single threaded
    if (noConflictsWith(runSpecIds)) {
      addLocks(runSpecIds)
      Try(f)
    } else {
      Failure(new LockingFailedException("Failed to acquire locks."))
    }
  }

  def noConflictsWith(runSpecIds: Set[PathId]): Boolean = {
    val conflicts = lockedRunSpecs.keySet intersect runSpecIds
    conflicts.isEmpty
  }

  def removeLocks(runSpecIds: Set[PathId]): Unit = runSpecIds.foreach(removeLock)

  def removeLock(runSpecId: PathId): Unit = {
    if (lockedRunSpecs.contains(runSpecId)) {
      val locks = lockedRunSpecs(runSpecId) - 1
      if (locks <= 0) lockedRunSpecs -= runSpecId else lockedRunSpecs(runSpecId) -= 1
    }
  }

  def addLocks(runSpecIds: Set[PathId]): Unit = runSpecIds.foreach(addLock)

  def addLock(runSpecId: PathId): Unit = lockedRunSpecs(runSpecId) += 1

  /**
    * Tries to acquire the lock for the given runSpecId.
    * If it succeeds it executes the given function,
    * otherwise the result will contain an AppLockedException.
    */
  def withLockFor[A](runSpecId: PathId)(f: => A): Try[A] =
    withLockFor(Set(runSpecId))(f)

  // there has to be a better way...
  @SuppressWarnings(Array("OptionGet"))
  def driver: SchedulerDriver = marathonSchedulerDriverHolder.driver.get

  def deploy(origSender: ActorRef, cmd: Deploy): Unit = {
    val plan = cmd.plan
    val runSpecIds = plan.affectedRunSpecIds

    // If there are no conflicting locks or the deployment is forced we lock passed runSpecIds.
    // Afterwards the deployment plan is sent to DeploymentManager. It will take care of cancelling
    // conflicting deployments, scheduling new one or (if there were conflicts but the deployment
    // is not forced) send to the original sender and AppLockedException with conflicting deployments.
    //
    // If a deployment is forced (and there exists an old one):
    // - the new deployment will be started
    // - the old deployment will be cancelled and release all claimed locks
    // - only in this case, one RunSpec can have 2 locks
    if (noConflictsWith(runSpecIds) || cmd.force) {
      addLocks(runSpecIds)
    }
    deploymentManager ! StartDeployment(plan, origSender, cmd.force)
  }

  //deployment 成功
  def deploymentSuccess(plan: DeploymentPlan): Unit = {
    log.info(s"----->MarthonSchedulerActor.scala<-----Deployment ${plan.id}:${plan.version} of ${plan.target.id} finished")
    eventBus.publish(DeploymentSuccess(plan.id, plan))
  }

  def deploymentFailed(plan: DeploymentPlan, reason: Throwable): Unit = {
    log.error(reason, s"Deployment ${plan.id}:${plan.version} of ${plan.target.id} failed")
    plan.affectedRunSpecIds.foreach(runSpecId => launchQueue.purge(runSpecId))
    eventBus.publish(DeploymentFailed(plan.id, plan))
  }
}

object MarathonSchedulerActor {
  @SuppressWarnings(Array("MaxParameters"))
  def props(
             createSchedulerActions: ActorRef => SchedulerActions,
             deploymentManagerProps: SchedulerActions => Props,
             historyActorProps: Props,
             healthCheckManager: HealthCheckManager,
             killService: KillService,
             launchQueue: LaunchQueue,
             marathonSchedulerDriverHolder: MarathonSchedulerDriverHolder,
             electionService: ElectionService,
             eventBus: EventStream)(implicit mat: Materializer): Props = {
    Props(new MarathonSchedulerActor(
      createSchedulerActions,
      deploymentManagerProps,
      historyActorProps,
      healthCheckManager,
      killService,
      launchQueue,
      marathonSchedulerDriverHolder,
      electionService,
      eventBus
    ))
  }

  case class LoadedDeploymentsOnLeaderElection(deployments: Seq[DeploymentPlan])

  sealed trait Command {
    def answer: Event
  }

  case object ReconcileTasks extends Command {
    def answer: Event = TasksReconciled
  }

  private case object ReconcileFinished

  case object ReconcileHealthChecks

  case object ScaleRunSpecs

  case class ScaleRunSpec(runSpecId: PathId) extends Command {
    def answer: Event = RunSpecScaled(runSpecId)
  }

  case class Deploy(plan: DeploymentPlan, force: Boolean = false) extends Command {
    def answer: Event = DeploymentStarted(plan)
  }

  case class KillTasks(runSpecId: PathId, tasks: Seq[Instance]) extends Command {
    def answer: Event = TasksKilled(runSpecId, tasks.map(_.instanceId))
  }

  case object RetrieveRunningDeployments

  sealed trait Event

  case class RunSpecScaled(runSpecId: PathId) extends Event

  case object TasksReconciled extends Event

  case class DeploymentStarted(plan: DeploymentPlan) extends Event

  case class TasksKilled(runSpecId: PathId, taskIds: Seq[Instance.Id]) extends Event

  case class RunningDeployments(plans: Seq[DeploymentStepInfo])

  case class CommandFailed(cmd: Command, reason: Throwable) extends Event

  case object CancellationTimeoutExceeded

}

class SchedulerActions(
                        groupRepository: GroupRepository,
                        healthCheckManager: HealthCheckManager,
                        instanceTracker: InstanceTracker,
                        launchQueue: LaunchQueue,
                        eventBus: EventStream,
                        val schedulerActor: ActorRef,
                        val killService: KillService)(implicit ec: ExecutionContext) {

  private[this] val log = LoggerFactory.getLogger(getClass)

  // TODO move stuff below out of the scheduler

  def startRunSpec(runSpec: RunSpec): Unit = {
    //runSpec.id 就是appID , 如 /ftp/lgy007b
    // 创建时，会执行这个
    log.info(s"----->SchedulerActions.scala>-----Starting runSpec ${runSpec.id}\n----<SchedulerActions.scala>-----runSpec: $runSpec")
    //    AppDefinition(
    //                  /ftp/lgy002b,
    //                  Some(while [ true ] ; do echo 'Hello Marathon, Hello spark' ; sleep 5 ; done),
    //                  List(),
    //                  None,
    //                  Map(),
    //                  0,
    //                  Resources(0.1,128.0,0.0,0),
    //                  ,
    //                  Set(),
    //                  List(),
    //                  List(),
    //                  List(PortDefinition(10034,tcp,None,Map())),
    //                  false,
    //                  BackoffStrategy(1 second,3600 seconds,1.15),
    //                  Some(Mesos(List(PersistentVolume(ftp_data,PersistentVolumeInfo(10,None,root,Set()),RW)))),
    //                  Set(),
    //                  List(),
    //                  None,
    //                  Set(),
    //                  UpgradeStrategy(0.0,0.0),
    //                  Map(),
    //                  Set(),
    //                  None,
    //                  FullVersionInfo(2017-08-29T01:02:58.569Z,2017-08-29T01:02:58.569Z,2017-08-29T01:02:58.569Z),
    //                  Some(Residency(10,WAIT_FOREVER)),
    //                  Map(),
    //                  UnreachableDisabled,
    //                  YoungestFirst)
    //                  (mesosphere.marathon.SchedulerActions:marathon-akka.actor.default-dispatcher-7)
    scale(runSpec)
  }

  def stopRunSpec(runSpec: RunSpec): Future[_] = {
    healthCheckManager.removeAllFor(runSpec.id)

    log.info(s"Stopping runSpec ${runSpec.id}")
    instanceTracker.specInstances(runSpec.id).map { tasks =>
      tasks.foreach {
        instance =>
          if (instance.isLaunched) {
            log.info("Killing {}", instance.instanceId)
            killService.killInstance(instance, KillReason.DeletingApp)
          }
      }
      launchQueue.purge(runSpec.id)
      launchQueue.resetDelay(runSpec)

      // The tasks will be removed from the InstanceTracker when their termination
      // was confirmed by Mesos via a task update.

      eventBus.publish(AppTerminatedEvent(runSpec.id))
    }
  }

  def scaleRunSpec(): Unit = {
    groupRepository.root().foreach { root =>
      root.transitiveRunSpecs.foreach(spec => schedulerActor ! ScaleRunSpec(spec.id))
    }
  }

  /**
    * Make sure all runSpecs are running the configured amount of tasks.
    *
    * Should be called some time after the framework re-registers,
    * to give Mesos enough time to deliver task updates.
    *
    * @param driver scheduler driver
    */
  def reconcileTasks(driver: SchedulerDriver): Future[Status] = {
    groupRepository.root().flatMap { root =>
      val runSpecIds = root.transitiveRunSpecsById.keySet
      instanceTracker.instancesBySpec().map { instances =>
        val knownTaskStatuses = runSpecIds.flatMap { runSpecId =>
          TaskStatusCollector.collectTaskStatusFor(instances.specInstances(runSpecId))
        }
        log.info("------>instances.allSpecIdsWithInstances--------:\t" + instances.allSpecIdsWithInstances)
        log.info("------>runSpecIds--------:\t" + runSpecIds)
        (instances.allSpecIdsWithInstances -- runSpecIds).foreach { unknownId =>
          log.warn(
            s"RunSpec $unknownId exists in InstanceTracker, but not store. " +
              "The run spec was likely terminated. Will now expunge."
          )
          instances.specInstances(unknownId).foreach { orphanTask =>
            log.info(s"Killing ${orphanTask.instanceId}")
            killService.killInstance(orphanTask, KillReason.Orphaned)
          }
        }

        log.info("---------Requesting task reconciliation with the Mesos master------------")
        log.debug(s"--------Tasks to reconcile: $knownTaskStatuses")
        if (knownTaskStatuses.nonEmpty)
          driver.reconcileTasks(knownTaskStatuses)

        // in addition to the known statuses send an empty list to get the unknown
        driver.reconcileTasks(java.util.Arrays.asList())
      }
    }
  }

  def reconcileHealthChecks(): Unit = {
    groupRepository.root().flatMap { rootGroup =>
      healthCheckManager.reconcile(rootGroup.transitiveAppsById.valuesIterator.to[Seq])
    }
  }

  /**
    * Make sure the runSpec is running the correct number of instances
    * 异步
    */
  // FIXME: extract computation into a function that can be easily tested
  @SuppressWarnings(Array("all")) // async/await
  def scale(runSpec: RunSpec): Future[Done] = async {
    //异步代码块
    log.debug("Scale for run spec {}", runSpec)

    log.info("--------<SchedulerActions.scala>----------scale----await-----before------------")
    val runningInstances = await(instanceTracker.specInstances(runSpec.id)).filter { x =>
      log.info("--------<SchedulerActions.scala>---x-----:\n" + x + "\n")
      x.state.condition.isActive
    }
    log.info("-----<SchedulerActions.scala>------------scale----await-----after------------")
    def killToMeetConstraints(notSentencedAndRunning: Seq[Instance], toKillCount: Int) = {
      Constraints.selectInstancesToKill(runSpec, notSentencedAndRunning, toKillCount)
    }

    //从runSpec里获取当前的示例个数是多少
    val targetCount = runSpec.instances
    log.info("-----<SchedulerActions.scala>------从---runSpec里----获取到---实例个数----scale----targetCount--------:\t" + targetCount)

    // 这里 应该是声明了一个变量，注意，此变量是大写哦
    val ScalingProposition(instancesToKill, instancesToStart) = ScalingProposition.propose(
      runningInstances, None, killToMeetConstraints, targetCount, runSpec.killSelection)

    // 处理多task逻辑， 将多余的task，进行删除
    instancesToKill.foreach { instances: Seq[Instance] =>
      log.info(s"-----<SchedulerActions.scala>-------Scaling ${runSpec.id} from ${runningInstances.size} down to $targetCount instances-------")
      launchQueue.purge(runSpec.id)
      //map高阶函数
      //_.instanceId  对instances集合获取一个实例ID集合
      val xej_taskID = instances.map(_.instanceId)
      log.info("-----<SchedulerActions.scala>--------Killing instances {}---instanceID-----\n", xej_taskID.foreach(println(_)))
      killService.killInstances(instances, KillReason.OverCapacity)
    }

    instancesToStart.foreach { toStart: Int =>
      log.info(s"------<SchedulerActions.scala>---Need to scale ${runSpec.id} from ${runningInstances.size} up to $targetCount instances")
      val leftToLaunch = launchQueue.get(runSpec.id).fold(0)(_.instancesLeftToLaunch)
      val toAdd = toStart - leftToLaunch
      log.info("-----<SchedulerActions.scala>------>toAdd-----:\t" + toAdd)
      if (toAdd > 0) {
        log.info(s"---<SchedulerActions.scala>----->Queueing $toAdd new instances for ${runSpec.id} to the already $leftToLaunch queued ones")
        launchQueue.add(runSpec, toAdd)
      } else {
        log.info(s"---<SchedulerActions.scala>----->Already queued or started ${runningInstances.size} instances for ${runSpec.id}. Not scaling.")
      }
    }

    if (instancesToKill.isEmpty && instancesToStart.isEmpty) {
      log.info(s"----<SchedulerActions.scala>-----Already running ${runSpec.instances} instances of ${runSpec.id}. Not scaling.")
    }
    Done
  }

  @SuppressWarnings(Array("all")) // async/await
  def scale(runSpecId: PathId): Future[Done] = async {
    val runSpec = await(runSpecById(runSpecId))
    log.info("----->MarathonSchedulerActor.scala<---异步获取到了----runSpec-----\n" + runSpec)
    runSpec match {
      case Some(runSpec) =>
        log.info("----->MarathonSchedulerActor.scala<----已经获取到了----定义好的----runSpec-----")
        log.info("----->MarathonSchedulerActor.scala<----开始异步----执行----runSpec-----")
        await(scale(runSpec))
      case _ =>
        log.warn(s"RunSpec $runSpecId does not exist. Not scaling.")
        Done
    }
  }

  def runSpecById(id: PathId): Future[Option[RunSpec]] = {
    log.info("---->MarathonSchedulerActor.scala<---从--存储里---获取----定义好的app----runSpec-----")
    groupRepository.root().map(_.transitiveRunSpecsById.get(id))
  }
}

/**
  * Provides means to collect Mesos TaskStatus information for reconciliation.
  */
object TaskStatusCollector {
  def collectTaskStatusFor(instances: Seq[Instance]): Seq[mesos.Protos.TaskStatus] = {
    instances.flatMap { instance =>
      instance.tasksMap.values.withFilter {
        task =>
          println("----->xej-------task.isTerminal:\t" + !task.isTerminal)
          println("----->xej-------task.isReserved:\t" + !task.isReserved)
          !task.isTerminal && !task.isReserved
      }.map { task =>
        task.status.mesosStatus.getOrElse(initialMesosStatusFor(task, instance.agentInfo))
      }
    }(collection.breakOut)
  }

  /**
    * If a task was started but Marathon never received a status update for it, it will not have a
    * Mesos TaskStatus attached. In order to reconcile the state of this task, we need to create a
    * TaskStatus and fill it with the required information.
    */
  private[this] def initialMesosStatusFor(task: Task, agentInfo: AgentInfo): mesos.Protos.TaskStatus = {
    val taskStatusBuilder = mesos.Protos.TaskStatus.newBuilder
      // in fact we haven't received a status update for these yet, we just pretend it's staging
      .setState(TaskState.TASK_STAGING)
      .setTaskId(task.taskId.mesosTaskId)

    agentInfo.agentId.foreach { agentId =>
      taskStatusBuilder.setSlaveId(mesos.Protos.SlaveID.newBuilder().setValue(agentId))
    }

    taskStatusBuilder.build()
  }

}
