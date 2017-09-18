package mesosphere.marathon
package api

import javax.inject.Inject

import akka.Done
import mesosphere.marathon.core.group.GroupManager
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.instance.update.InstanceUpdateOperation
import mesosphere.marathon.core.task.termination.{ KillReason, KillService }
import mesosphere.marathon.core.task.tracker.{ InstanceTracker, TaskStateOpProcessor }
import mesosphere.marathon.plugin.auth.{ Authenticator, Authorizer, Identity, UpdateRunSpec }
import mesosphere.marathon.state._
import mesosphere.marathon.upgrade.DeploymentPlan
import org.slf4j.LoggerFactory

import scala.async.Async.{ async, await }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.control.NonFatal

class TaskKiller @Inject() (
    instanceTracker: InstanceTracker,
    stateOpProcessor: TaskStateOpProcessor,
    groupManager: GroupManager,
    service: MarathonSchedulerService,
    val config: MarathonConf,
    val authenticator: Authenticator,
    val authorizer: Authorizer,
    killService: KillService) extends AuthResource {

  private[this] val log = LoggerFactory.getLogger(getClass)

  @SuppressWarnings(Array("all")) // async/await
  def kill(
    runSpecId: PathId,
    findToKill: (Seq[Instance] => Seq[Instance]),
    wipe: Boolean = false)(implicit identity: Identity): Future[Seq[Instance]] = {

    result(groupManager.runSpec(runSpecId)) match {
      case Some(runSpec) =>
        checkAuthorization(UpdateRunSpec, runSpec)
        async { // linter:ignore:UnnecessaryElseBranch
          val allInstances = await(instanceTracker.specInstances(runSpecId))
          val foundInstances = findToKill(allInstances)
          val launchedInstances = foundInstances.filter(_.isLaunched)

          if (wipe) {
            log.info("----<TaskKiller.scala>-----删除task----擦除---持久卷-----------")
            val done1 = await(expunge(foundInstances))
            val done2 = await(killService.killInstances(launchedInstances, KillReason.KillingTasksViaApi))
          } else {
            if (launchedInstances.nonEmpty) service.killInstances(runSpecId, launchedInstances)
          }
          // Return killed *and* expunged instances.
          // The user only cares that all instances won't exist eventually. That's why we send all instances back and
          // not just the killed instances.
          foundInstances
        }

      case None => Future.failed(PathNotFoundException(runSpecId))
    }
  }

  //expunge 删除
  private[this] def expunge(instances: Seq[Instance]): Future[Done] = {
    // Note: We process all instances sequentially.

    instances.foldLeft(Future.successful(Done)) { (resultSoFar, nextInstance) =>
      resultSoFar.flatMap { _ =>
        log.info("Expunging {}", nextInstance.instanceId)
        stateOpProcessor.process(InstanceUpdateOperation.ForceExpunge(nextInstance.instanceId)).map(_ => Done).recover {
          case NonFatal(cause) =>
            log.info("Failed to expunge {}, got: {}", Array[Object](nextInstance.instanceId, cause): _*)
            Done
        }
      }
    }
  }

  @SuppressWarnings(Array("all")) // async/await
  def killAndScale(
    appId: PathId,
    findToKill: (Seq[Instance] => Seq[Instance]),
    force: Boolean)(implicit identity: Identity): Future[DeploymentPlan] = async {
    val instances = await(instanceTracker.specInstances(appId))
    val launchedInstances = instances.filter(_.isLaunched)
    val instancesToKill = findToKill(launchedInstances)
    await(killAndScale(Map(appId -> instancesToKill), force))
  }

  @SuppressWarnings(Array("all")) // async/await
  def killAndScale(
    appInstances: Map[PathId, Seq[Instance]],
    force: Boolean)(implicit identity: Identity): Future[DeploymentPlan] = {
    def scaleApp(app: AppDefinition): AppDefinition = {
      checkAuthorization(UpdateRunSpec, app)
      appInstances.get(app.id).fold(app) { instances =>
        // only count active instances that did not already receive a kill request.
        val toKillCount = instances.count(i => i.isActive && !i.isKilling)
        // make sure we never scale below zero instances.
        app.copy(instances = math.max(0, app.instances - toKillCount))
      }
    }

    val version = Timestamp.now()

    def killDeployment = groupManager.updateRoot(
      _.updateTransitiveApps(PathId.empty, scaleApp, version),
      version = version,
      force = force,
      toKill = appInstances
    )

    async {
      val allInstances = await(instanceTracker.instancesBySpec()).instancesMap
      //TODO: The exception does not take multiple ids.
      appInstances.keys.find(!allInstances.contains(_)).map(id => throw PathNotFoundException(id))
      await(killDeployment)
    }
  }
}
