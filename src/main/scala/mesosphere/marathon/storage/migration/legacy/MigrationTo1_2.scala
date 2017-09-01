package mesosphere.marathon
package storage.migration.legacy

import akka.Done
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import mesosphere.marathon.core.task.TaskCondition
import mesosphere.marathon.core.task.tracker.impl.TaskConditionSerializer
import mesosphere.marathon.metrics.Metrics
import mesosphere.marathon.state.MarathonTaskState
import mesosphere.marathon.storage.LegacyStorageConfig
import mesosphere.marathon.storage.repository.{ DeploymentRepository, TaskRepository }
import mesosphere.marathon.upgrade.DeploymentPlan
import org.slf4j.LoggerFactory

import scala.async.Async.{ async, await }
import scala.concurrent.{ ExecutionContext, Future }

/**
  * Removes all deployment version nodes from ZK
  */
@SuppressWarnings(Array("ClassNames"))
class MigrationTo1_2(legacyConfig: Option[LegacyStorageConfig])(implicit
  ctx: ExecutionContext,
    metrics: Metrics,
    mat: Materializer) {
  private[this] val log = LoggerFactory.getLogger(getClass)

  @SuppressWarnings(Array("all")) // async/await
  def migrate(): Future[Unit] =
    legacyConfig.fold(Future.successful(())) { config =>
      log.info("Start 1.2 migration")

      val entityStore = DeploymentRepository.legacyRepository(config.entityStore[DeploymentPlan]).store
      val taskStore = TaskRepository.legacyRepository(config.entityStore[MarathonTaskState]).repo

      import mesosphere.marathon.state.VersionedEntry.isVersionKey
      async { // linter:ignore UnnecessaryElseBranch
        val removeDeploymentVersions =
          entityStore.names().map(_.filter(isVersionKey)).flatMap { versionNodes =>
            versionNodes.foldLeft(Future.successful(())) { (future, versionNode) =>
              future.flatMap { _ =>
                entityStore.expunge(versionNode).map(_ => ())
              }
            }
          }

        val addTaskStatuses = taskStore.all().mapAsync(Int.MaxValue) { task =>
          val proto = task.toProto
          if (!proto.hasCondition) {
            val updated = proto.toBuilder
              .setCondition(
                if (proto.hasStatus) TaskConditionSerializer.toProto(TaskCondition(proto.getStatus))
                else Protos.MarathonTask.Condition.Unknown
              )
            taskStore.store(MarathonTaskState(updated.build()))
          } else {
            Future.successful(Done)
          }
        }.runWith(Sink.ignore)

        await(removeDeploymentVersions)
        await(addTaskStatuses)
      }.map { _ =>
        log.info("Finished 1.2 migration")
        ()
      }
    }
}
