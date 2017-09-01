package mesosphere.marathon
package core.pod.impl

import akka.NotUsed
import akka.stream.scaladsl.Source
import mesosphere.marathon.core.group.GroupManager
import mesosphere.marathon.core.pod.{ PodDefinition, PodManager }
import mesosphere.marathon.state.{ PathId, Timestamp }
import mesosphere.marathon.stream._
import mesosphere.marathon.upgrade.DeploymentPlan

import scala.collection.immutable.Seq
import scala.concurrent.{ ExecutionContext, Future }

case class PodManagerImpl(groupManager: GroupManager)(implicit ctx: ExecutionContext) extends PodManager {

  override def ids(): Source[PathId, NotUsed] =
    Source.fromFuture(groupManager.rootGroup()).mapConcat(_.transitivePodsById.keySet)

  def create(p: PodDefinition, force: Boolean): Future[DeploymentPlan] = {
    def createOrThrow(opt: Option[PodDefinition]) = opt
      .map(_ => throw ConflictingChangeException(s"A pod with id [${p.id}] already exists."))
      .getOrElse(p)
    groupManager.updatePod(p.id, createOrThrow, p.version, force)
  }

  def findAll(filter: (PodDefinition) => Boolean): Source[PodDefinition, NotUsed] = {
    val pods: Future[Seq[PodDefinition]] =
      groupManager.rootGroup().map(_.transitivePodsById.values.filterAs(filter)(collection.breakOut))
    Source.fromFuture(pods).mapConcat(identity)
  }

  def find(id: PathId): Future[Option[PodDefinition]] = groupManager.pod(id)

  def update(p: PodDefinition, force: Boolean): Future[DeploymentPlan] =
    groupManager.updatePod(p.id, _ => p, p.version, force)

  def delete(id: PathId, force: Boolean): Future[DeploymentPlan] = {
    groupManager.updateRoot(_.removePod(id), force = force)
  }

  override def versions(id: PathId): Source[Timestamp, NotUsed] = groupManager.podVersions(id).map(Timestamp(_))

  override def version(id: PathId, version: Timestamp): Future[Option[PodDefinition]] =
    groupManager.podVersion(id, version.toOffsetDateTime)
}
