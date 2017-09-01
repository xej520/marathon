package mesosphere.marathon
package core.group

import java.time.OffsetDateTime

import akka.NotUsed
import akka.stream.scaladsl.Source
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.pod.PodDefinition
import mesosphere.marathon.state.{ AppDefinition, Group, PathId, RootGroup, RunSpec, Timestamp }
import mesosphere.marathon.upgrade.DeploymentPlan

import scala.concurrent.Future
import scala.collection.immutable.Seq

/**
  * The group manager is the facade for all group related actions.
  * It persists the state of a group and initiates deployments.
  */
trait GroupManager {

  def rootGroup(): Future[RootGroup]

  /**
    * Get all available versions for given group identifier.
    * @param id the identifier of the group.
    * @return the list of versions of this object.
    */
  def versions(id: PathId): Future[Seq[Timestamp]]

  /**
    * Get all available app versions for a given app id
    */
  def appVersions(id: PathId): Source[OffsetDateTime, NotUsed]

  /**
    * Get the app definition for an id at a specific version
    */
  def appVersion(id: PathId, version: OffsetDateTime): Future[Option[AppDefinition]]

  /**
    * Get all available pod versions for a given pod id
    */
  def podVersions(id: PathId): Source[OffsetDateTime, NotUsed]

  /**
    * Get the pod definition for an id at a specific version
    */
  def podVersion(id: PathId, version: OffsetDateTime): Future[Option[PodDefinition]]

  /**
    * Get a specific group by its id.
    * @param id the id of the group.
    * @return the group if it is found, otherwise None
    */
  def group(id: PathId): Future[Option[Group]]

  /**
    * Get a specific group with a specific version.
    * @param id the identifier of the group.
    * @param version the version of the group.
    * @return the group if it is found, otherwise None
    */
  def group(id: PathId, version: Timestamp): Future[Option[Group]]

  /**
    * Get a specific run spec by its Id
    * @param id The id of the runSpec
    * @return The run spec if it is found, otherwise none.
    */
  def runSpec(id: PathId): Future[Option[RunSpec]]

  /**
    * Get a specific app definition by its id.
    * @param id the id of the app.
    * @return the app if it is found, otherwise false
    */
  def app(id: PathId): Future[Option[AppDefinition]]

  /**
    * Get a specific pod definition by its id.
    * @param id the id of the pod.
    * @return the pod if it is found, otherwise false
    */
  def pod(id: PathId): Future[Option[PodDefinition]]

  /**
    * Update a group with given identifier.
    * The change of the group is defined by a change function.
    * The complete tree gets the given version.
    * The change could take time to get deployed.
    * For this reason, we return the DeploymentPlan as result, which can be queried in the marathon scheduler.
    *
    * @param fn the update function, which is applied to the root group
    * @param version the new version of the group, after the change has applied.
    * @param force only one update can be applied to applications at a time. with this flag
    *              one can control, to stop a current deployment and start a new one.
    * @return the deployment plan which will be executed.
    */
  def updateRoot(
    fn: RootGroup => RootGroup,
    version: Timestamp = Timestamp.now(),
    force: Boolean = false,
    toKill: Map[PathId, Seq[Instance]] = Map.empty): Future[DeploymentPlan]

  /**
    * Update application with given identifier and update function.
    * The change could take time to get deployed.
    * For this reason, we return the DeploymentPlan as result, which can be queried in the marathon scheduler.
    *
    * @param appId the identifier of the application
    * @param fn the application change function
    * @param version the version of the change
    * @param force if the change has to be forced.
    * @return the deployment plan which will be executed.
    */
  def updateApp(
    appId: PathId,
    fn: Option[AppDefinition] => AppDefinition,
    version: Timestamp = Timestamp.now(),
    force: Boolean = false,
    toKill: Seq[Instance] = Seq.empty): Future[DeploymentPlan]

  def updatePod(
    podId: PathId,
    fn: Option[PodDefinition] => PodDefinition,
    version: Timestamp = Timestamp.now(),
    force: Boolean = false,
    toKill: Seq[Instance] = Seq.empty
  ): Future[DeploymentPlan]
}
