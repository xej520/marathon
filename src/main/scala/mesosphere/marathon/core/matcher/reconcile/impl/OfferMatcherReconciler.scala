package mesosphere.marathon
package core.matcher.reconcile.impl

import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.instance.update.InstanceUpdateOperation
import mesosphere.marathon.core.launcher.InstanceOp
import mesosphere.marathon.core.launcher.impl.TaskLabels
import mesosphere.marathon.core.matcher.base.OfferMatcher
import mesosphere.marathon.core.matcher.base.OfferMatcher.{ InstanceOpSource, InstanceOpWithSource, MatchedInstanceOps }
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.core.task.tracker.InstanceTracker
import mesosphere.marathon.core.task.tracker.InstanceTracker.InstancesBySpec
import mesosphere.marathon.state.{ RootGroup, Timestamp }
import mesosphere.marathon.storage.repository.GroupRepository
import mesosphere.marathon.stream._
import mesosphere.util.state.FrameworkId
import org.apache.mesos.Protos.{ Offer, OfferID, Resource }
import org.slf4j.LoggerFactory

import scala.concurrent.Future

/**
  * Matches task labels found in offer against known tasks/apps and
  *
  * * destroys unknown volumes
  * * unreserves unknown reservations
  *
  * In the future, we probably want to switch to a less agressive approach
  *
  * * by creating tasks in state "unknown" of unknown tasks which are then transitioned to state "garbage" after
  *   a delay
  * * and creating unreserved/destroy operations for tasks in state "garbage" only
  */
private[reconcile] class OfferMatcherReconciler(instanceTracker: InstanceTracker, groupRepository: GroupRepository)
    extends OfferMatcher {

  private val log = LoggerFactory.getLogger(getClass)

  import scala.concurrent.ExecutionContext.Implicits.global

  override def matchOffer(deadline: Timestamp, offer: Offer): Future[MatchedInstanceOps] = {

    val frameworkId = FrameworkId("").mergeFromProto(offer.getFrameworkId)

    val resourcesByTaskId: Map[Task.Id, Seq[Resource]] = {
      // TODO(PODS): don't use resident resources yet. Once they're needed it's not clear whether the labels
      // will continue to be task IDs, or pod instance IDs
      offer.getResourcesList.groupBy(TaskLabels.taskIdForResource(frameworkId, _)).collect {
        case (Some(taskId), resources) => taskId -> resources.to[Seq]
      }
    }

    processResourcesByTaskId(offer, resourcesByTaskId)
  }

  /**
    * Generate auxiliary instance operations for an offer based on current instance status.
    * For example, if an instance is no longer required then any resident resources it's using should be released.
    * 例如，如果一个实例，不再需要的话，那么，资源需要释放掉的
    */
  private[this] def processResourcesByTaskId(
    offer: Offer, resourcesByTaskId: Map[Task.Id, Seq[Resource]]): Future[MatchedInstanceOps] =
    {
      // do not query instanceTracker in the common case
      if (resourcesByTaskId.isEmpty) Future.successful(MatchedInstanceOps.noMatch(offer.getId))
      else {
        def createInstanceOps(instancesBySpec: InstancesBySpec, rootGroup: RootGroup): MatchedInstanceOps = {

          // TODO(jdef) pods don't suport resident resources yet so we don't need to worry about including them here
          /* Was this task launched from a previous app definition, or a prior launch that did not clean up properly */
          def spurious(instanceId: Instance.Id): Boolean =
            instancesBySpec.instance(instanceId).isEmpty || rootGroup.app(instanceId.runSpecId).isEmpty

          val instanceOps: Seq[InstanceOpWithSource] = resourcesByTaskId.collect {
            case (taskId, spuriousResources) if spurious(taskId.instanceId) =>
              val unreserveAndDestroy =
                // 从名字中，可以看出来，针对实例的工具类
                // 不再保存并且销毁掉持久卷
                InstanceOp.UnreserveAndDestroyVolumes(
                  stateOp = InstanceUpdateOperation.ForceExpunge(taskId.instanceId),
                  oldInstance = instancesBySpec.instance(taskId.instanceId),
                  resources = spuriousResources
                )
              log.warn(
                "removing spurious resources and volumes of {} because the instance does no longer exist",
                taskId.instanceId)
              InstanceOpWithSource(source(offer.getId), unreserveAndDestroy)
          }(collection.breakOut)

          MatchedInstanceOps(offer.getId, instanceOps, resendThisOffer = true)
        }

        // query in parallel
        val instancesBySpedFuture = instanceTracker.instancesBySpec()
        val rootGroupFuture = groupRepository.root()

        for { instancesBySpec <- instancesBySpedFuture; rootGroup <- rootGroupFuture }
          yield createInstanceOps(instancesBySpec, rootGroup)
      }
    }

  private[this] def source(offerId: OfferID) = new InstanceOpSource {
    override def instanceOpAccepted(instanceOp: InstanceOp): Unit =
      log.info(s"accepted unreserveAndDestroy for ${instanceOp.instanceId} in offer [${offerId.getValue}]")
    override def instanceOpRejected(instanceOp: InstanceOp, reason: String): Unit =
      log.info("rejected unreserveAndDestroy for {} in offer [{}]: {}", instanceOp.instanceId, offerId.getValue, reason)
  }
}
