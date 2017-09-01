package mesosphere.marathon
package core.pod

import mesosphere.marathon.core.group.GroupManager
import mesosphere.marathon.core.pod.impl.PodManagerImpl

import scala.concurrent.ExecutionContext

case class PodModule(groupManager: GroupManager)(implicit ctx: ExecutionContext) {
  lazy val podManager: PodManager = PodManagerImpl(groupManager)
}
