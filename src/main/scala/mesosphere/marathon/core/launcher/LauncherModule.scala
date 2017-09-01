package mesosphere.marathon.core.launcher

import mesosphere.marathon.{ MarathonConf, MarathonSchedulerDriverHolder }
import akka.stream.scaladsl.SourceQueue
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.launcher.impl.{ OfferProcessorImpl, TaskLauncherImpl, InstanceOpFactoryImpl }
import mesosphere.marathon.core.matcher.base.OfferMatcher
import mesosphere.marathon.core.task.tracker.InstanceCreationHandler
import mesosphere.marathon.core.plugin.PluginManager
import mesosphere.marathon.metrics.Metrics
import org.apache.mesos.Protos.Offer

/**
  * This module contains the glue code between matching tasks to resource offers
  * and actually launching the matched tasks.
  */
class LauncherModule(
    metrics: Metrics,
    conf: MarathonConf,
    taskCreationHandler: InstanceCreationHandler,
    marathonSchedulerDriverHolder: MarathonSchedulerDriverHolder,
    offerMatcher: OfferMatcher,
    pluginManager: PluginManager,
    offerStreamInput: SourceQueue[Offer]
)(implicit clock: Clock) {

  lazy val offerProcessor: OfferProcessor =
    new OfferProcessorImpl(
      conf, clock,
      metrics,
      offerMatcher, taskLauncher, taskCreationHandler,
      offerStreamInput)

  lazy val taskLauncher: TaskLauncher = new TaskLauncherImpl(
    metrics,
    marathonSchedulerDriverHolder)

  lazy val taskOpFactory: InstanceOpFactory = new InstanceOpFactoryImpl(conf, pluginManager)
}
