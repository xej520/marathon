package mesosphere.marathon.core.launchqueue.impl

import akka.Done
import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.core.instance.update.InstanceChange
import mesosphere.marathon.core.launchqueue.LaunchQueue.{ QueuedInstanceInfo, QueuedInstanceInfoWithStatistics }
import mesosphere.marathon.core.launchqueue.{ LaunchQueue, LaunchQueueConfig }
import mesosphere.marathon.state.{ PathId, RunSpec }

import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }
import scala.reflect.ClassTag
import scala.util.control.NonFatal

private[launchqueue] class LaunchQueueDelegate(
    config: LaunchQueueConfig,
    actorRef: ActorRef,
    rateLimiterRef: ActorRef) extends LaunchQueue with StrictLogging {

  // When purging, we wait for the TaskLauncherActor to shut down. This actor will wait for
  // in-flight task op notifications before complying, therefore we need to adjust the timeout accordingly.
  val purgeTimeout: Timeout = config.launchQueueRequestTimeout().milliseconds + config.taskOpNotificationTimeout().millisecond

  val launchQueueRequestTimeout: Timeout = config.launchQueueRequestTimeout().milliseconds

  override def list: Seq[QueuedInstanceInfo] = {
    askQueueActor[LaunchQueueDelegate.Request, Seq[QueuedInstanceInfo]]("list")(LaunchQueueDelegate.List)
  }

  override def listWithStatistics: Seq[QueuedInstanceInfoWithStatistics] = {
    askQueueActor[LaunchQueueDelegate.Request, Seq[QueuedInstanceInfoWithStatistics]]("listWithStatistics")(LaunchQueueDelegate.ListWithStatistics)
  }

  override def get(runSpecId: PathId): Option[QueuedInstanceInfo] =
    askQueueActor[LaunchQueueDelegate.Request, Option[QueuedInstanceInfo]]("get")(LaunchQueueDelegate.Count(runSpecId))

  override def notifyOfInstanceUpdate(update: InstanceChange): Future[Done] =
    askQueueActorFuture[InstanceChange, Done]("notifyOfInstanceUpdate")(update)

  override def count(runSpecId: PathId): Int = get(runSpecId).map(_.instancesLeftToLaunch).getOrElse(0)

  override def listRunSpecs: Seq[RunSpec] = list.map(_.runSpec)

  override def purge(runSpecId: PathId): Unit = {
    askQueueActor[LaunchQueueDelegate.Request, Unit]("purge", timeout = purgeTimeout)(LaunchQueueDelegate.Purge(runSpecId))
  }

  override def add(runSpec: RunSpec, count: Int): Unit = askQueueActor[LaunchQueueDelegate.Request, Unit]("add")(LaunchQueueDelegate.Add(runSpec, count))

  private[this] def askQueueActor[T, R: ClassTag](
    method: String,
    timeout: Timeout = launchQueueRequestTimeout)(message: T): R = {

    val answerFuture = askQueueActorFuture[T, R](method, timeout)(message)
    Await.result(answerFuture, timeout.duration)
  }

  private[this] def askQueueActorFuture[T, R: ClassTag](
    method: String,
    timeout: Timeout = launchQueueRequestTimeout)(message: T): Future[R] = {

    implicit val timeoutImplicit: Timeout = timeout
    val answerFuture = actorRef ? message
    import scala.concurrent.ExecutionContext.Implicits.global
    answerFuture.recover {
      case NonFatal(e) => throw new RuntimeException(s"in $method", e)
    }
    answerFuture.mapTo[R]
  }

  override def addDelay(spec: RunSpec): Unit = rateLimiterRef ! RateLimiterActor.AddDelay(spec)

  override def resetDelay(spec: RunSpec): Unit = rateLimiterRef ! RateLimiterActor.ResetDelay(spec)
}

private[impl] object LaunchQueueDelegate {
  sealed trait Request
  case object List extends Request
  case object ListWithStatistics extends Request
  case class Count(runSpecId: PathId) extends Request
  case class Purge(runSpecId: PathId) extends Request
  case object ConfirmPurge extends Request
  case class Add(spec: RunSpec, count: Int) extends Request
}
