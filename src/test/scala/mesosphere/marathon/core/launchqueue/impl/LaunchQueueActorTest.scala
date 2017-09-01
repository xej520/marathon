package mesosphere.marathon.core.launchqueue.impl

import akka.Done
import akka.actor.{ Actor, Props }
import akka.pattern.ask
import akka.testkit.{ ImplicitSender, TestActorRef }
import akka.util.Timeout
import mesosphere.AkkaFunTest
import mesosphere.marathon.core.instance.TestInstanceBuilder
import mesosphere.marathon.core.instance.update.{ InstanceChange, InstanceUpdated }
import mesosphere.marathon.core.launchqueue.LaunchQueue.QueuedInstanceInfo
import mesosphere.marathon.core.launchqueue.LaunchQueueConfig
import mesosphere.marathon.state.{ AppDefinition, PathId, RunSpec, Timestamp }
import org.rogach.scallop.ScallopConf

import scala.concurrent.duration._

class LaunchQueueActorTest extends AkkaFunTest with ImplicitSender {

  implicit val timeout = Timeout(1.seconds)

  test("InstanceChange message is answered with Done, if there is no launcher actor") {
    Given("A LaunchQueueActor without any task launcher")
    val f = new Fixture
    import f._

    When("An InstanceChange is send to the task launcher actor")
    launchQueue ! instanceUpdate

    Then("A Done is send directly from the LaunchQueue")
    expectMsg(Done)
  }

  test("InstanceChange message is answered with Done, if there is a launcher actor") {
    Given("A LaunchQueueActor with a task launcher for app /foo")
    val f = new Fixture
    import f._
    launchQueue.ask(LaunchQueueDelegate.Add(app, 3)).futureValue
    launchQueue.underlyingActor.launchers should have size 1

    When("An InstanceChange is send to the task launcher actor")
    launchQueue ! instanceUpdate

    Then("A Done is send as well, but this time the answer comes from the LauncherActor")
    expectMsg(Done)
    val launcher = launchQueue.underlyingActor.launchers(app.id)
    launcher ! "GetChanges"
    expectMsg(List(instanceUpdate))
  }

  class Fixture {
    val config = new ScallopConf(Seq.empty) with LaunchQueueConfig {
      verify()
    }
    def runSpecActorProps(runSpec: RunSpec, count: Int) = Props(new TestLauncherActor) // linter:ignore UnusedParameter
    val app = AppDefinition(PathId("/foo"))
    val instance = TestInstanceBuilder.newBuilder(app.id).addTaskRunning().getInstance()
    val instanceUpdate = InstanceUpdated(instance, None, Seq.empty)
    val instanceInfo = QueuedInstanceInfo(app, true, 1, 1, Timestamp.now(), Timestamp.now())
    val launchQueue = TestActorRef[LaunchQueueActor](LaunchQueueActor.props(config, Actor.noSender, runSpecActorProps))

    // Mock the behaviour of the TaskLauncherActor
    class TestLauncherActor extends Actor {
      var changes = List.empty[InstanceChange]
      override def receive: Receive = {
        case TaskLauncherActor.GetCount => sender() ! instanceInfo
        case change: InstanceChange =>
          changes = change :: changes
          sender() ! Done
        case "GetChanges" => sender() ! changes // not part of the LauncherActor protocol. Only used to verify changes.
      }
    }
  }
}
