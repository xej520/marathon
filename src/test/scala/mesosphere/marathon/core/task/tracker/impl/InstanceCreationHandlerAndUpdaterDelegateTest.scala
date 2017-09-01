package mesosphere.marathon.core.task.tracker.impl

import akka.Done
import akka.actor.Status
import akka.testkit.TestProbe
import mesosphere.marathon.core.instance.TestInstanceBuilder
import mesosphere.marathon.core.base.ConstantClock
import mesosphere.marathon.core.instance.update.{ InstanceUpdateEffect, InstanceUpdateOperation }
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.state.PathId
import mesosphere.marathon.test.{ MarathonActorSupport, MarathonSpec, MarathonTestHelper, Mockito }
import org.apache.mesos.Protos.{ TaskID, TaskStatus }
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ GivenWhenThen, Matchers }

class InstanceCreationHandlerAndUpdaterDelegateTest
    extends MarathonActorSupport with MarathonSpec with Mockito with GivenWhenThen with ScalaFutures with Matchers {

  test("Launch succeeds") {
    val f = new Fixture
    val appId: PathId = PathId("/test")
    val instance = TestInstanceBuilder.newBuilderWithLaunchedTask(appId).getInstance()
    val stateOp = InstanceUpdateOperation.LaunchEphemeral(instance)
    val expectedStateChange = InstanceUpdateEffect.Update(instance, None, events = Nil)

    When("created is called")
    val create = f.delegate.created(stateOp)

    Then("an update operation is requested")
    f.taskTrackerProbe.expectMsg(
      InstanceTrackerActor.ForwardTaskOp(f.timeoutFromNow, instance.instanceId, stateOp)
    )

    When("the request is acknowledged")
    f.taskTrackerProbe.reply(expectedStateChange)
    Then("The reply is Unit, because task updates are deferred")
    create.futureValue should be(Done)
  }

  test("Launch fails") {
    val f = new Fixture
    val appId: PathId = PathId("/test")
    val instance = TestInstanceBuilder.newBuilderWithLaunchedTask(appId).getInstance()
    val stateOp = InstanceUpdateOperation.LaunchEphemeral(instance)

    When("created is called")
    val create = f.delegate.created(stateOp)

    Then("an update operation is requested")
    f.taskTrackerProbe.expectMsg(
      InstanceTrackerActor.ForwardTaskOp(f.timeoutFromNow, instance.instanceId, stateOp)
    )

    When("the response is an error")
    val cause: RuntimeException = new scala.RuntimeException("test failure")
    f.taskTrackerProbe.reply(Status.Failure(cause))
    Then("The reply is the value of task")
    val createValue = create.failed.futureValue
    createValue.getMessage should include(appId.toString)
    createValue.getMessage should include(instance.instanceId.idString)
    createValue.getMessage should include("Launch")
    createValue.getCause should be(cause)
  }

  test("Terminated succeeds") {
    val f = new Fixture
    val appId: PathId = PathId("/test")
    val instance = TestInstanceBuilder.newBuilderWithLaunchedTask(appId).getInstance()
    val stateOp = InstanceUpdateOperation.ForceExpunge(instance.instanceId)
    val expectedStateChange = InstanceUpdateEffect.Expunge(instance, events = Nil)

    When("terminated is called")
    val terminated = f.delegate.process(stateOp)

    Then("an expunge operation is requested")
    f.taskTrackerProbe.expectMsg(
      InstanceTrackerActor.ForwardTaskOp(f.timeoutFromNow, instance.instanceId, stateOp)
    )

    When("the request is acknowledged")
    f.taskTrackerProbe.reply(expectedStateChange)
    Then("The reply is the value of the future")
    terminated.futureValue should be(expectedStateChange)
  }

  test("Terminated fails") {
    val f = new Fixture
    val appId: PathId = PathId("/test")
    val instance = TestInstanceBuilder.newBuilderWithLaunchedTask(appId).getInstance()
    val stateOp = InstanceUpdateOperation.ForceExpunge(instance.instanceId)

    When("terminated is called")
    val terminated = f.delegate.terminated(stateOp)

    Then("an expunge operation is requested")
    f.taskTrackerProbe.expectMsg(
      InstanceTrackerActor.ForwardTaskOp(f.timeoutFromNow, instance.instanceId, stateOp)
    )

    When("the response is an error")
    val cause: RuntimeException = new scala.RuntimeException("test failure")
    f.taskTrackerProbe.reply(Status.Failure(cause))
    Then("The reply is the value of task")
    val terminatedValue = terminated.failed.futureValue
    terminatedValue.getMessage should include(appId.toString)
    terminatedValue.getMessage should include(instance.instanceId.idString)
    terminatedValue.getMessage should include("Expunge")
    terminatedValue.getCause should be(cause)
  }

  test("StatusUpdate succeeds") {
    val f = new Fixture
    val appId: PathId = PathId("/test")
    val builder = TestInstanceBuilder.newBuilderWithLaunchedTask(appId)
    val instance = builder.getInstance()
    val task: Task.LaunchedEphemeral = builder.pickFirstTask()
    val taskIdString = task.taskId.idString
    val now = f.clock.now()

    val update = TaskStatus.newBuilder().setTaskId(TaskID.newBuilder().setValue(taskIdString)).buildPartial()
    val stateOp = InstanceUpdateOperation.MesosUpdate(instance, update, now)

    When("created is called")
    val statusUpdate = f.delegate.process(stateOp)

    Then("an update operation is requested")
    f.taskTrackerProbe.expectMsg(
      InstanceTrackerActor.ForwardTaskOp(f.timeoutFromNow, instance.instanceId, stateOp)
    )

    When("the request is acknowledged")
    val expectedStateChange = InstanceUpdateEffect.Update(instance, Some(instance), events = Nil)
    f.taskTrackerProbe.reply(expectedStateChange)
    Then("The reply is the value of the future")
    statusUpdate.futureValue should be(expectedStateChange)
  }

  test("StatusUpdate fails") {
    val f = new Fixture
    val appId: PathId = PathId("/test")
    val builder = TestInstanceBuilder.newBuilderWithLaunchedTask(appId)
    val instance = builder.getInstance()
    val task: Task.LaunchedEphemeral = builder.pickFirstTask()
    val taskId = task.taskId
    val now = f.clock.now()

    val update = TaskStatus.newBuilder().setTaskId(taskId.mesosTaskId).buildPartial()
    val stateOp = InstanceUpdateOperation.MesosUpdate(instance, update, now)

    When("statusUpdate is called")
    val statusUpdate = f.delegate.process(stateOp)

    Then("an update operation is requested")
    f.taskTrackerProbe.expectMsg(
      InstanceTrackerActor.ForwardTaskOp(f.timeoutFromNow, instance.instanceId, stateOp)
    )

    When("the response is an error")
    val cause: RuntimeException = new scala.RuntimeException("test failure")
    f.taskTrackerProbe.reply(Status.Failure(cause))
    Then("The reply is the value of task")
    val updateValue = statusUpdate.failed.futureValue
    updateValue.getMessage should include(appId.toString)
    updateValue.getMessage should include(taskId.toString)
    updateValue.getMessage should include("MesosUpdate")
    updateValue.getCause should be(cause)
  }

  class Fixture {
    lazy val clock = ConstantClock()
    lazy val config = MarathonTestHelper.defaultConfig()
    lazy val taskTrackerProbe = TestProbe()
    lazy val delegate = new InstanceCreationHandlerAndUpdaterDelegate(clock, config, taskTrackerProbe.ref)
    lazy val timeoutDuration = delegate.timeout.duration
    def timeoutFromNow = clock.now() + timeoutDuration
  }
}
