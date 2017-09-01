package mesosphere.marathon.core.task.tracker.impl

import java.util.concurrent.TimeoutException

import akka.actor.{ Status, Terminated }
import akka.testkit.{ TestActorRef, TestProbe }
import com.codahale.metrics.MetricRegistry
import mesosphere.marathon.core.base.ConstantClock
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.instance.update.InstanceUpdateOperation
import mesosphere.marathon.integration.setup.WaitTestSupport
import mesosphere.marathon.metrics.Metrics
import mesosphere.marathon.state.{ PathId, Timestamp }
import mesosphere.marathon.test.{ MarathonActorSupport, Mockito }
import org.scalatest.{ FunSuiteLike, GivenWhenThen, Matchers }

import scala.concurrent.duration._
import scala.concurrent.{ Future, Promise }

class InstanceUpdateActorTest
    extends MarathonActorSupport with FunSuiteLike with Mockito with GivenWhenThen with Matchers {

  test("process failures are escalated") {
    val f = new Fixture

    Given("an op")
    val appId = PathId("/app")
    val instanceId = Instance.Id.forRunSpec(appId)
    val op = InstanceOpProcessor.Operation(f.oneSecondInFuture, f.opInitiator.ref, instanceId, InstanceUpdateOperation.ForceExpunge(instanceId))

    And("a processor that fails immediately")
    val processingFailure: RuntimeException = new scala.RuntimeException("processing failed")
    f.processor.process(eq(op))(any) returns Future.failed(processingFailure)

    When("the op is passed to the actor for processing")
    f.updateActor.receive(InstanceUpdateActor.ProcessInstanceOp(op))

    Then("process op was called")
    verify(f.processor).process(eq(op))(any)

    And("the exception is escalated and the actor dies")
    watch(f.updateActor)
    expectMsgClass(classOf[Terminated]).getActor should equal(f.updateActor)

    And("there are no more interactions")
    f.verifyNoMoreInteractions()
  }

  test("process timeouts are checked") {
    val f = new Fixture

    Given("an op with an already reached deadline")
    val appId = PathId("/app")
    val instanceId = Instance.Id.forRunSpec(appId)
    val op = InstanceOpProcessor.Operation(f.clock.now(), f.opInitiator.ref, instanceId, InstanceUpdateOperation.ForceExpunge(instanceId))

    And("a processor that succeeds immediately")
    f.processor.process(eq(op))(any) returns Future.successful(())

    When("the op is passed to the actor for processing")
    f.updateActor.receive(InstanceUpdateActor.ProcessInstanceOp(op))

    Then("we the sender gets a timeout exception")
    val failure = f.opInitiator.expectMsgClass(classOf[Status.Failure])
    failure.cause.getClass should be(classOf[TimeoutException])

    And("there are no more interactions")
    f.verifyNoMoreInteractions()

    Given("a processor that processes 'anotherOp' immediately")
    val anotherOp = op.copy(deadline = f.oneSecondInFuture)
    f.processor.process(eq(anotherOp))(any) returns Future.successful(())

    When("we process another op, it is not effected")
    f.updateActor.receive(InstanceUpdateActor.ProcessInstanceOp(anotherOp))

    Then("process op was called")
    verify(f.processor).process(eq(anotherOp))(any)

    And("there are no more interactions")
    f.verifyNoMoreInteractions()
  }

  test("first op for a task is directly processed") {
    val f = new Fixture

    Given("an op")
    val appId = PathId("/app")
    val instanceId = Instance.Id.forRunSpec(appId)
    val op = InstanceOpProcessor.Operation(f.oneSecondInFuture, f.opInitiator.ref, instanceId, InstanceUpdateOperation.ForceExpunge(instanceId))

    And("a processor that processes it immediately")
    f.processor.process(eq(op))(any) returns Future.successful(())

    When("the op is passed to the actor for processing")
    f.updateActor.receive(InstanceUpdateActor.ProcessInstanceOp(op))

    Then("process op was called")
    verify(f.processor).process(eq(op))(any)

    And("all gauges are zero again")
    f.actorMetrics.numberOfActiveOps.getValue should be(0)
    f.actorMetrics.numberOfQueuedOps.getValue should be(0)

    And("there are no more interactions")
    f.verifyNoMoreInteractions()
  }

  test("currently processed ops are visible in the metrics") {
    val f = new Fixture

    Given("an op")
    val appId = PathId("/app")
    val instanceId = Instance.Id.forRunSpec(appId)
    val op = InstanceOpProcessor.Operation(f.oneSecondInFuture, f.opInitiator.ref, instanceId, InstanceUpdateOperation.ForceExpunge(instanceId))

    And("a processor that does not return")
    f.processor.process(eq(op))(any) returns Promise[Unit]().future

    When("the op is passed to the actor for processing")
    f.updateActor.receive(InstanceUpdateActor.ProcessInstanceOp(op))

    Then("process op was called")
    verify(f.processor).process(eq(op))(any)

    And("there is one active request and none queued")
    f.actorMetrics.numberOfActiveOps.getValue should be(1)
    f.actorMetrics.numberOfQueuedOps.getValue should be(0)

    And("there are no more interactions")
    f.verifyNoMoreInteractions()
  }

  test("ops for different tasks are processed concurrently") {
    val f = new Fixture

    Given("an op")
    val appId = PathId("/app")
    val instance1Id = Instance.Id.forRunSpec(appId)
    val op1 = InstanceOpProcessor.Operation(f.oneSecondInFuture, f.opInitiator.ref, instance1Id, InstanceUpdateOperation.ForceExpunge(instance1Id))
    val instance = Instance.Id.forRunSpec(appId)
    val op2 = InstanceOpProcessor.Operation(f.oneSecondInFuture, f.opInitiator.ref, instance, InstanceUpdateOperation.ForceExpunge(instance))

    And("a processor that does not return")
    val op1Promise: Promise[Unit] = Promise[Unit]()
    f.processor.process(eq(op1))(any) returns op1Promise.future
    val op2Promise: Promise[Unit] = Promise[Unit]()
    f.processor.process(eq(op2))(any) returns op2Promise.future

    When("the ops are passed to the actor for processing")
    f.updateActor.receive(InstanceUpdateActor.ProcessInstanceOp(op1))
    f.updateActor.receive(InstanceUpdateActor.ProcessInstanceOp(op2))

    Then("process op was called for both ops")
    verify(f.processor).process(eq(op1))(any)
    verify(f.processor).process(eq(op2))(any)

    And("there are two active requests and none queued")
    f.actorMetrics.numberOfActiveOps.getValue should be(2)
    f.actorMetrics.numberOfQueuedOps.getValue should be(0)

    And("there are no more interactions")
    f.verifyNoMoreInteractions()

    When("op2 finishes")
    op2Promise.success(())

    Then("eventually our active ops count gets decreased")
    WaitTestSupport.waitUntil("actor reacts to op2 finishing", 1.second)(f.actorMetrics.numberOfActiveOps.getValue == 1)

    And("the second task doesn't have queue anymore")
    f.updateActor.underlyingActor.operationsByInstanceId should have size 1

    And("but the first task still does have a queue")
    f.updateActor.underlyingActor.operationsByInstanceId(instance1Id) should have size 1
  }

  test("ops for the same task are processed sequentially") {
    val f = new Fixture

    Given("an op")
    val appId = PathId("/app")
    val instance1Id = Instance.Id.forRunSpec(appId)
    val op1 = InstanceOpProcessor.Operation(
      f.oneSecondInFuture, f.opInitiator.ref, instance1Id, InstanceUpdateOperation.ForceExpunge(instance1Id)
    )
    val op2 = InstanceOpProcessor.Operation(
      f.oneSecondInFuture, f.opInitiator.ref, instance1Id, InstanceUpdateOperation.ForceExpunge(instance1Id)
    )

    And("a processor that does not return")
    val op1Promise: Promise[Unit] = Promise[Unit]()
    f.processor.process(eq(op1))(any) returns op1Promise.future

    When("the ops are passed to the actor for processing")
    f.updateActor.receive(InstanceUpdateActor.ProcessInstanceOp(op1))
    f.updateActor.receive(InstanceUpdateActor.ProcessInstanceOp(op2))

    Then("process op was called for op1")
    verify(f.processor).process(eq(op1))(any)

    And("there are one active request and one queued")
    f.actorMetrics.numberOfActiveOps.getValue should be(1)
    f.actorMetrics.numberOfQueuedOps.getValue should be(1)

    And("there are no more interactions (for now)")
    f.verifyNoMoreInteractions()

    When("op1 finishes")
    val op2Promise: Promise[Unit] = Promise[Unit]()
    f.processor.process(eq(op2))(any) returns op2Promise.future
    op1Promise.success(())

    Then("eventually process gets called on op2")
    verify(f.processor, timeout(1000)).process(eq(op2))(any)

    And("there are one active request and none queued anymore")
    f.actorMetrics.numberOfActiveOps.getValue should be(1)
    f.actorMetrics.numberOfQueuedOps.getValue should be(0)

    And("there are no more interactions (for now)")
    f.verifyNoMoreInteractions()

    When("op2 finishes")
    op2Promise.success(())

    Then("eventually our active ops count gets decreased")
    WaitTestSupport.waitUntil("actor reacts to op2 finishing", 1.second)(f.actorMetrics.numberOfActiveOps.getValue == 0)

    And("our queue will be empty")
    f.updateActor.underlyingActor.operationsByInstanceId should be(empty)

    And("there are no more interactions")
    f.verifyNoMoreInteractions()
  }

  class Fixture {
    lazy val clock = ConstantClock()
    lazy val opInitiator = TestProbe()
    lazy val metrics = new Metrics(new MetricRegistry)
    lazy val actorMetrics = new InstanceUpdateActor.ActorMetrics(metrics)
    lazy val processor = mock[InstanceOpProcessor]
    lazy val updateActor = TestActorRef(new InstanceUpdateActor(clock, actorMetrics, processor))

    def oneSecondInFuture: Timestamp = clock.now() + 1.second

    def verifyNoMoreInteractions(): Unit = {
      noMoreInteractions(processor)
      reset(processor)
    }
  }
}
