package mesosphere.marathon

import akka.Done
import akka.event.EventStream
import akka.testkit.TestProbe
import mesosphere.AkkaFunTest
import mesosphere.marathon.core.event._
import mesosphere.marathon.core.launcher.OfferProcessor
import mesosphere.marathon.core.launchqueue.LaunchQueue
import mesosphere.marathon.core.task.update.TaskStatusUpdateProcessor
import mesosphere.marathon.storage.repository.{ AppRepository, FrameworkIdRepository }
import mesosphere.marathon.test.MarathonTestHelper
import mesosphere.util.state.{ FrameworkId, MesosLeaderInfo, MutableMesosLeaderInfo }
import org.apache.mesos.Protos._
import org.apache.mesos.SchedulerDriver

import scala.concurrent.Future

class MarathonSchedulerTest extends AkkaFunTest {

  var probe: TestProbe = _
  var repo: AppRepository = _
  var queue: LaunchQueue = _
  var marathonScheduler: MarathonScheduler = _
  var frameworkIdRepository: FrameworkIdRepository = _
  var mesosLeaderInfo: MesosLeaderInfo = _
  var config: MarathonConf = _
  var eventBus: EventStream = _
  var offerProcessor: OfferProcessor = _
  var taskStatusProcessor: TaskStatusUpdateProcessor = _
  var suicideFn: (Boolean) => Unit = { _ => () }

  before {
    repo = mock[AppRepository]
    queue = mock[LaunchQueue]
    frameworkIdRepository = mock[FrameworkIdRepository]
    mesosLeaderInfo = new MutableMesosLeaderInfo
    mesosLeaderInfo.onNewMasterInfo(MasterInfo.getDefaultInstance)
    config = MarathonTestHelper.defaultConfig(maxTasksPerOffer = 10)
    probe = TestProbe()
    eventBus = system.eventStream
    taskStatusProcessor = mock[TaskStatusUpdateProcessor]
    marathonScheduler = new MarathonScheduler(
      eventBus,
      offerProcessor = offerProcessor,
      taskStatusProcessor = taskStatusProcessor,
      frameworkIdRepository,
      mesosLeaderInfo,
      config) {
      override protected def suicide(removeFrameworkId: Boolean): Unit = {
        suicideFn(removeFrameworkId)
      }
    }
  }

  test("Publishes event when registered") {
    val driver = mock[SchedulerDriver]
    val frameworkId = FrameworkID.newBuilder
      .setValue("some_id")
      .build()

    val masterInfo = MasterInfo.newBuilder()
      .setId("")
      .setIp(0)
      .setPort(5050)
      .setHostname("some_host")
      .build()

    frameworkIdRepository.store(any) returns Future.successful(Done)

    eventBus.subscribe(probe.ref, classOf[SchedulerRegisteredEvent])

    marathonScheduler.registered(driver, frameworkId, masterInfo)

    try {
      val msg = probe.expectMsgType[SchedulerRegisteredEvent]

      assert(msg.frameworkId == frameworkId.getValue)
      assert(msg.master == masterInfo.getHostname)
      assert(msg.eventType == "scheduler_registered_event")
      assert(mesosLeaderInfo.currentLeaderUrl.get == "http://some_host:5050/")
      verify(frameworkIdRepository).store(FrameworkId.fromProto(frameworkId))
      noMoreInteractions(frameworkIdRepository)
    } finally {
      eventBus.unsubscribe(probe.ref)
    }
  }

  test("Publishes event when reregistered") {
    val driver = mock[SchedulerDriver]
    val masterInfo = MasterInfo.newBuilder()
      .setId("")
      .setIp(0)
      .setPort(5050)
      .setHostname("some_host")
      .build()

    eventBus.subscribe(probe.ref, classOf[SchedulerReregisteredEvent])

    marathonScheduler.reregistered(driver, masterInfo)

    try {
      val msg = probe.expectMsgType[SchedulerReregisteredEvent]

      assert(msg.master == masterInfo.getHostname)
      assert(msg.eventType == "scheduler_reregistered_event")
      assert(mesosLeaderInfo.currentLeaderUrl.get == "http://some_host:5050/")
    } finally {
      eventBus.unsubscribe(probe.ref)
    }
  }

  // Currently does not work because of the injection used in MarathonScheduler.callbacks
  test("Publishes event when disconnected") {
    val driver = mock[SchedulerDriver]

    eventBus.subscribe(probe.ref, classOf[SchedulerDisconnectedEvent])

    marathonScheduler.disconnected(driver)

    try {
      val msg = probe.expectMsgType[SchedulerDisconnectedEvent]

      assert(msg.eventType == "scheduler_disconnected_event")
    } finally {
      eventBus.unsubscribe(probe.ref)
    }

    // we **heavily** rely on driver.stop to delegate enforcement of leadership abdication,
    // so it's worth testing that this behavior isn't lost.
    verify(driver, times(1)).stop(true)

    noMoreInteractions(driver)
  }

  test("Suicide with an unknown error will not remove the framework id") {
    Given("A suicide call trap")
    val driver = mock[SchedulerDriver]
    var suicideCall: Option[Boolean] = None
    suicideFn = remove => { suicideCall = Some(remove) }

    When("An error is reported")
    marathonScheduler.error(driver, "some weird mesos message")

    Then("Suicide is called without removing the framework id")
    suicideCall should be(defined)
    suicideCall.get should be (false)
  }

  test("Suicide with a framework error will remove the framework id") {
    Given("A suicide call trap")
    val driver = mock[SchedulerDriver]
    var suicideCall: Option[Boolean] = None
    suicideFn = remove => { suicideCall = Some(remove) }

    When("An error is reported")
    marathonScheduler.error(driver, "Framework has been removed")

    Then("Suicide is called with removing the framework id")
    suicideCall should be(defined)
    suicideCall.get should be (true)
  }
}
