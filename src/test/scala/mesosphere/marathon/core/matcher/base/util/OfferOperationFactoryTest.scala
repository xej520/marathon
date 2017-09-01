package mesosphere.marathon
package core.matcher.base.util

import mesosphere.marathon.core.task.Task
import mesosphere.marathon.state.{ DiskSource, PathId, PersistentVolume, PersistentVolumeInfo }
import mesosphere.marathon.stream._
import mesosphere.marathon.test.{ MarathonSpec, MarathonTestHelper, Mockito }
import org.apache.mesos.{ Protos => Mesos }
import org.scalatest.{ GivenWhenThen, Matchers }

class OfferOperationFactoryTest extends MarathonSpec with GivenWhenThen with Mockito with Matchers {

  test("Launch operation succeeds even if principal/role are not set") {
    val f = new Fixture

    Given("a factory without principal or role")
    val factory = new OfferOperationFactory(None, None)
    val taskInfo = MarathonTestHelper.makeOneCPUTask(Task.Id.forRunSpec(f.runSpecId)).build()

    When("We create a launch operation")
    val operation = factory.launch(taskInfo)

    Then("the Offer Operation is created")
    operation.hasLaunch shouldEqual true
    operation.getLaunch.getTaskInfos(0) shouldEqual taskInfo
  }

  test("Reserve operation fails when role is not set") {
    val f = new Fixture

    Given("a factory without role")
    val factory = new OfferOperationFactory(Some("principal"), None)

    When("We create a reserve operation")
    val error = intercept[WrongConfigurationException] {
      factory.reserve(f.frameworkId, Task.Id.forRunSpec(PathId("/test")), Seq(Mesos.Resource.getDefaultInstance))
    }

    Then("A meaningful exception is thrown")
    error.getMessage should startWith ("No role set")
  }

  test("Reserve operation succeeds") {
    val f = new Fixture

    Given("A simple task")
    val factory = new OfferOperationFactory(Some("principal"), Some("role"))
    val task = MarathonTestHelper.makeOneCPUTask(Task.Id.forRunSpec(f.runSpecId))

    When("We create a reserve operation")
    val operation = factory.reserve(f.frameworkId, Task.Id(task.getTaskId), task.getResourcesList.to[Seq])

    Then("The operation is as expected")
    operation.getType shouldEqual Mesos.Offer.Operation.Type.RESERVE
    operation.hasReserve shouldEqual true
    operation.getReserve.getResourcesCount shouldEqual task.getResourcesCount

    And("The resource is reserved")
    val resource = operation.getReserve.getResources(0)
    resource.getName shouldEqual "cpus"
    resource.getType shouldEqual Mesos.Value.Type.SCALAR
    resource.getScalar.getValue shouldEqual 1
    resource.getRole shouldEqual "role"
    resource.hasReservation shouldEqual true
    resource.getReservation.getPrincipal shouldEqual "principal"
  }

  test("CreateVolumes operation succeeds") {
    val f = new Fixture

    Given("a factory without principal")
    val factory = new OfferOperationFactory(Some("principal"), Some("role"))
    val task = MarathonTestHelper.makeOneCPUTask(Task.Id.forRunSpec(f.runSpecId))
    val volumes = Seq(f.localVolume("mount"))
    val resource = MarathonTestHelper.scalarResource("disk", 1024)

    When("We create a reserve operation")
    val operation = factory.createVolumes(f.frameworkId, Task.Id(task.getTaskId), volumes.map(v => (DiskSource.root, v)))

    Then("The operation is as expected")
    operation.getType shouldEqual Mesos.Offer.Operation.Type.CREATE
    operation.hasCreate shouldEqual true
    operation.getCreate.getVolumesCount shouldEqual volumes.size

    And("The volumes are correct")
    val volume = operation.getCreate.getVolumes(0)
    val originalVolume = volumes.head
    volume.getName shouldEqual "disk"
    volume.getRole shouldEqual "role"
    volume.getScalar.getValue shouldEqual 10
    volume.hasReservation shouldEqual true
    volume.getReservation.getPrincipal shouldEqual "principal"
    volume.hasDisk shouldEqual true
    volume.getDisk.hasPersistence shouldEqual true
    volume.getDisk.getPersistence.getId shouldEqual originalVolume.id.idString
    volume.getDisk.hasVolume shouldEqual true
    volume.getDisk.getVolume.getContainerPath shouldEqual originalVolume.persistentVolume.containerPath
    volume.getDisk.getVolume.getMode shouldEqual originalVolume.persistentVolume.mode
  }

  class Fixture {
    val runSpecId = PathId("/my-app")
    val frameworkId = MarathonTestHelper.frameworkId
    val principal = Some("principal")
    val role = Some("role")
    val factory = new OfferOperationFactory(principal, role)

    def localVolume(containerPath: String): Task.LocalVolume = {
      val pv = PersistentVolume(
        containerPath = containerPath,
        persistent = PersistentVolumeInfo(size = 10),
        mode = Mesos.Volume.Mode.RW)
      Task.LocalVolume(Task.LocalVolumeId(runSpecId, pv), pv)
    }
  }
}
