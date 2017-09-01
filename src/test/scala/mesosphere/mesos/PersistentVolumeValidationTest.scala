package mesosphere.mesos

import com.wix.accord._
import mesosphere.marathon.state.{ PersistentVolume, PersistentVolumeInfo, Volume }
import mesosphere.marathon.test.{ MarathonSpec, Mockito }
import org.apache.mesos
import org.scalatest.{ GivenWhenThen, Matchers }

class PersistentVolumeValidationTest extends MarathonSpec with GivenWhenThen with Mockito with Matchers {

  test("create a PersistentVolume with no validation violations") {
    Given("a PersistentVolume with no validation violations")
    val path = "path"
    val volume = PersistentVolume(path, PersistentVolumeInfo(1), mesos.Protos.Volume.Mode.RW)

    When("The volume is created and validation succeeded")
    volume should not be null
    val validation = Volume.validVolume(Set())(volume)

    Then("A validation exists with no error message")
    validation.isSuccess should be (true)
  }

  test("create a PersistentVolume with validation violation in containerPath") {
    Given("a PersistentVolume with validation violation in containerPath")
    val path = "/path"
    val volume = PersistentVolume(path, PersistentVolumeInfo(1), mesos.Protos.Volume.Mode.RW)

    When("The volume is created and validation failed")
    volume should not be null
    volume.containerPath should be (path)
    val validation = Volume.validVolume(Set())(volume)
    validation.isSuccess should be (false)

    Then("A validation exists with a readable error message")
    validation match {
      case Failure(violations) =>
        violations should contain (RuleViolation("/path", "value must not contain \"/\"", Some("containerPath")))
      case Success =>
        fail("validation should fail!")
    }
  }
}
