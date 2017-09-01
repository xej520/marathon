package mesosphere.marathon
package api.validation

import mesosphere.marathon.core.health._
import mesosphere.marathon.core.plugin.PluginManager
import mesosphere.marathon.state._
import mesosphere.marathon.test.MarathonSpec
import org.scalatest.{ GivenWhenThen, Matchers }

class AppDefinitionMesosHealthCheckValidationTest extends MarathonSpec with Matchers with GivenWhenThen {

  lazy val validAppDefinition = AppDefinition.validAppDefinition(Set.empty)(PluginManager.None)

  test("app with 0 Mesos health checks is valid") {
    val f = new Fixture
    Given("an app with only Marathon Health Checks")
    val app = f.app()

    Then("the app is considered valid")
    validAppDefinition(app).isSuccess shouldBe true
  }

  test("app with 1 Mesos health check is valid") {
    val f = new Fixture
    Given("an app with one health check")
    val app = f.app(healthChecks = Set(MesosCommandHealthCheck(command = Command("true"))))

    Then("the app is considered valid")
    validAppDefinition(app).isSuccess shouldBe true
  }

  test("app with more than 1 non-command Mesos health check is invalid") {
    val f = new Fixture
    Given("an app with one health check")
    val app = f.app(healthChecks = Set(MesosHttpHealthCheck(port = Some(80)), MesosHttpHealthCheck()))

    Then("the app is considered invalid")
    validAppDefinition(app).isFailure shouldBe true
  }

  test("app with more than 1 command Mesos health check is valid") {
    val f = new Fixture
    Given("an app with one health check")
    val app = f.app(healthChecks = Set(
      MesosCommandHealthCheck(command = Command("true")),
      MesosCommandHealthCheck(command = Command("true"))))

    Then("the app is considered valid")
    validAppDefinition(app).isSuccess shouldBe true
  }

  test("health check with port validates port references") {
    val f = new Fixture
    Given("an app with one Mesos Health Check but without port")
    val mesosHealthByIndex = f.app(Set(MesosHttpHealthCheck(portIndex = Some(PortReference.ByIndex(0)))))
    val marathonHealthByIndex = f.app(Set(MesosHttpHealthCheck(portIndex = Some(PortReference.ByIndex(0)))))
    val mesosHealthNoPort = mesosHealthByIndex.copy(portDefinitions = Seq.empty)
    val marathonHealthNoPort = marathonHealthByIndex.copy(portDefinitions = Seq.empty)

    Then("the app is considered valid")
    validAppDefinition(mesosHealthByIndex).isSuccess shouldBe true
    validAppDefinition(marathonHealthByIndex).isSuccess shouldBe true
    validAppDefinition(mesosHealthNoPort).isSuccess shouldBe false
    validAppDefinition(marathonHealthNoPort).isSuccess shouldBe false
  }

  class Fixture {
    def app(healthChecks: Set[_ <: HealthCheck] = Set(MarathonHttpHealthCheck())): AppDefinition =
      AppDefinition(
        id = PathId("/test"),
        cmd = Some("sleep 1000"),
        instances = 1,
        healthChecks = healthChecks
      )
  }
}
