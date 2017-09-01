package mesosphere.marathon
package api.v2.json

import com.wix.accord._
import mesosphere.marathon.api.JsonTestHelper
import mesosphere.marathon.api.v2.ValidationHelper
import mesosphere.marathon.core.health.HealthCheck
import mesosphere.marathon.core.readiness.ReadinessCheckTestHelper
import mesosphere.marathon.state.Container._
import mesosphere.marathon.state.DiscoveryInfo.Port
import mesosphere.marathon.state.PathId._
import mesosphere.marathon.state._
import mesosphere.marathon.test.MarathonSpec
import org.scalatest.Matchers
import play.api.data.validation.ValidationError
import play.api.libs.json.{ JsError, JsPath, Json }

import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.util.Try

class AppUpdateTest extends MarathonSpec with Matchers {
  import Formats._
  import mesosphere.marathon.integration.setup.V2TestFormats._

  implicit val appUpdateValidator = AppUpdate.appUpdateValidator(Set())
  val runSpecId = PathId("/test")

  def shouldViolate(update: AppUpdate, path: String, template: String): Unit = {
    val violations = validate(update)
    assert(violations.isFailure)
    assert(ValidationHelper.getAllRuleConstrains(violations).exists(v =>
      v.path.getOrElse(false) == path && v.message == template
    ), s"expected path $path and message $template, but instead found" +
      ValidationHelper.getAllRuleConstrains(violations))
  }

  def shouldNotViolate(update: AppUpdate, path: String, template: String): Unit = {
    val violations = validate(update)
    assert(!ValidationHelper.getAllRuleConstrains(violations).exists(v =>
      v.path.getOrElse(false) == path && v.message == template))
  }

  test("Validation") {
    val update = AppUpdate()

    shouldViolate(
      update.copy(portDefinitions = Some(PortDefinitions(9000, 8080, 9000))),
      "/portDefinitions",
      "Ports must be unique."
    )

    shouldViolate(
      update.copy(portDefinitions = Some(Seq(
        PortDefinition(port = 9000, name = Some("foo")),
        PortDefinition(port = 9001, name = Some("foo"))))
      ),
      "/portDefinitions",
      "Port names must be unique."
    )

    shouldNotViolate(
      update.copy(portDefinitions = Some(Seq(
        PortDefinition(port = 9000, name = Some("foo")),
        PortDefinition(port = 9001, name = Some("bar"))))
      ),
      "/portDefinitions",
      "Port names must be unique."
    )

    shouldViolate(update.copy(mem = Some(-3.0)), "/mem", "got -3.0, expected 0.0 or more")
    shouldViolate(update.copy(cpus = Some(-3.0)), "/cpus", "got -3.0, expected 0.0 or more")
    shouldViolate(update.copy(disk = Some(-3.0)), "/disk", "got -3.0, expected 0.0 or more")
    shouldViolate(update.copy(instances = Some(-3)), "/instances", "got -3, expected 0 or more")
  }

  test("Validate secrets") {
    val update = AppUpdate()

    shouldViolate(update.copy(secrets = Some(Map(
      "a" -> Secret("")
    ))), "/secrets(a)/source", "must not be empty")

    shouldViolate(update.copy(secrets = Some(Map(
      "" -> Secret("a/b/c")
    ))), "/secrets()", "must not be empty")
  }

  private[this] def fromJsonString(json: String): AppUpdate = {
    Json.fromJson[AppUpdate](Json.parse(json)).get
  }

  test("SerializationRoundtrip for empty definition") {
    val update0 = AppUpdate(container = Some(Container.Mesos()))
    JsonTestHelper.assertSerializationRoundtripWorks(update0)
  }

  test("SerializationRoundtrip for definition with simple AppC container") {
    val update0 = AppUpdate(container = Some(Container.MesosAppC(
      image = "anImage",
      labels = Map("key" -> "foo", "value" -> "bar")
    )))
    JsonTestHelper.assertSerializationRoundtripWorks(update0)
  }

  test("SerializationRoundtrip for extended definition") {
    val update1 = AppUpdate(
      cmd = Some("sleep 60"),
      args = None,
      user = Some("nobody"),
      env = Some(EnvVarValue(Map("LANG" -> "en-US"))),
      instances = Some(16),
      cpus = Some(2.0),
      mem = Some(256.0),
      disk = Some(1024.0),
      executor = Some("/opt/executors/bin/some.executor"),
      constraints = Some(Set()),
      fetch = Some(Seq(FetchUri(uri = "http://dl.corp.org/prodX-1.2.3.tgz"))),
      backoff = Some(2.seconds),
      backoffFactor = Some(1.2),
      maxLaunchDelay = Some(1.minutes),
      container = Some(Docker(
        volumes = Nil,
        image = "docker:///group/image"
      )),
      healthChecks = Some(Set[HealthCheck]()),
      taskKillGracePeriod = Some(2.seconds),
      dependencies = Some(Set[PathId]()),
      upgradeStrategy = Some(UpgradeStrategy.empty),
      labels = Some(
        Map(
          "one" -> "aaa",
          "two" -> "bbb",
          "three" -> "ccc"
        )
      ),
      ipAddress = Some(IpAddress(
        groups = Seq("a", "b", "c"),
        labels = Map(
          "foo" -> "bar",
          "baz" -> "buzz"
        ),
        discoveryInfo = DiscoveryInfo(
          ports = Seq(Port(name = "http", number = 80, protocol = "tcp"))
        )
      )),
      unreachableStrategy = Some(UnreachableEnabled(998.seconds, 999.seconds))
    )
    JsonTestHelper.assertSerializationRoundtripWorks(update1)
  }

  test("Serialization result of empty container") {
    val update2 = AppUpdate(container = None)
    val json2 =
      """
      {
        "cmd": null,
        "user": null,
        "env": null,
        "instances": null,
        "cpus": null,
        "mem": null,
        "disk": null,
        "executor": null,
        "constraints": null,
        "uris": null,
        "ports": null,
        "backoffSeconds": null,
        "backoffFactor": null,
        "container": null,
        "healthChecks": null,
        "dependencies": null,
        "version": null
      }
    """
    val readResult2 = fromJsonString(json2)
    assert(readResult2 == update2)
  }

  test("Serialization result of empty ipAddress") {
    val update2 = AppUpdate(ipAddress = None)
    val json2 =
      """
      {
        "cmd": null,
        "user": null,
        "env": null,
        "instances": null,
        "cpus": null,
        "mem": null,
        "disk": null,
        "executor": null,
        "constraints": null,
        "uris": null,
        "ports": null,
        "backoffSeconds": null,
        "backoffFactor": null,
        "container": null,
        "healthChecks": null,
        "dependencies": null,
        "ipAddress": null,
        "version": null
      }
      """
    val readResult2 = fromJsonString(json2)
    assert(readResult2 == update2)
  }

  test("Empty json corresponds to default instance") {
    val update3 = AppUpdate()
    val json3 = "{}"
    val readResult3 = fromJsonString(json3)
    assert(readResult3 == update3)
  }

  test("Args are correctly read") {
    val update4 = AppUpdate(args = Some(Seq("a", "b", "c")))
    val json4 = """{ "args": ["a", "b", "c"] }"""
    val readResult4 = fromJsonString(json4)
    assert(readResult4 == update4)
  }

  test("'version' field can only be combined with 'id'") {
    assert(AppUpdate(version = Some(Timestamp.now())).onlyVersionOrIdSet)

    assert(AppUpdate(id = Some("foo".toPath), version = Some(Timestamp.now())).onlyVersionOrIdSet)

    intercept[IllegalArgumentException] {
      AppUpdate(cmd = Some("foo"), version = Some(Timestamp.now()))
    }
  }

  test("acceptedResourceRoles of update is only applied when != None") {
    val app = AppDefinition(id = PathId("withAcceptedRoles"), acceptedResourceRoles = Set("a"))

    val unchanged = AppUpdate().apply(app).copy(versionInfo = app.versionInfo)
    assert(unchanged == app)

    val changed = AppUpdate(acceptedResourceRoles = Some(Set("b"))).apply(app).copy(versionInfo = app.versionInfo)
    assert(changed == app.copy(acceptedResourceRoles = Set("b")))
  }

  test("AppUpdate does not change existing versionInfo") {
    val app = AppDefinition(
      id = PathId("test"),
      cmd = Some("sleep 1"),
      versionInfo = VersionInfo.forNewConfig(Timestamp(1))
    )

    val updateCmd = AppUpdate(cmd = Some("sleep 2"))
    assert(updateCmd(app).versionInfo == app.versionInfo)
  }

  test("AppUpdate with a version and other changes are not allowed") {
    val attempt = Try(AppUpdate(id = Some(PathId("/test")), cmd = Some("sleep 2"), version = Some(Timestamp(2))))
    assert(attempt.failed.get.getMessage.contains("The 'version' field may only be combined with the 'id' field."))
  }

  test("update may not have both uris and fetch") {
    val json =
      """
      {
        "id": "app-with-network-isolation",
        "uris": ["http://example.com/file1.tar.gz"],
        "fetch": [{"uri": "http://example.com/file1.tar.gz"}]
      }
      """

    import Formats._
    val result = Json.fromJson[AppUpdate](Json.parse(json))
    assert(result == JsError(ValidationError("You cannot specify both uris and fetch fields")))
  }

  test("update may not have both ports and portDefinitions") {
    val json =
      """
      {
        "id": "app",
        "ports": [1],
        "portDefinitions": [{"port": 2}]
      }
      """

    import Formats._
    val result = Json.fromJson[AppUpdate](Json.parse(json))
    assert(result == JsError(ValidationError("You cannot specify both ports and port definitions")))
  }

  test("update may not have duplicated ports") {
    val json =
      """
      {
        "id": "app",
        "ports": [1, 1]
      }
      """

    import Formats._
    val result = Json.fromJson[AppUpdate](Json.parse(json))
    assert(result == JsError(JsPath \ "ports", ValidationError("Ports must be unique.")))
  }

  test("update JSON serialization preserves readiness checks") {
    val update = AppUpdate(
      id = Some(PathId("/test")),
      readinessChecks = Some(Seq(ReadinessCheckTestHelper.alternativeHttps))
    )
    val json = Json.toJson(update)
    val reread = json.as[AppUpdate]
    assert(reread == update)
  }

  test("update readiness checks are applied to app") {
    val update = AppUpdate(
      id = Some(PathId("/test")),
      readinessChecks = Some(Seq(ReadinessCheckTestHelper.alternativeHttps))
    )
    val app = AppDefinition(id = PathId("/test"))
    val updated = update(app)

    assert(updated.readinessChecks == update.readinessChecks.get)
  }

  test("empty app updateStrategy on persistent volumes") {
    val json =
      """
      {
        "cmd": "sleep 1000",
        "container": {
          "type": "MESOS",
          "volumes": [
            {
              "containerPath": "home",
              "mode": "RW",
              "persistent": {
                "size": 100
                }
              }]
        },
        "residency": {
          "relaunchEscalationTimeoutSeconds": 10,
          "taskLostBehavior": "WAIT_FOREVER"
        }
      }
      """

    val update = fromJsonString(json)
    val strategy = update.empty("foo".toPath).upgradeStrategy
    assert(strategy.minimumHealthCapacity == 0.5
      && strategy.maximumOverCapacity == 0)
  }

  test("empty app residency on persistent volumes") {
    val json =
      """
      {
        "cmd": "sleep 1000",
        "container": {
          "type": "MESOS",
          "volumes": [
            {
              "containerPath": "home",
              "mode": "RW",
              "persistent": {
                "size": 100
                }
              }]
        },
        "upgradeStrategy": {
          "minimumHealthCapacity": 0.2,
          "maximumOverCapacity": 0
        }
      }
      """

    val update = fromJsonString(json)
    val residency = update.empty("foo".toPath).residency
    assert(residency.isDefined)
    assert(residency.forall(_ == Residency.defaultResidency))
  }

  test("empty app updateStrategy") {
    val json =
      """
      {
        "cmd": "sleep 1000",
        "container": {
          "type": "MESOS",
          "volumes": [
            {
              "containerPath": "home",
              "mode": "RW",
              "persistent": {
                "size": 100
                }
              }]
        },
        "residency": {
          "relaunchEscalationTimeoutSeconds": 10,
          "taskLostBehavior": "WAIT_FOREVER"
        }
      }
      """

    val update = fromJsonString(json)
    val strategy = update.empty("foo".toPath).upgradeStrategy
    assert(strategy.minimumHealthCapacity == 0.5
      && strategy.maximumOverCapacity == 0)
  }

  test("empty app persists container") {
    val json =
      """
        {
          "id": "/payload-id",
          "args": [],
          "container": {
            "type": "DOCKER",
            "volumes": [
              {
                "containerPath": "data",
                "mode": "RW",
                "persistent": {
                  "size": 100
                }
              }
            ],
            "docker": {
              "image": "anImage"
            }
          },
          "residency": {
            "taskLostBehavior": "WAIT_FOREVER",
            "relaunchEscalationTimeoutSeconds": 3600
          }
        }
      """

    val update = fromJsonString(json)
    val create = update.empty("/put-path-id".toPath)
    assert(update.container.isDefined)
    assert(update.container == create.container)
  }

  test("empty app persists existing residency") {
    val json =
      """
        {
          "id": "/app",
          "args": [],
          "container": {
            "type": "DOCKER",
            "volumes": [
              {
                "containerPath": "data",
                "mode": "RW",
                "persistent": {
                  "size": 100
                }
              }
            ],
            "docker": {
              "image": "anImage"
            }
          },
          "residency": {
            "taskLostBehavior": "WAIT_FOREVER",
            "relaunchEscalationTimeoutSeconds": 1234
          }
        }
      """

    val update = fromJsonString(json)
    val create = update.empty("/app".toPath)
    assert(update.residency.isDefined)
    assert(update.residency == create.residency)
  }

  test("empty app persists existing upgradeStrategy") {
    val json =
      """
        {
          "id": "/app",
          "args": [],
          "container": {
            "type": "DOCKER",
            "volumes": [
              {
                "containerPath": "data",
                "mode": "RW",
                "persistent": {
                  "size": 100
                }
              }
            ],
            "docker": {
              "image": "anImage"
            }
          },
          "residency": {
            "taskLostBehavior": "WAIT_FOREVER",
            "relaunchEscalationTimeoutSeconds": 1234
          },
          "upgradeStrategy": {
            "minimumHealthCapacity": 0.1,
            "maximumOverCapacity": 0.0
          }
        }
      """

    val update = fromJsonString(json)
    val create = update.empty("/app".toPath)
    assert(update.upgradeStrategy.isDefined)
    assert(update.upgradeStrategy.get == create.upgradeStrategy)
  }

  test("empty app residency") {
    val json =
      """
      {
        "cmd": "sleep 1000",
        "container": {
          "type": "MESOS",
          "volumes": [
            {
              "containerPath": "home",
              "mode": "RW",
              "persistent": {
                "size": 100
                }
              }]
        },
        "upgradeStrategy": {
          "minimumHealthCapacity": 0.2,
          "maximumOverCapacity": 0
        }
      }
      """

    val update = fromJsonString(json)
    val residency = update.empty("foo".toPath).residency
    assert(residency.isDefined)
    assert(residency.forall(_ == Residency.defaultResidency))
  }

  test("empty app update strategy on external volumes") {
    val json =
      """
      {
        "cmd": "sleep 1000",
        "container": {
          "type": "MESOS",
          "volumes": [
            {
              "containerPath": "/docker_storage",
              "mode": "RW",
              "external": {
                "name": "my-external-volume",
                "provider": "dvdi",
                "size": 1234
                }
              }]
        }
      }
      """

    val update = fromJsonString(json)
    val strategy = update.empty("foo".toPath).upgradeStrategy
    assert(strategy == UpgradeStrategy.forResidentTasks)
  }

  test("container change in AppUpdate should be stored") {
    val appDef = AppDefinition(id = runSpecId, container = Some(Docker()))
    val appUpdate = AppUpdate(container = Some(Docker(portMappings = Seq(
      Container.PortMapping(containerPort = 4000, protocol = "tcp")
    ))))
    val roundTrip = appUpdate(appDef)
    roundTrip.container.get.portMappings should have size 1
    roundTrip.container.get.portMappings.head.containerPort should be (4000)
  }

  test("app update changes kill selection") {
    val appDef = AppDefinition(id = runSpecId, killSelection = KillSelection.YoungestFirst)
    val update = AppUpdate(killSelection = Some(KillSelection.OldestFirst))
    val result = update(appDef)
    result.killSelection should be(KillSelection.OldestFirst)
  }
}
