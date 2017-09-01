package mesosphere.marathon
package state

import mesosphere.marathon.Protos.ServiceDefinition
import mesosphere.marathon.raml.Resources
import mesosphere.marathon.state.EnvVarValue._
import mesosphere.marathon.state.PathId._
import mesosphere.marathon.stream._
import mesosphere.marathon.test.MarathonSpec
import org.apache.mesos.{ Protos => mesos }
import org.scalatest.Matchers
import scala.concurrent.duration._

class AppDefinitionTest extends MarathonSpec with Matchers {

  val fullVersion = VersionInfo.forNewConfig(Timestamp(1))
  val runSpecId = PathId("/test")

  test("ToProto with port definitions") {
    val app1 = AppDefinition(
      id = "play".toPath,
      cmd = Some("bash foo-*/start -Dhttp.port=$PORT"),
      resources = Resources(cpus = 4.0, mem = 256.0),
      instances = 5,
      portDefinitions = PortDefinitions(8080, 8081),
      executor = "//cmd",
      acceptedResourceRoles = Set("a", "b")
    )

    val proto1 = app1.toProto
    assert("play" == proto1.getId)
    assert(proto1.getCmd.hasValue)
    assert(proto1.getCmd.getShell)
    assert("bash foo-*/start -Dhttp.port=$PORT" == proto1.getCmd.getValue)
    assert(5 == proto1.getInstances)
    assert(Seq(8080, 8081) == proto1.getPortDefinitionsList.map(_.getNumber))
    assert("//cmd" == proto1.getExecutor)
    assert(4 == getScalarResourceValue(proto1, "cpus"), 1e-6)
    assert(256 == getScalarResourceValue(proto1, "mem"), 1e-6)
    assert("bash foo-*/start -Dhttp.port=$PORT" == proto1.getCmd.getValue)
    assert(!proto1.hasContainer)
    assert(1.0 == proto1.getUpgradeStrategy.getMinimumHealthCapacity)
    assert(1.0 == proto1.getUpgradeStrategy.getMaximumOverCapacity)
    assert(proto1.hasAcceptedResourceRoles)
    assert(proto1.getAcceptedResourceRoles == Protos.ResourceRoles.newBuilder().addRole("a").addRole("b").build())

    val app2 = AppDefinition(
      id = "play".toPath,
      cmd = None,
      args = Seq("a", "b", "c"),
      container = Some(Container.Docker(image = "group/image")),
      resources = Resources(cpus = 4.0, mem = 256.0),
      instances = 5,
      portDefinitions = PortDefinitions(8080, 8081),
      executor = "//cmd",
      upgradeStrategy = UpgradeStrategy(0.7, 0.4)
    )

    val proto2 = app2.toProto
    assert("play" == proto2.getId)
    assert(!proto2.getCmd.hasValue)
    assert(!proto2.getCmd.getShell)
    proto2.getCmd.getArgumentsList should contain theSameElementsInOrderAs Seq("a", "b", "c")
    assert(5 == proto2.getInstances)
    assert(Seq(8080, 8081) == proto2.getPortDefinitionsList.map(_.getNumber))
    assert("//cmd" == proto2.getExecutor)
    assert(4 == getScalarResourceValue(proto2, "cpus"), 1e-6)
    assert(256 == getScalarResourceValue(proto2, "mem"), 1e-6)
    assert(proto2.hasContainer)
    assert(0.7 == proto2.getUpgradeStrategy.getMinimumHealthCapacity)
    assert(0.4 == proto2.getUpgradeStrategy.getMaximumOverCapacity)
    assert(0 == proto2.getAcceptedResourceRoles.getRoleCount)
  }

  test("CMD to proto and back again") {
    val app = AppDefinition(
      id = "play".toPath,
      cmd = Some("bash foo-*/start -Dhttp.port=$PORT"),
      versionInfo = fullVersion
    )

    val proto = app.toProto
    proto.getId should be("play")
    proto.getCmd.hasValue should be(true)
    proto.getCmd.getShell should be(true)
    proto.getCmd.getValue should be("bash foo-*/start -Dhttp.port=$PORT")

    val read = AppDefinition(id = runSpecId).mergeFromProto(proto)
    read should be(app)
  }

  test("ARGS to proto and back again") {
    val app = AppDefinition(
      id = "play".toPath,
      args = Seq("bash", "foo-*/start", "-Dhttp.port=$PORT"),
      versionInfo = fullVersion
    )

    val proto = app.toProto
    proto.getId should be("play")
    proto.getCmd.hasValue should be(true)
    proto.getCmd.getShell should be(false)
    proto.getCmd.getValue should be("bash")
    proto.getCmd.getArgumentsList should contain theSameElementsInOrderAs Seq("bash", "foo-*/start", "-Dhttp.port=$PORT")

    val read = AppDefinition(id = runSpecId).mergeFromProto(proto)
    read should be(app)
  }

  test("ipAddress to proto and back again") {
    val app = AppDefinition(
      id = "app-with-ip-address".toPath,
      cmd = Some("sleep 30"),
      portDefinitions = Nil,
      ipAddress = Some(
        IpAddress(
          groups = Seq("a", "b", "c"),
          labels = Map(
            "foo" -> "bar",
            "baz" -> "buzz"
          )
        )
      )
    )

    val proto = app.toProto
    proto.getId should be("app-with-ip-address")
    proto.hasIpAddress should be (true)

    val read = AppDefinition(id = runSpecId).mergeFromProto(proto)
    read should be(app)
  }

  test("ipAddress to proto and back again w/ Docker container w/ virtual networking") {
    val app = AppDefinition(
      id = "app-with-ip-address".toPath,
      cmd = Some("sleep 30"),
      portDefinitions = Nil,
      ipAddress = Some(
        IpAddress(
          groups = Seq("a", "b", "c"),
          labels = Map(
            "foo" -> "bar",
            "baz" -> "buzz"
          ),
          networkName = Some("blahze")
        )
      ),
      container = Some(Container.Docker(
        image = "jdef/foo",
        network = Some(mesos.ContainerInfo.DockerInfo.Network.USER),
        portMappings = Seq(
          Container.PortMapping(hostPort = None),
          Container.PortMapping(hostPort = Some(123)),
          Container.PortMapping(containerPort = 1, hostPort = Some(234), protocol = "udp")
        )
      ))
    )

    val proto = app.toProto
    proto.getId should be("app-with-ip-address")
    proto.hasIpAddress should be (true)

    val read = AppDefinition(id = runSpecId).mergeFromProto(proto)
    read should be(app)
  }

  test("ipAddress to proto and back again w/ Docker container w/ bridge") {
    val app = AppDefinition(
      id = "app-with-ip-address".toPath,
      cmd = Some("sleep 30"),
      portDefinitions = Nil,
      container = Some(Container.Docker(
        image = "jdef/foo",
        network = Some(mesos.ContainerInfo.DockerInfo.Network.BRIDGE),
        portMappings = Seq(
          Container.PortMapping(hostPort = Some(0)),
          Container.PortMapping(hostPort = Some(123)),
          Container.PortMapping(containerPort = 1, hostPort = Some(234), protocol = "udp")
        )
      ))
    )

    val proto = app.toProto
    proto.getId should be("app-with-ip-address")
    proto.hasIpAddress should be (false)

    val read = AppDefinition(id = runSpecId).mergeFromProto(proto)
    read should be(app)
  }

  test("ipAddress discovery to proto and back again") {
    val app = AppDefinition(
      id = "app-with-ip-address".toPath,
      cmd = Some("sleep 30"),
      portDefinitions = Nil,
      ipAddress = Some(
        IpAddress(
          groups = Seq("a", "b", "c"),
          labels = Map(
            "foo" -> "bar",
            "baz" -> "buzz"
          ),
          discoveryInfo = DiscoveryInfo(
            ports = Vector(DiscoveryInfo.Port(name = "http", number = 80, protocol = "tcp"))
          )
        )
      )
    )

    val proto = app.toProto

    proto.getIpAddress.hasDiscoveryInfo should be (true)
    proto.getIpAddress.getDiscoveryInfo.getPortsList.size() should be (1)
    val read = AppDefinition(id = runSpecId).mergeFromProto(proto)
    read should equal(app)
  }

  test("MergeFromProto") {
    val cmd = mesos.CommandInfo.newBuilder
      .setValue("bash foo-*/start -Dhttp.port=$PORT")

    val proto1 = ServiceDefinition.newBuilder
      .setId("play")
      .setCmd(cmd)
      .setInstances(3)
      .setExecutor("//cmd")
      .setVersion(Timestamp.now().toString)
      .build

    val app1 = AppDefinition(id = runSpecId).mergeFromProto(proto1)

    assert("play" == app1.id.toString)
    assert(3 == app1.instances)
    assert("//cmd" == app1.executor)
    assert(Some("bash foo-*/start -Dhttp.port=$PORT") == app1.cmd)
  }

  test("Read obsolete ports from proto") {
    val cmd = mesos.CommandInfo.newBuilder.setValue("bash foo-*/start -Dhttp.port=$PORT")

    val proto1 = ServiceDefinition.newBuilder
      .setId("/app")
      .setCmd(cmd)
      .setInstances(1)
      .setExecutor("//cmd")
      .setVersion(Timestamp.now().toString)
      .addPorts(1000)
      .addPorts(1001)
      .build

    val app = AppDefinition(id = runSpecId).mergeFromProto(proto1)

    assert(PortDefinitions(1000, 1001) == app.portDefinitions)
  }

  test("ProtoRoundtrip") {
    val app1 = AppDefinition(
      id = "play".toPath,
      cmd = Some("bash foo-*/start -Dhttp.port=$PORT"),
      resources = Resources(cpus = 4.0, mem = 256.0),
      instances = 5,
      portDefinitions = PortDefinitions(8080, 8081),
      executor = "//cmd",
      labels = Map(
        "one" -> "aaa",
        "two" -> "bbb",
        "three" -> "ccc"
      ),
      versionInfo = fullVersion,
      unreachableStrategy = UnreachableEnabled(inactiveAfter = 998.seconds, expungeAfter = 999.seconds),
      killSelection = KillSelection.OldestFirst
    )
    val result1 = AppDefinition(id = runSpecId).mergeFromProto(app1.toProto)
    assert(result1 == app1)

    val app2 = AppDefinition(
      id = runSpecId,
      cmd = None,
      args = Seq("a", "b", "c"),
      versionInfo = fullVersion
    )
    val result2 = AppDefinition(id = runSpecId).mergeFromProto(app2.toProto)
    assert(result2 == app2)
  }

  test("ProtoRoundtrip for secrets") {
    val app = AppDefinition(
      id = runSpecId,
      cmd = None,
      secrets = Map[String, Secret](
        "psst" -> Secret("/something/secret")
      ),
      env = Map[String, EnvVarValue](
        "foo" -> "bar".toEnvVar,
        "ssh" -> EnvVarSecretRef("psst")
      ),
      versionInfo = fullVersion
    )
    val result = AppDefinition(id = runSpecId).mergeFromProto(app.toProto)
    assert(result == app, s"expected $app instead of $result")
  }

  def getScalarResourceValue(proto: ServiceDefinition, name: String) = {
    proto.getResourcesList
      .find(_.getName == name)
      .get.getScalar.getValue
  }
}
