package mesosphere.marathon
package storage.migration.legacy

import akka.stream.scaladsl.Sink
import com.codahale.metrics.MetricRegistry
import mesosphere.marathon.core.pod.PodDefinition
import mesosphere.marathon.metrics.Metrics
import mesosphere.marathon.state.{ AppDefinition, Group, PathId, PortDefinitions, Timestamp, VersionInfo }
import mesosphere.marathon.storage.LegacyInMemConfig
import mesosphere.marathon.storage.repository.legacy.store.MarathonStore
import mesosphere.marathon.storage.repository.legacy.{ AppEntityRepository, GroupEntityRepository, PodEntityRepository }
import mesosphere.marathon.stream._
import mesosphere.marathon.test.{ GroupCreation, MarathonActorSupport }
import org.scalatest.{ GivenWhenThen, Matchers }

import scala.concurrent.ExecutionContext

class MigrationTo0_16Test extends MarathonActorSupport with GivenWhenThen with Matchers with GroupCreation {

  class Fixture {
    implicit val ctx = ExecutionContext.global
    implicit lazy val metrics = new Metrics(new MetricRegistry)
    val maxVersions = 25
    lazy val config = LegacyInMemConfig(maxVersions)
    lazy val store = config.store

    lazy val appStore = new MarathonStore[AppDefinition](store, metrics, () => AppDefinition(id = PathId("/test")), prefix = "app:")
    lazy val appRepo = new AppEntityRepository(appStore, maxVersions = maxVersions)(ExecutionContext.global, metrics)

    lazy val podStore = new MarathonStore[PodDefinition](store, metrics, () => PodDefinition(), prefix = "pod:")
    lazy val podRepo = new PodEntityRepository(podStore, maxVersions = maxVersions)(ExecutionContext.global, metrics)

    lazy val groupStore = new MarathonStore[Group](store, metrics, () => createRootGroup(), prefix = "group:")
    lazy val groupRepo = new GroupEntityRepository(groupStore, maxVersions = maxVersions, appRepo, podRepo)

    lazy val migration = new MigrationTo0_16(Some(config))
  }

  test("empty migration does nothing") {
    Given("no apps/groups")
    val f = new Fixture

    When("migrating")
    f.migration.migrate().futureValue

    Then("only an empty root Group is created")
    val rootGroup = f.groupRepo.root().futureValue
    rootGroup.groupsById should be('empty)
    rootGroup.apps should be('empty)
    rootGroup.dependencies should be('empty)
    f.appRepo.ids().runWith(Sink.seq).futureValue should be('empty)
  }

  test("an app and all its revisions are migrated") {
    import PathId._
    val f = new Fixture

    def appProtoInNewFormatAsserts(proto: Protos.ServiceDefinition) = {
      val ports = proto.getPortDefinitionsList.map(_.getNumber)
      assert(Seq(1000, 1001) == ports, ports)
      assert(proto.getPortsCount == 0)
    }

    def appProtoIsInNewFormat(version: Option[Long]): Unit = {
      def fetchAppProto(version: Option[Long]): Protos.ServiceDefinition = {
        version.fold {
          f.appRepo.get("test".toRootPath).futureValue.value.toProto
        } { v =>
          f.appRepo.getVersion("test".toRootPath, Timestamp(v).toOffsetDateTime).futureValue.value.toProto
        }
      }

      appProtoInNewFormatAsserts(fetchAppProto(version))
    }

    def groupProtoIsInNewFormat(version: Option[Long]): Unit = {
      def fetchGroupProto(version: Option[Long]): Protos.GroupDefinition = {
        version.fold {
          f.groupRepo.root().futureValue.toProto
        } { v =>
          f.groupRepo.rootVersion(Timestamp(v).toOffsetDateTime).futureValue.value.toProto
        }
      }

      val proto = fetchGroupProto(version)
      proto.getDeprecatedAppsList.foreach(appProtoInNewFormatAsserts)
    }

    val appV1 = deprecatedAppDefinition(1)
    val appV2 = deprecatedAppDefinition(2)

    f.appRepo.store(appV1).futureValue
    f.appRepo.store(appV2).futureValue

    val groupWithApp = createRootGroup(apps = Map(appV2.id -> appV2), version = Timestamp(2))
    f.groupRepo.storeRoot(groupWithApp, Nil, Nil, Nil, Nil).futureValue

    When("migrating")
    f.migration.migrate().futureValue

    Then("all the app protos must be in the new format")
    appProtoIsInNewFormat(None)
    appProtoIsInNewFormat(Some(1))
    appProtoIsInNewFormat(Some(2))

    Then("the apps in the group proto must be in the new format")
    groupProtoIsInNewFormat(None)
    groupProtoIsInNewFormat(Some(2))
  }

  test("A deprecatedAppDefinition serializes in the deprecated format") {
    val app = deprecatedAppDefinition()

    val proto = app.toProto

    proto.getPortDefinitionsCount should be(0)
    proto.getPortsList.toSet should be (Set(1000, 1001))
  }

  private[this] def deprecatedAppDefinition(version: Long = 0) =
    {
      class T extends AppDefinition(
        PathId("/test"),
        cmd = Some("true"),
        portDefinitions = PortDefinitions(1000, 1001),
        versionInfo = VersionInfo.OnlyVersion(Timestamp(version))
      ) with DeprecatedSerialization

      new T()
    }

  private[this] trait DeprecatedSerialization extends AppDefinition {
    override def toProto: Protos.ServiceDefinition = {
      val builder = super.toProto.toBuilder

      builder.getPortDefinitionsList.map(_.getNumber).map(builder.addPorts)
      builder.clearPortDefinitions()

      builder.build
    }
  }
}
