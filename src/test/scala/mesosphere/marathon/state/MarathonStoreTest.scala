package mesosphere.marathon
package state

import com.codahale.metrics.MetricRegistry
import mesosphere.marathon.StoreCommandFailedException
import mesosphere.marathon.metrics.Metrics
import mesosphere.marathon.state.PathId._
import mesosphere.marathon.storage.repository.legacy.store.{ InMemoryStore, MarathonStore, PersistentEntity, PersistentStore }
import mesosphere.marathon.test.MarathonSpec
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.Matchers

import scala.collection.immutable.Seq
import scala.concurrent._
import scala.concurrent.duration._

class MarathonStoreTest extends MarathonSpec with Matchers {
  var metrics: Metrics = _
  var runSpecId = PathId("/test")

  before {
    metrics = new Metrics(new MetricRegistry)
  }

  test("Fetch") {
    val state = mock[PersistentStore]
    val variable = mock[PersistentEntity]
    val now = Timestamp.now()
    val appDef = AppDefinition(id = "testApp".toPath, args = Seq("arg"),
      versionInfo = VersionInfo.forNewConfig(now))

    when(variable.bytes).thenReturn(appDef.toProtoByteArray.toIndexedSeq)
    when(state.load("app:testApp")).thenReturn(Future.successful(Some(variable)))
    val store = new MarathonStore[AppDefinition](state, metrics, () => AppDefinition(id = runSpecId), "app:")
    val res = store.fetch("testApp")

    verify(state).load("app:testApp")
    assert(Some(appDef) == Await.result(res, 5.seconds), "Should return the expected AppDef")
  }

  test("FetchFail") {
    val state = mock[PersistentStore]

    when(state.load("app:testApp")).thenReturn(Future.failed(new StoreCommandFailedException("failed")))

    val store = new MarathonStore[AppDefinition](state, metrics, () => AppDefinition(id = runSpecId), "app:")
    val res = store.fetch("testApp")

    verify(state).load("app:testApp")

    intercept[StoreCommandFailedException] {
      Await.result(res, 5.seconds)
    }
  }

  test("FetchInvalidProto") {
    val state = mock[PersistentStore]
    val variable = mock[PersistentEntity]
    when(variable.bytes).thenReturn(IndexedSeq[Byte](1, 1, 1))

    when(state.load("app:testApp")).thenReturn(Future.successful(Some(variable)))
    val store = new MarathonStore[AppDefinition](state, metrics, () => AppDefinition(PathId.empty), "app:")
    val res = store.fetch("testApp")
    verify(state).load("app:testApp")
    res.futureValue should be ('empty)
  }

  test("Modify") {
    val state = mock[PersistentStore]
    val variable = mock[PersistentEntity]
    val now = Timestamp.now()
    val appDef = AppDefinition(id = "testApp".toPath, args = Seq("arg"),
      versionInfo = VersionInfo.forNewConfig(now))

    val newAppDef = appDef.copy(id = "newTestApp".toPath)
    val newVariable = mock[PersistentEntity]

    when(newVariable.bytes).thenReturn(newAppDef.toProtoByteArray.toIndexedSeq)
    when(variable.bytes).thenReturn(appDef.toProtoByteArray.toIndexedSeq)
    when(variable.withNewContent(any())).thenReturn(newVariable)
    when(state.load("app:testApp")).thenReturn(Future.successful(Some(variable)))
    when(state.update(newVariable)).thenReturn(Future.successful(newVariable))

    val store = new MarathonStore[AppDefinition](state, metrics, () => AppDefinition(id = runSpecId), "app:")
    val res = store.modify("testApp") { _ =>
      newAppDef
    }

    assert(newAppDef == Await.result(res, 5.seconds), "Should return the new AppDef")
    verify(state).load("app:testApp")
    verify(state).update(newVariable)
  }

  test("ModifyFail") {
    val state = mock[PersistentStore]
    val variable = mock[PersistentEntity]
    val appDef = AppDefinition(id = "testApp".toPath, args = Seq("arg"))

    val newAppDef = appDef.copy(id = "newTestApp".toPath)
    val newVariable = mock[PersistentEntity]

    when(newVariable.bytes).thenReturn(newAppDef.toProtoByteArray.toIndexedSeq)
    when(variable.bytes).thenReturn(appDef.toProtoByteArray.toIndexedSeq)
    when(variable.withNewContent(any())).thenReturn(newVariable)
    when(state.load("app:testApp")).thenReturn(Future.successful(Some(variable)))
    when(state.update(newVariable)).thenReturn(Future.failed(new StoreCommandFailedException("failed")))

    val store = new MarathonStore[AppDefinition](state, metrics, () => AppDefinition(id = runSpecId), "app:")
    val res = store.modify("testApp") { _ =>
      newAppDef
    }

    intercept[StoreCommandFailedException] {
      Await.result(res, 5.seconds)
    }
  }

  test("Expunge") {
    val state = mock[PersistentStore]

    when(state.delete("app:testApp")).thenReturn(Future.successful(true))
    val store = new MarathonStore[AppDefinition](state, metrics, () => AppDefinition(id = runSpecId), "app:")
    val res = store.expunge("testApp")

    Await.ready(res, 5.seconds)
    verify(state).delete("app:testApp")
  }

  test("ExpungeFail") {
    val state = mock[PersistentStore]

    when(state.delete("app:testApp")).thenReturn(Future.failed(new StoreCommandFailedException("failed")))

    val store = new MarathonStore[AppDefinition](state, metrics, () => AppDefinition(id = runSpecId), "app:")

    val res = store.expunge("testApp")

    intercept[StoreCommandFailedException] {
      Await.result(res, 5.seconds)
    }
  }

  test("Names") {
    val state = new InMemoryStore

    def populate(key: String, value: Array[Byte]) = {
      state.load(key).futureValue match {
        case Some(ent) => state.update(ent.withNewContent(value.toIndexedSeq)).futureValue
        case None => state.create(key, value.toIndexedSeq).futureValue
      }
    }

    populate("app:foo", Array())
    populate("app:bar", Array())
    populate("no_match", Array())

    val store = new MarathonStore[AppDefinition](state, metrics, () => AppDefinition(id = runSpecId), "app:")
    val res = store.names()

    assert(Set("foo", "bar") == Await.result(res, 5.seconds).toSet, "Should return all application keys")
  }

  test("NamesFail") {
    val state = mock[PersistentStore]

    when(state.allIds()).thenReturn(Future.failed(new StoreCommandFailedException("failed")))

    val store = new MarathonStore[AppDefinition](state, metrics, () => AppDefinition(id = runSpecId), "app:")
    val res = store.names()

    whenReady(res.failed) { _ shouldBe a[StoreCommandFailedException] }
  }

  test("ConcurrentModifications") {
    val state = new InMemoryStore

    val store = new MarathonStore[AppDefinition](state, metrics, () => AppDefinition(id = runSpecId), "app:")

    store.store("foo", AppDefinition(id = "foo".toPath, instances = 0)).futureValue

    def plusOne() = {
      store.modify("foo") { f =>
        val appDef = f()
        appDef.copy(instances = appDef.instances + 1)
      }
    }

    val results = for (_ <- 0 until 1000) yield plusOne()

    implicit val ec = scala.concurrent.ExecutionContext.Implicits.global
    val res = Future.sequence(results)

    Await.ready(res, 5.seconds)

    assert(1000 == Await.result(store.fetch("foo"), 5.seconds).map(_.instances)
      .getOrElse(0), "Instances of 'foo' should be set to 1000")
  }

  // regression test for #1507
  test("state.names() throwing exception is treated as empty iterator (ExecutionException without cause)") {
    val state = new InMemoryStore() {
      override def allIds(): Future[Seq[ID]] = super.allIds()
    }

    val store = new MarathonStore[AppDefinition](state, metrics, () => AppDefinition(id = runSpecId), "app:")

    noException should be thrownBy {
      Await.result(store.names(), 1.second)
    }
  }

  class MyWeirdExecutionException extends ExecutionException("weird without cause")

  // regression test for #1507
  test("state.names() throwing exception is treated as empty iterator (ExecutionException with itself as cause)") {
    val state = new InMemoryStore() {
      override def allIds(): Future[Seq[ID]] = super.allIds()
    }

    val store = new MarathonStore[AppDefinition](state, metrics, () => AppDefinition(id = runSpecId), "app:")

    noException should be thrownBy {
      Await.result(store.names(), 1.second)
    }
  }

  test("state.names() throwing exception is treated as empty iterator (direct)") {
    val state = new InMemoryStore() {
      override def allIds(): Future[Seq[ID]] = super.allIds()
    }

    val store = new MarathonStore[AppDefinition](state, metrics, () => AppDefinition(id = runSpecId), "app:")

    noException should be thrownBy {
      Await.result(store.names(), 1.second)
    }
  }

  test("state.names() throwing exception is treated as empty iterator (RuntimeException in ExecutionException)") {
    val state = new InMemoryStore() {
      override def allIds(): Future[Seq[ID]] = super.allIds()
    }

    val store = new MarathonStore[AppDefinition](state, metrics, () => AppDefinition(id = runSpecId), "app:")

    noException should be thrownBy {
      Await.result(store.names(), 1.second)
    }
  }

  def registry: MetricRegistry = new MetricRegistry
}
