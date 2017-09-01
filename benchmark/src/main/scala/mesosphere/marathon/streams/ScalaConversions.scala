package mesosphere.marathon
package stream

import java.util
import java.util.concurrent.TimeUnit

import mesosphere.marathon.stream._
import org.openjdk.jmh.annotations.{ Benchmark, BenchmarkMode, Fork, Mode, OutputTimeUnit, Scope, State }
import org.openjdk.jmh.infra.Blackhole

import scala.collection.JavaConverters._

@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Array(Mode.Throughput, Mode.AverageTime))
@Fork(1)
class ScalaConversions {
  import ScalaConversionsState._

  @Benchmark
  def smallAsJavaStream(hole: Blackhole): Unit = {
    val asJava: util.List[Int] = small
    hole.consume(asJava)
  }

  @Benchmark
  def smallConverted(hole: Blackhole): Unit = {
    val asJava = small.asJava
    hole.consume(asJava)
  }

  @Benchmark
  def mediumAsJavaStream(hole: Blackhole): Unit = {
    val asJava: util.List[Int] = medium
    hole.consume(asJava)
  }

  @Benchmark
  def mediumConverted(hole: Blackhole): Unit = {
    val asJava = medium.asJava
    hole.consume(asJava)
  }

  @Benchmark
  def largeAsJavaStream(hole: Blackhole): Unit = {
    val asJava: util.List[Int] = large
    hole.consume(asJava)
  }

  @Benchmark
  def largeConverted(hole: Blackhole): Unit = {
    val asJava = large.asJava
    hole.consume(asJava)
  }

  @Benchmark
  def veryLargeAsJavaStream(hole: Blackhole): Unit = {
    val asJava: util.List[Int] = veryLarge
    hole.consume(asJava)
  }

  @Benchmark
  def veryLargeConverted(hole: Blackhole): Unit = {
    val asJava = veryLarge.asJava
    hole.consume(asJava)
  }
}
