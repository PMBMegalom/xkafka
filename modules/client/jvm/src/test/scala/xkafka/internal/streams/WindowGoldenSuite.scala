/*
 * Copyright (c) 2026 xkafka contributors
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package xkafka
package internal
package streams

import java.time.Duration
import java.util.Properties

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

import cats.effect.IO
import cats.syntax.all.*
import fs2.Chunk
import munit.CatsEffectSuite
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Serdes, StringSerializer}
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.{StreamsBuilder, StreamsConfig, TopologyTestDriver}
import org.apache.kafka.streams.kstream.{Grouped, Materialized, Produced, TimeWindows as JavaTimeWindows, WindowedSerdes}
import org.apache.kafka.streams.state.{Stores, WindowStore}
import org.apache.kafka.streams.test.TestRecord
import xkafka.streams.{KStream, Pipeline, Repartition, Serde, Store, TimeWindows, Topology}

/** What a windowed aggregation writes downstream and to its changelog, and keeps in its store, against what Java Streams' `TopologyTestDriver` does
  * for the same program and records, with Java's record cache off. Java runs each program twice: over an in-memory window store, which is what xkafka
  * keeps, and over its default, RocksDB, which a Java member most likely keeps; both must write what xkafka writes.
  */
final class WindowGoldenSuite extends CatsEffectSuite:
  import WindowGoldenSuite.*

  private val text  = Serde.utf8[IO]
  private val count = Serde.long[IO]
  private val in    = Topic.from("in").orFail
  private val out   = Topic.from("out").orFail

  // Windows of 10 s with 5 s of grace. Records come out of order: some within the grace, some past it, one on a window's first instant.
  private val inputs =
    List(
      ("k1", "a", 1000L),
      ("k1", "b", 3000L),
      ("k2", "c", 4000L),
      ("k1", "d", 12000L),
      ("k1", "e", 9000L), // late, but within the grace of [0, 10000)
      ("k2", "f", 30000L),
      ("k1", "g", 8000L), // past the grace of [0, 10000)
      ("k1", "h", 24000L),
      ("k1", "i", 10000L), // past the grace of [10000, 20000)
      ("k2", "j", 50000L),
      ("k2", "k", 34999L), // past the grace of [30000, 40000)
      ("k1", "l", 50000L)
    )

  private val tumbling = TimeWindows.tumbling(10.seconds, grace = 5.seconds)
  private val hopping  = TimeWindows.hopping(10.seconds, advance = 5.seconds, grace = 5.seconds)

  private def javaWindows(advance: Long) =
    JavaTimeWindows.ofSizeAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(5)).advanceBy(Duration.ofMillis(advance))

  private def inMemory(name: String) = Stores.inMemoryWindowStore(name, Duration.ofSeconds(15), Duration.ofSeconds(10), false)

  test("counting in tumbling windows writes, logs and keeps what Java does, dropping records past the grace"):
    same[java.lang.Long](
      KStream(in, text, text).groupByKey(text, text).windowedBy(tumbling).count(Store("counts")).toStream
        .to(out, Serde.timeWindowed(text, 10.seconds), count),
      materialized =>
        _.stream[String, String]("in").groupByKey.windowedBy(javaWindows(10000L)).count(materialized("counts").withValueSerde(Serdes.Long)).toStream
          .to("out", Produced.`with`(WindowedSerdes.timeWindowedSerdeFrom(classOf[String], 10000L), Serdes.Long)),
      "counts",
      count.deserializer
    )

  test("reducing in hopping windows writes, logs and keeps what Java does, each record in every window it falls in"):
    same[String](
      KStream(in, text, text).groupByKey(text, text).windowedBy(hopping).reduce(Store("joined"))(_ + _).toStream
        .to(out, Serde.timeWindowed(text, 10.seconds), text),
      materialized =>
        _.stream[String, String]("in").groupByKey.windowedBy(javaWindows(5000L))
          .reduce((a, b) => a + b, materialized("joined").withValueSerde(Serdes.String)).toStream
          .to("out", Produced.`with`(WindowedSerdes.timeWindowedSerdeFrom(classOf[String], 10000L), Serdes.String)),
      "joined",
      text.deserializer
    )

  test("aggregating windows of records grouped by a new key goes through the repartition topic as Java's does"):
    same[java.lang.Long](
      KStream(in, text, text).groupBy(Repartition("by-value"), text, text)((k, v) => if v < "f" then k else s"$k!").windowedBy(tumbling)
        .aggregate(Store("lengths"), count)(100L)((_, v, n) => n + v.length).toStream.to(out, Serde.timeWindowed(text, 10.seconds), count),
      materialized =>
        _.stream[String, String]("in").groupBy((k, v) => if v < "f" then k else s"$k!", Grouped.as("by-value")).windowedBy(javaWindows(10000L))
          .aggregate(
            () => java.lang.Long.valueOf(100L),
            (_, v, n) => java.lang.Long.valueOf(n + v.length),
            materialized("lengths").withValueSerde(Serdes.Long)
          ).toStream.to("out", Produced.`with`(WindowedSerdes.timeWindowedSerdeFrom(classOf[String], 10000L), Serdes.Long)),
      "lengths",
      count.deserializer
    )

  test("windows are found for a record as Java finds them, tumbling and hopping, around time 0 and window edges"):
    val shapes = List((10000L, 10000L), (10000L, 5000L), (10000L, 3000L), (7L, 1L))
    for (size, advance) <- shapes; timestamp <- (-2L to 25L) ++ List(9999L, 10000L, 10001L, 29999L, 30000L) do
      val ours  = TimeWindows.hoppingWithNoGrace(size.millis, advance.millis).windowsFor(timestamp).map(w => w.start -> w.end)
      val javas =
        JavaTimeWindows.ofSizeWithNoGrace(Duration.ofMillis(size)).advanceBy(Duration.ofMillis(advance)).windowsFor(timestamp).asScala.toList
          .map((start, window) => start.toLong -> window.end)
      assertEquals(ours, javas, s"size $size, advance $advance, at $timestamp")

  test("a windowed key is written and read as Java's windowed serde writes and reads it"):
    val java = WindowedSerdes.timeWindowedSerdeFrom(classOf[String], 10000L)
    val key  = org.apache.kafka.streams.kstream.Windowed[String]("key", org.apache.kafka.streams.kstream.internals.TimeWindow(30000L, 40000L))
    val ours = Serde.timeWindowed(text, 10.seconds)
    for
      written <- ours.serializer.serialize(in, Headers.empty, xkafka.streams.Windowed("key", xkafka.streams.Window(30000L, 40000L)))
      read    <- ours.deserializer.deserialize(in, Headers.empty, Some(Chunk.array(java.serializer.serialize("in", key))))
    yield
      assertEquals(written.map(_.toVector), Some(java.serializer.serialize("in", key).toVector))
      assertEquals(read, xkafka.streams.Windowed("key", xkafka.streams.Window(30000L, 40000L)))

  /** Runs `inputs` through the program ours and Java's ways and compares what each writes downstream and to the store's changelog, record for record
    * and byte for byte, and what Java's in-memory store and ours keep.
    */
  private def same[V](
      pipeline: Pipeline[IO],
      java: (String => Materialized[String, V, WindowStore[Bytes, Array[Byte]]]) => StreamsBuilder => Unit,
      store: String,
      values: Deserializer[IO, ?]
  ): IO[Unit] =
    val changelog = s"app-$store-changelog"
    ours(pipeline).flatMap: (written, state) =>
      state.store(store).toList.traverse: (key, stored) =>
        values.deserialize(in, Headers.empty, Some(stored.value)).map: value =>
          (String(key.take(key.size - 12).toArray), internal.streams.WindowStore.startOf(key)) -> (value.toString, stored.timestamp)
      .map: kept =>
        val memory  = run(java(name => Materialized.as[String, V](inMemory(name))), Some(store))
        val rocksdb = run(java(name => Materialized.as[String, V, WindowStore[Bytes, Array[Byte]]](name)), None)
        assert(memory.out.nonEmpty, "Java wrote nothing, so there is nothing to compare")
        assertEquals(written.filter(_.topic == out).map(record), memory.out)
        assertEquals(written.filter(_.topic.value == changelog).map(record), memory.topic(changelog))
        assertEquals(kept.toMap[(String, Long), (String, Long)], memory.kept)
        assertEquals(written.filter(_.topic == out).map(record), rocksdb.out, "Java over RocksDB")
        assertEquals(written.filter(_.topic.value == changelog).map(record), rocksdb.topic(changelog), "Java over RocksDB")

  /** Each segment's task on partition 0, a repartitioned record going on to the next segment at once, as Java's test driver runs it; with the last
    * segment's state, which holds the window store.
    */
  private def ours(pipeline: Pipeline[IO]): IO[(Vector[TaskOutput], TaskState)] =
    val tasks = Topology.of(pipeline).orFail.pipelines.head.segments.toList.toVector.map(Task[IO]("app", _, 0))
    def go(index: Int, input: TaskInput, states: Vector[TaskState]): IO[(Vector[TaskOutput], Vector[TaskState])] =
      tasks(index).process(states(index), input).flatMap: (state, outputs) =>
        outputs.foldLeftM((Vector.empty[TaskOutput], states.updated(index, state))):
          case ((done, now), written) if written.topic.value.endsWith("-repartition") =>
            go(index + 1, TaskInput(written.key, written.value, written.timestamp, written.headers), now).map((more, after) => (done ++ more, after))
          case ((done, now), written) => IO.pure((done :+ written, now))
    inputs.foldLeftM((Vector.empty[TaskOutput], Vector.fill(tasks.size)(TaskState.empty))):
      case ((done, states), (key, value, timestamp)) =>
        go(0, TaskInput(Some(Chunk.array(key.getBytes)), Some(Chunk.array(value.getBytes)), timestamp, Headers.empty), states)
          .map((more, after) => (done ++ more, after))
    .map((written, states) => (written, states.last))

  /** What Java's test driver writes to each topic for `inputs`, and what the window store `kept` names holds after them. */
  private def run(build: StreamsBuilder => Unit, kept: Option[String]): Ran =
    val properties = Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "app")
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1")
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0")
    properties.put(StreamsConfig.STATE_DIR_CONFIG, java.nio.file.Files.createTempDirectory("xkafka-window-golden").toString)
    val builder = StreamsBuilder()
    build(builder)
    val driver = TopologyTestDriver(builder.build(properties), properties)
    try
      val input = driver.createInputTopic("in", StringSerializer(), StringSerializer())
      inputs.foreach((key, value, timestamp) => input.pipeInput(TestRecord(key, value, java.time.Instant.ofEpochMilli(timestamp))))
      val topics =
        driver.producedTopicNames.asScala.toList.map: name =>
          name -> driver.createOutputTopic(name, ByteArrayDeserializer(), ByteArrayDeserializer()).readRecordsToList.asScala.toVector.map: r =>
            Output(Option(r.key).map(_.toVector), Option(r.value).map(_.toVector), r.timestamp, r.headers.toArray.length)
      val held =
        kept.fold(Map.empty): name =>
          driver.getTimestampedWindowStore[String, Any](name).all.asScala.toList.map: kv =>
            (kv.key.key, kv.key.window.start) -> (kv.value.value.toString, kv.value.timestamp)
          .toMap
      Ran(topics.toMap, held)
    finally driver.close()

  private def record(written: TaskOutput): Output =
    Output(written.key.map(_.toVector), written.value.map(_.toVector), written.timestamp, written.headers.values.size)

object WindowGoldenSuite:
  /** A record as bytes, with how many headers it carries. */
  final case class Output(key: Option[Vector[Byte]], value: Option[Vector[Byte]], timestamp: Long, headers: Int)

  /** What Java wrote to each topic, and what its window store kept by key and window start. */
  final case class Ran(topics: Map[String, Vector[Output]], kept: Map[(String, Long), (String, Long)]):
    def out: Vector[Output]                 = topic("out")
    def topic(name: String): Vector[Output] = topics.getOrElse(name, Vector.empty)
