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
import org.apache.kafka.streams.kstream.{
  EmitStrategy, Grouped, Materialized, Produced, SessionWindows as JavaSessionWindows, SlidingWindows as JavaSlidingWindows,
  TimeWindows as JavaTimeWindows, WindowedSerdes
}
import org.apache.kafka.streams.state.{SessionStore, Stores, WindowStore}
import org.apache.kafka.streams.test.TestRecord
import xkafka.streams.{KStream, Pipeline, Repartition, Serde, SessionWindows, SlidingWindows, Store, TimeWindows, Topology}

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
  private val updates =
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

  // For windows that emit once they close: record time moves on a few seconds at a time, so each window closes and is emitted before it expires,
  // with one record past the grace.
  private val closing =
    List(
      ("k1", "a", 1000L),
      ("k1", "b", 3000L),
      ("k2", "c", 4000L),
      ("k1", "d", 9000L),
      ("k2", "e", 11000L),
      ("k1", "f", 12000L),
      ("k2", "g", 16000L), // closes [0, 10000)
      ("k1", "h", 17000L),
      ("k2", "i", 26000L), // closes [10000, 20000)
      ("k1", "j", 14000L), // past the grace of [10000, 20000)
      ("k1", "k", 31000L),
      ("k0", "n", 31000L), // at the time of another key's record, so closes together with it
      ("k2", "l", 38000L),
      ("k1", "m", 47000L),
      ("k2", "o", 60000L) // a second after the last look, so the sessions ending at 31000 are emitted, by key
    )

  /** The header each input carries, naming its place among the inputs. */
  private def traced(index: Int): Headers = Headers(Header("trace", Some(Chunk.array(index.toString.getBytes))))

  /** The wall-clock time each input arrives at, 600 ms apart, so that some arrive within a second of the last look for closed windows. */
  private def wall(index: Int): Long = index * 600L

  private val tumbling = TimeWindows.tumbling(10.seconds, grace = 5.seconds)
  private val hopping  = TimeWindows.hopping(10.seconds, advance = 5.seconds, grace = 5.seconds)

  private def javaWindows(advance: Long) =
    JavaTimeWindows.ofSizeAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(5)).advanceBy(Duration.ofMillis(advance))

  private def inMemory(name: String, retention: Duration, size: Duration) = Stores.inMemoryWindowStore(name, retention, size, false)

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

  test("a session key is written and read as Java's session serde writes and reads it, and an absent value as Java's null"):
    val java = WindowedSerdes.sessionWindowedSerdeFrom(classOf[String])
    val key  = org.apache.kafka.streams.kstream.Windowed[String]("key", org.apache.kafka.streams.kstream.internals.SessionWindow(30000L, 45000L))
    val ours = Serde.sessionWindowed(text)
    for
      written <- ours.serializer.serialize(in, Headers.empty, xkafka.streams.Windowed("key", xkafka.streams.Window(30000L, 45000L)))
      read    <- ours.deserializer.deserialize(in, Headers.empty, Some(Chunk.array(java.serializer.serialize("in", key))))
      absent  <- count.option.serializer.serialize(in, Headers.empty, None)
      back    <- count.option.deserializer.deserialize(in, Headers.empty, None)
    yield
      assertEquals(written.map(_.toVector), Some(java.serializer.serialize("in", key).toVector))
      assertEquals(read, xkafka.streams.Windowed("key", xkafka.streams.Window(30000L, 45000L)))
      assertEquals((absent, back), (None, None))

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

  test("counting in tumbling windows emitted once they close writes what Java does, looking for closed windows at most once a second"):
    same[java.lang.Long](
      KStream(in, text, text).groupByKey(text, text).windowedBy(tumbling).emitOnWindowClose.count(Store("counts")).toStream
        .to(out, Serde.timeWindowed(text, 10.seconds), count),
      materialized =>
        _.stream[String, String]("in").groupByKey.windowedBy(javaWindows(10000L)).emitStrategy(EmitStrategy.onWindowClose)
          .count(materialized("counts").withValueSerde(Serdes.Long)).toStream
          .to("out", Produced.`with`(WindowedSerdes.timeWindowedSerdeFrom(classOf[String], 10000L), Serdes.Long)),
      "counts",
      count.deserializer,
      closing
    )

  // Kept only for their size and grace, hopping windows expire before they are emitted, in Java's in-memory store and ours alike; kept a minute,
  // every window is emitted, and Java's stores agree.
  test(
    "reducing in hopping windows kept a minute, emitted once they close, writes what Java does, every window closed since the last, by start and key"
  ):
    same[String](
      KStream(in, text, text).groupByKey(text, text).windowedBy(hopping).withRetention(1.minute).emitOnWindowClose.reduce(Store("joined"))(_ + _)
        .toStream.to(out, Serde.timeWindowed(text, 10.seconds), text),
      materialized =>
        _.stream[String, String]("in").groupByKey.windowedBy(javaWindows(5000L)).emitStrategy(EmitStrategy.onWindowClose)
          .reduce((a, b) => a + b, materialized("joined").withValueSerde(Serdes.String)).toStream
          .to("out", Produced.`with`(WindowedSerdes.timeWindowedSerdeFrom(classOf[String], 10000L), Serdes.String)),
      "joined",
      text.deserializer,
      closing,
      Duration.ofMinutes(1)
    )

  private val sliding = SlidingWindows(5.seconds, grace = 5.seconds)

  private def javaSliding = JavaSlidingWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(5), Duration.ofSeconds(5))

  test("counting in sliding windows writes, logs and keeps what Java does, early records in the window from 0 and late ones dropped"):
    same[java.lang.Long](
      KStream(in, text, text).groupByKey(text, text).windowedBy(sliding).count(Store("counts")).toStream
        .to(out, Serde.timeWindowed(text, 5.seconds), count),
      materialized =>
        _.stream[String, String]("in").groupByKey.windowedBy(javaSliding).count(materialized("counts").withValueSerde(Serdes.Long)).toStream
          .to("out", Produced.`with`(WindowedSerdes.timeWindowedSerdeFrom(classOf[String], 5000L), Serdes.Long)),
      "counts",
      count.deserializer,
      size = Duration.ofSeconds(5)
    )

  // Sliding windows' own cases: early records out of order, records at equal times, and out-of-order records that several windows cover.
  private val tangled =
    List(
      ("k1", "a", 1000L),
      ("k4", "t", 4000L),
      ("k4", "u", 2000L), // early, before the only other record, which the window from 0 holds
      ("k1", "b", 4000L),
      ("k1", "c", 2000L), // early, before a later early record
      ("k1", "d", 3000L),
      ("k1", "e", 4000L), // early, at the time of another
      ("k2", "f", 10000L),
      ("k2", "g", 10000L), // at the time of another
      ("k2", "h", 12000L),
      ("k2", "i", 10000L), // at the time of others, with a window starting just after
      ("k2", "j", 13000L),
      ("k2", "k", 11000L), // out of order, in several windows
      ("k2", "l", 13000L),
      ("k2", "m", 9000L), // out of order, before every other
      ("k1", "n", 20000L),
      ("k1", "o", 16000L),
      ("k3", "p", 16000L),
      ("k1", "s", 24000L),
      ("k3", "q", 17000L), // its window ending at it has closed, so it is not made
      ("k3", "r", 17000L)  // at the time of the last, whose window starting just after the one before holds it
    )

  test("reducing in sliding windows writes, logs and keeps what Java does, for records out of order, at equal times and early"):
    same[String](
      KStream(in, text, text).groupByKey(text, text).windowedBy(sliding).reduce(Store("joined"))(_ + _).toStream
        .to(out, Serde.timeWindowed(text, 5.seconds), text),
      materialized =>
        _.stream[String, String]("in").groupByKey.windowedBy(javaSliding)
          .reduce((a, b) => a + b, materialized("joined").withValueSerde(Serdes.String)).toStream
          .to("out", Produced.`with`(WindowedSerdes.timeWindowedSerdeFrom(classOf[String], 5000L), Serdes.String)),
      "joined",
      text.deserializer,
      tangled,
      size = Duration.ofSeconds(5)
    )

  test("reducing in sliding windows emitted once they close writes what Java does"):
    same[String](
      KStream(in, text, text).groupByKey(text, text).windowedBy(sliding).withRetention(1.minute).emitOnWindowClose.reduce(Store("joined"))(_ + _)
        .toStream.to(out, Serde.timeWindowed(text, 5.seconds), text),
      materialized =>
        _.stream[String, String]("in").groupByKey.windowedBy(javaSliding).emitStrategy(EmitStrategy.onWindowClose)
          .reduce((a, b) => a + b, materialized("joined").withValueSerde(Serdes.String)).toStream
          .to("out", Produced.`with`(WindowedSerdes.timeWindowedSerdeFrom(classOf[String], 5000L), Serdes.String)),
      "joined",
      text.deserializer,
      closing,
      Duration.ofMinutes(1),
      Duration.ofSeconds(5)
    )

  test("aggregating sliding windows of records grouped by a new key goes through the repartition topic as Java's does"):
    same[java.lang.Long](
      KStream(in, text, text).groupBy(Repartition("by-value"), text, text)((k, v) => if v < "f" then k else s"$k!").windowedBy(sliding)
        .aggregate(Store("lengths"), count)(100L)((_, v, n) => n + v.length).toStream.to(out, Serde.timeWindowed(text, 5.seconds), count),
      materialized =>
        _.stream[String, String]("in").groupBy((k, v) => if v < "f" then k else s"$k!", Grouped.as("by-value")).windowedBy(javaSliding).aggregate(
          () => java.lang.Long.valueOf(100L),
          (_, v, n) => java.lang.Long.valueOf(n + v.length),
          materialized("lengths").withValueSerde(Serdes.Long)
        ).toStream.to("out", Produced.`with`(WindowedSerdes.timeWindowedSerdeFrom(classOf[String], 5000L), Serdes.Long)),
      "lengths",
      count.deserializer,
      size = Duration.ofSeconds(5)
    )

  // Sessions with a gap of 5 s and 5 s of grace: a record bridging two sessions, one at the time of a session of one record, and late records.
  private val sessioned =
    List(
      ("k1", "a", 1000L),
      ("k1", "b", 10000L),
      ("k1", "c", 5500L), // bridges [1000] and [10000]
      ("k2", "d", 3000L),
      ("k2", "e", 3000L), // at the time of the session of one record
      ("k2", "f", 20000L),
      ("k1", "g", 13000L), // joins [1000, 10000]
      ("k2", "h", 9000L),  // past the grace of [3000]
      ("k2", "i", 16000L), // late, joining [20000] within its grace
      ("k1", "j", 40000L),
      ("k1", "k", 30000L), // past the grace of [1000, 13000], a session of its own within its own
      ("k2", "l", 52000L),
      ("k3", "p", 42000L), // ends at the close time, the clock a retention past it: logged, not kept
      ("k3", "q", 42000L), // finds no session to join
      ("k4", "r", 55000L),
      ("k4", "s", 50000L) // a gap before [55000], so joins it
    )

  private val sessions = SessionWindows(5.seconds, grace = 5.seconds)

  private def javaSessions = JavaSessionWindows.ofInactivityGapAndGrace(Duration.ofSeconds(5), Duration.ofSeconds(5))

  test("counting in sessions writes and logs what Java does, merged sessions going on as tombstones"):
    sameSessions[java.lang.Long](
      KStream(in, text, text).groupByKey(text, text).windowedBy(sessions).count(Store("counts")).toStream
        .to(out, Serde.sessionWindowed(text), count.option),
      materialized =>
        _.stream[String, String]("in").groupByKey.windowedBy(javaSessions).count(materialized("counts").withValueSerde(Serdes.Long)).toStream
          .to("out", Produced.`with`(WindowedSerdes.sessionWindowedSerdeFrom(classOf[String]), Serdes.Long)),
      "counts"
    )

  test("reducing in sessions merges them in Java's order, and writes and logs what Java does"):
    sameSessions[String](
      KStream(in, text, text).groupByKey(text, text).windowedBy(sessions).reduce(Store("joined"))(_ + _).toStream
        .to(out, Serde.sessionWindowed(text), text.option),
      materialized =>
        _.stream[String, String]("in").groupByKey.windowedBy(javaSessions)
          .reduce((a, b) => a + b, materialized("joined").withValueSerde(Serdes.String)).toStream
          .to("out", Produced.`with`(WindowedSerdes.sessionWindowedSerdeFrom(classOf[String]), Serdes.String)),
      "joined"
    )

  test("aggregating in sessions of records grouped by a new key, with a merge of its own, writes and logs what Java does"):
    sameSessions[String](
      KStream(in, text, text).groupBy(Repartition("by-value"), text, text)((k, v) => if v < "f" then k else s"$k!").windowedBy(sessions)
        .aggregate(Store("trail"), text)("<")((_, v, acc) => acc + v)((_, a, b) => s"($a|$b)").toStream
        .to(out, Serde.sessionWindowed(text), text.option),
      materialized =>
        _.stream[String, String]("in").groupBy((k, v) => if v < "f" then k else s"$k!", Grouped.as("by-value")).windowedBy(javaSessions)
          .aggregate(() => "<", (_, v, acc) => acc + v, (_, a, b) => s"($a|$b)", materialized("trail").withValueSerde(Serdes.String)).toStream
          .to("out", Produced.`with`(WindowedSerdes.sessionWindowedSerdeFrom(classOf[String]), Serdes.String)),
      "trail"
    )

  test("counting in sessions emitted once they close writes what Java does"):
    sameSessions[java.lang.Long](
      KStream(in, text, text).groupByKey(text, text).windowedBy(sessions).withRetention(1.minute).emitOnWindowClose.count(Store("counts")).toStream
        .to(out, Serde.sessionWindowed(text), count.option),
      materialized =>
        _.stream[String, String]("in").groupByKey.windowedBy(javaSessions).emitStrategy(EmitStrategy.onWindowClose)
          .count(materialized("counts").withValueSerde(Serdes.Long)).toStream
          .to("out", Produced.`with`(WindowedSerdes.sessionWindowedSerdeFrom(classOf[String]), Serdes.Long)),
      "counts",
      closing,
      Duration.ofMinutes(1)
    )

  /** Runs `inputs` through a session program ours and Java's ways, over Java's in-memory session store and its RocksDB one, and compares what each
    * writes downstream and to the store's changelog, record for record and byte for byte.
    */
  private def sameSessions[V](
      pipeline: Pipeline[IO],
      java: (String => Materialized[String, V, SessionStore[Bytes, Array[Byte]]]) => StreamsBuilder => Unit,
      store: String,
      inputs: List[(String, String, Long)] = sessioned,
      retention: Duration = Duration.ofSeconds(10)
  ): IO[Unit] =
    val changelog = s"app-$store-changelog"
    ours(pipeline, inputs).map: (written, _) =>
      val memory  = run(java(name => Materialized.as[String, V](Stores.inMemorySessionStore(name, retention))), None, inputs)
      val rocksdb = run(java(name => Materialized.as[String, V, SessionStore[Bytes, Array[Byte]]](name).withRetention(retention)), None, inputs)
      assert(memory.out.nonEmpty, "Java wrote nothing, so there is nothing to compare")
      assertEquals(written.filter(_.topic == out).map(record), memory.out)
      assertEquals(written.filter(_.topic.value == changelog).map(record), memory.topic(changelog))
      assertEquals(written.filter(_.topic == out).map(record), rocksdb.out, "Java over RocksDB")
      assertEquals(written.filter(_.topic.value == changelog).map(record), rocksdb.topic(changelog), "Java over RocksDB")

  /** Runs `inputs` through the program ours and Java's ways and compares what each writes downstream and to the store's changelog, record for record
    * and byte for byte, and what Java's in-memory store and ours keep.
    */
  private def same[V](
      pipeline: Pipeline[IO],
      java: (String => Materialized[String, V, WindowStore[Bytes, Array[Byte]]]) => StreamsBuilder => Unit,
      store: String,
      values: Deserializer[IO, ?],
      inputs: List[(String, String, Long)] = updates,
      retention: Duration = Duration.ofSeconds(15),
      size: Duration = Duration.ofSeconds(10)
  ): IO[Unit] =
    val changelog = s"app-$store-changelog"
    ours(pipeline, inputs).flatMap: (written, state) =>
      state.store(store).toList.traverse: (key, stored) =>
        values.deserialize(in, Headers.empty, Some(stored.value)).map: value =>
          (String(key.take(key.size - 12).toArray), internal.streams.WindowStore.startOf(key)) -> (value.toString, stored.timestamp)
      .map: kept =>
        val memory  = run(java(name => Materialized.as[String, V](inMemory(name, retention, size))), Some(store), inputs)
        val rocksdb = run(java(name => Materialized.as[String, V, WindowStore[Bytes, Array[Byte]]](name).withRetention(retention)), None, inputs)
        assert(memory.out.nonEmpty, "Java wrote nothing, so there is nothing to compare")
        assertEquals(written.filter(_.topic == out).map(record), memory.out)
        assertEquals(written.filter(_.topic.value == changelog).map(record), memory.topic(changelog))
        assertEquals(kept.toMap[(String, Long), (String, Long)], memory.kept)
        assertEquals(written.filter(_.topic == out).map(record), rocksdb.out, "Java over RocksDB")
        assertEquals(written.filter(_.topic.value == changelog).map(record), rocksdb.topic(changelog), "Java over RocksDB")

  /** Each segment's task on partition 0, a repartitioned record going on to the next segment at once, as Java's test driver runs it; with the last
    * segment's state, which holds the window store.
    */
  private def ours(pipeline: Pipeline[IO], inputs: List[(String, String, Long)]): IO[(Vector[TaskOutput], TaskState)] =
    val tasks = Topology.of(pipeline).orFail.pipelines.head.segments.toList.toVector.map(Task[IO]("app", _, 0))
    def go(index: Int, input: TaskInput, states: Vector[TaskState], at: Long): IO[(Vector[TaskOutput], Vector[TaskState])] =
      tasks(index).process(states(index), input, at).flatMap: (state, outputs) =>
        outputs.foldLeftM((Vector.empty[TaskOutput], states.updated(index, state))):
          case ((done, now), written) if written.topic.value.endsWith("-repartition") =>
            go(index + 1, TaskInput(written.key, written.value, written.timestamp, written.headers), now, at)
              .map((more, after) => (done ++ more, after))
          case ((done, now), written) => IO.pure((done :+ written, now))
    inputs.zipWithIndex.foldLeftM((Vector.empty[TaskOutput], Vector.fill(tasks.size)(TaskState.empty))):
      case ((done, states), ((key, value, timestamp), index)) =>
        go(0, TaskInput(Some(Chunk.array(key.getBytes)), Some(Chunk.array(value.getBytes)), timestamp, traced(index)), states, wall(index))
          .map((more, after) => (done ++ more, after))
    .map((written, states) => (written, states.last))

  /** What Java's test driver writes to each topic for `inputs`, and what the window store `kept` names holds after them. */
  private def run(build: StreamsBuilder => Unit, kept: Option[String], inputs: List[(String, String, Long)]): Ran =
    val properties = Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "app")
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1")
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0")
    // The test driver looks for closed windows on every record unless told otherwise; an application looks at most once a second.
    properties.put(StreamsConfig.InternalConfig.EMIT_INTERVAL_MS_KSTREAMS_WINDOWED_AGGREGATION, 1000L)
    properties.put(StreamsConfig.STATE_DIR_CONFIG, java.nio.file.Files.createTempDirectory("xkafka-window-golden").toString)
    val builder = StreamsBuilder()
    build(builder)
    val driver = TopologyTestDriver(builder.build(properties), properties, java.time.Instant.EPOCH)
    try
      val input = driver.createInputTopic("in", StringSerializer(), StringSerializer())
      inputs.zipWithIndex.foreach:
        case ((key, value, timestamp), index) =>
          driver.advanceWallClockTime(Duration.ofMillis(wall(index) - (if index == 0 then 0L else wall(index - 1))))
          val headers = org.apache.kafka.common.header.internals.RecordHeaders()
          headers.add("trace", index.toString.getBytes)
          input.pipeInput(TestRecord(key, value, headers, java.time.Instant.ofEpochMilli(timestamp)))
      val topics =
        driver.producedTopicNames.asScala.toList.map: name =>
          name -> driver.createOutputTopic(name, ByteArrayDeserializer(), ByteArrayDeserializer()).readRecordsToList.asScala.toVector.map: r =>
            Output(
              Option(r.key).map(_.toVector),
              Option(r.value).map(_.toVector),
              r.timestamp,
              r.headers.toArray.toVector.map(h => h.key -> Option(h.value).map(String(_)))
            )
      val held =
        kept.fold(Map.empty): name =>
          driver.getTimestampedWindowStore[String, Any](name).all.asScala.toList.map: kv =>
            (kv.key.key, kv.key.window.start) -> (kv.value.value.toString, kv.value.timestamp)
          .toMap
      Ran(topics.toMap, held)
    finally driver.close()

  private def record(written: TaskOutput): Output =
    Output(
      written.key.map(_.toVector),
      written.value.map(_.toVector),
      written.timestamp,
      written.headers.values.map(h => h.key -> h.value.map(v => String(v.toArray)))
    )

object WindowGoldenSuite:
  /** A record as bytes, with its headers. */
  final case class Output(key: Option[Vector[Byte]], value: Option[Vector[Byte]], timestamp: Long, headers: Vector[(String, Option[String])])

  /** What Java wrote to each topic, and what its window store kept by key and window start. */
  final case class Ran(topics: Map[String, Vector[Output]], kept: Map[(String, Long), (String, Long)]):
    def out: Vector[Output]                 = topic("out")
    def topic(name: String): Vector[Output] = topics.getOrElse(name, Vector.empty)
