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

import cats.data.Ior
import cats.effect.IO
import cats.syntax.all.*
import fs2.Chunk
import munit.CatsEffectSuite
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Serdes, StringSerializer}
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.{StreamsBuilder, StreamsConfig, TopologyTestDriver}
import org.apache.kafka.streams.kstream.{
  EmitStrategy, Grouped, JoinWindows as JavaJoinWindows, KTable as JavaTable, Materialized, Produced, SessionWindows as JavaSessionWindows,
  SlidingWindows as JavaSlidingWindows, StreamJoined, Suppressed as JavaSuppressed, TimeWindows as JavaTimeWindows, WindowedSerdes
}
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig
import org.apache.kafka.streams.state.{SessionStore, Stores, WindowStore}
import org.apache.kafka.streams.test.TestRecord
import xkafka.streams.{
  BufferLimit, JoinWindows, KStream, KTable, Named, Pipeline, Repartition, Serde, SessionWindows, SlidingWindows, Store, Suppressed, TimeWindows,
  Topology
}

/** What a windowed aggregation writes downstream and to its changelog, and keeps in its store, against what Java Streams' `TopologyTestDriver` does
  * for the same program and records, with Java's record cache off. Java runs each program twice: over an in-memory window store, which is what xkafka
  * keeps, and over its default, RocksDB, which a Java member most likely keeps; both must write what xkafka writes.
  */
final class WindowGoldenSuite extends CatsEffectSuite:
  import WindowGoldenSuite.*

  /** One of Java's joins of two tables into a store. */
  private type JavaJoin =
    (
        JavaTable[String, String],
        JavaTable[String, String],
        Materialized[String, String, org.apache.kafka.streams.state.KeyValueStore[Bytes, Array[Byte]]]
    ) => JavaTable[String, String]

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
    same[java.lang.Long, Long](
      KStream(Named("read-1"), in, text, text).groupByKey(text, text).windowedBy(tumbling).count(Named("count-1"), Store("counts"))
        .toStream(Named("to-stream-1")).to(Named("write-1"), out, Serde.timeWindowed(text, 10.seconds), count),
      materialized =>
        _.stream[String, String]("in").groupByKey.windowedBy(javaWindows(10000L)).count(materialized("counts").withValueSerde(Serdes.Long)).toStream
          .to("out", Produced.`with`(WindowedSerdes.timeWindowedSerdeFrom(classOf[String], 10000L), Serdes.Long)),
      "counts",
      count.deserializer
    )

  test("reducing in hopping windows writes, logs and keeps what Java does, each record in every window it falls in"):
    same[String, String](
      KStream(Named("read-2"), in, text, text).groupByKey(text, text).windowedBy(hopping).reduce(Named("reduce-1"), Store("joined"))(_ + _)
        .toStream(Named("to-stream-2")).to(Named("write-2"), out, Serde.timeWindowed(text, 10.seconds), text),
      materialized =>
        _.stream[String, String]("in").groupByKey.windowedBy(javaWindows(5000L))
          .reduce((a, b) => a + b, materialized("joined").withValueSerde(Serdes.String)).toStream
          .to("out", Produced.`with`(WindowedSerdes.timeWindowedSerdeFrom(classOf[String], 10000L), Serdes.String)),
      "joined",
      text.deserializer
    )

  test("aggregating windows of records grouped by a new key goes through the repartition topic as Java's does"):
    same[java.lang.Long, Long](
      KStream(Named("read-3"), in, text, text).groupBy(Repartition("by-value"), text, text)((k, v) => if v < "f" then k else s"$k!")
        .windowedBy(tumbling).aggregate(Named("aggregate-1"), Store("lengths"), count)(100L)((_, v, n) => n + v.length).toStream(Named("to-stream-3"))
        .to(Named("write-3"), out, Serde.timeWindowed(text, 10.seconds), count),
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

  test("a key goes to the partition the Java producer picks for it"):
    val keys = List("", "a", "ab", "abc", "abcd", "abcde", "k0", "k9", "key-with-more-bytes", "\u00e9\u00e8") ++ (0 until 200).map(n => s"key-$n")
    for key <- keys; partitions <- List(1, 2, 3, 7, 12) do
      val bytes = key.getBytes("UTF-8")
      assertEquals(Murmur2.hash(Chunk.array(bytes)), org.apache.kafka.common.utils.Utils.murmur2(bytes), key)
      assertEquals(
        Murmur2.partition(Chunk.array(bytes), partitions),
        org.apache.kafka.common.utils.Utils.toPositive(org.apache.kafka.common.utils.Utils.murmur2(bytes)) % partitions
      )

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
    same[java.lang.Long, Long](
      KStream(Named("read-4"), in, text, text).groupByKey(text, text).windowedBy(tumbling).emitOnWindowClose.count(Named("count-2"), Store("counts"))
        .toStream(Named("to-stream-4")).to(Named("write-4"), out, Serde.timeWindowed(text, 10.seconds), count),
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
    same[String, String](
      KStream(Named("read-5"), in, text, text).groupByKey(text, text).windowedBy(hopping).withRetention(1.minute).emitOnWindowClose
        .reduce(Named("reduce-2"), Store("joined"))(_ + _).toStream(Named("to-stream-5"))
        .to(Named("write-5"), out, Serde.timeWindowed(text, 10.seconds), text),
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
    same[java.lang.Long, Long](
      KStream(Named("read-6"), in, text, text).groupByKey(text, text).windowedBy(sliding).count(Named("count-3"), Store("counts"))
        .toStream(Named("to-stream-6")).to(Named("write-6"), out, Serde.timeWindowed(text, 5.seconds), count),
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
    same[String, String](
      KStream(Named("read-7"), in, text, text).groupByKey(text, text).windowedBy(sliding).reduce(Named("reduce-3"), Store("joined"))(_ + _)
        .toStream(Named("to-stream-7")).to(Named("write-7"), out, Serde.timeWindowed(text, 5.seconds), text),
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
    same[String, String](
      KStream(Named("read-8"), in, text, text).groupByKey(text, text).windowedBy(sliding).withRetention(1.minute).emitOnWindowClose
        .reduce(Named("reduce-4"), Store("joined"))(_ + _).toStream(Named("to-stream-8"))
        .to(Named("write-8"), out, Serde.timeWindowed(text, 5.seconds), text),
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
    same[java.lang.Long, Long](
      KStream(Named("read-9"), in, text, text).groupBy(Repartition("by-value"), text, text)((k, v) => if v < "f" then k else s"$k!")
        .windowedBy(sliding).aggregate(Named("aggregate-2"), Store("lengths"), count)(100L)((_, v, n) => n + v.length).toStream(Named("to-stream-9"))
        .to(Named("write-9"), out, Serde.timeWindowed(text, 5.seconds), count),
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
      KStream(Named("read-10"), in, text, text).groupByKey(text, text).windowedBy(sessions).count(Named("count-4"), Store("counts"))
        .toStream(Named("to-stream-10")).to(Named("write-10"), out, Serde.sessionWindowed(text), count.option),
      materialized =>
        _.stream[String, String]("in").groupByKey.windowedBy(javaSessions).count(materialized("counts").withValueSerde(Serdes.Long)).toStream
          .to("out", Produced.`with`(WindowedSerdes.sessionWindowedSerdeFrom(classOf[String]), Serdes.Long)),
      "counts"
    )

  test("reducing in sessions merges them in Java's order, and writes and logs what Java does"):
    sameSessions[String](
      KStream(Named("read-11"), in, text, text).groupByKey(text, text).windowedBy(sessions).reduce(Named("reduce-5"), Store("joined"))(_ + _)
        .toStream(Named("to-stream-11")).to(Named("write-11"), out, Serde.sessionWindowed(text), text.option),
      materialized =>
        _.stream[String, String]("in").groupByKey.windowedBy(javaSessions)
          .reduce((a, b) => a + b, materialized("joined").withValueSerde(Serdes.String)).toStream
          .to("out", Produced.`with`(WindowedSerdes.sessionWindowedSerdeFrom(classOf[String]), Serdes.String)),
      "joined"
    )

  test("aggregating in sessions of records grouped by a new key, with a merge of its own, writes and logs what Java does"):
    sameSessions[String](
      KStream(Named("read-12"), in, text, text).groupBy(Repartition("by-value"), text, text)((k, v) => if v < "f" then k else s"$k!")
        .windowedBy(sessions).aggregate(Named("aggregate-3"), Store("trail"), text)("<")((_, v, acc) => acc + v)((_, a, b) => s"($a|$b)")
        .toStream(Named("to-stream-12")).to(Named("write-12"), out, Serde.sessionWindowed(text), text.option),
      materialized =>
        _.stream[String, String]("in").groupBy((k, v) => if v < "f" then k else s"$k!", Grouped.as("by-value")).windowedBy(javaSessions)
          .aggregate(() => "<", (_, v, acc) => acc + v, (_, a, b) => s"($a|$b)", materialized("trail").withValueSerde(Serdes.String)).toStream
          .to("out", Produced.`with`(WindowedSerdes.sessionWindowedSerdeFrom(classOf[String]), Serdes.String)),
      "trail"
    )

  test("counting in sessions emitted once they close writes what Java does"):
    sameSessions[java.lang.Long](
      KStream(Named("read-13"), in, text, text).groupByKey(text, text).windowedBy(sessions).withRetention(1.minute).emitOnWindowClose
        .count(Named("count-5"), Store("counts")).toStream(Named("to-stream-13"))
        .to(Named("write-13"), out, Serde.sessionWindowed(text), count.option),
      materialized =>
        _.stream[String, String]("in").groupByKey.windowedBy(javaSessions).emitStrategy(EmitStrategy.onWindowClose)
          .count(materialized("counts").withValueSerde(Serdes.Long)).toStream
          .to("out", Produced.`with`(WindowedSerdes.sessionWindowedSerdeFrom(classOf[String]), Serdes.Long)),
      "counts",
      closing,
      Duration.ofMinutes(1)
    )

  private def windowedSerde(size: Long) = WindowedSerdes.timeWindowedSerdeFrom(classOf[String], size)

  private def counted(name: String) =
    Materialized.as[String, java.lang.Long, org.apache.kafka.streams.state.KeyValueStore[Bytes, Array[Byte]]](name).withValueSerde(Serdes.Long)

  test("a buffer logs its changed keys in the order Java's set of them iterates, as its table grows, is crowded and is cleared"):
    val random = scala.util.Random(7)
    def key()  = Chunk.array(Array.fill(1 + random.nextInt(6))(random.nextInt(256).toByte))
    // Keys of one bucket of 16, nine of which crowd it while the table is small.
    val crowd  = Iterator.continually(key()).filter(k => Dirty.hash(k) % 16 == 3).distinct.take(12).toList
    val rounds = List(crowd, List.fill(40)(key()), List.fill(5)(key()), List.fill(300)(key()) ++ crowd)
    val _      =
      rounds.foldLeft((Dirty.empty, new java.util.HashSet[Bytes]())):
        case ((ours, java), keys) =>
          val added =
            keys.foldLeft(ours): (dirty, key) =>
              java.add(Bytes.wrap(key.toArray))
              val next = dirty + key
              assertEquals(next.ordered.map(_.toVector), java.asScala.toVector.map(_.get.toVector))
              next
          java.clear()
          (added.cleared, java)

  test("final results of tumbling windows go on as Java's do, once each window closes, and the buffer logs what Java's logs"):
    sameEverywhere(
      KStream(Named("read-14"), in, text, text).groupByKey(text, text).windowedBy(tumbling).count(Named("count-6"), Store("counts"))
        .suppress(Suppressed.untilWindowCloses("final")).toStream(Named("to-stream-14"))
        .to(Named("write-14"), out, Serde.timeWindowed(text, 10.seconds), count),
      _.stream[String, String]("in").groupByKey.windowedBy(javaWindows(10000L))
        .count(Materialized.as("counts").withKeySerde(Serdes.String).withValueSerde(Serdes.Long))
        .suppress(JavaSuppressed.untilWindowCloses(BufferConfig.unbounded()).withName("final")).toStream
        .to("out", Produced.`with`(windowedSerde(10000L), Serdes.Long)),
      updates
    )

  test("final results of hopping windows go on as Java's do, earliest window first and then by key"):
    sameEverywhere(
      KStream(Named("read-15"), in, text, text).groupByKey(text, text).windowedBy(hopping).reduce(Named("reduce-6"), Store("joined"))(_ + _)
        .suppress(Suppressed.untilWindowCloses("final")).toStream(Named("to-stream-15"))
        .to(Named("write-15"), out, Serde.timeWindowed(text, 10.seconds), text),
      _.stream[String, String]("in").groupByKey.windowedBy(javaWindows(5000L))
        .reduce((a, b) => a + b, Materialized.as("joined").withKeySerde(Serdes.String).withValueSerde(Serdes.String))
        .suppress(JavaSuppressed.untilWindowCloses(BufferConfig.unbounded()).withName("final")).toStream
        .to("out", Produced.`with`(windowedSerde(10000L), Serdes.String)),
      closing
    )

  test("final results of sliding windows go on as Java's do"):
    sameEverywhere(
      KStream(Named("read-16"), in, text, text).groupByKey(text, text).windowedBy(sliding).count(Named("count-7"), Store("counts"))
        .suppress(Suppressed.untilWindowCloses("final")).toStream(Named("to-stream-16"))
        .to(Named("write-16"), out, Serde.timeWindowed(text, 5.seconds), count),
      _.stream[String, String]("in").groupByKey.windowedBy(javaSliding)
        .count(Materialized.as("counts").withKeySerde(Serdes.String).withValueSerde(Serdes.Long))
        .suppress(JavaSuppressed.untilWindowCloses(BufferConfig.unbounded()).withName("final")).toStream
        .to("out", Produced.`with`(windowedSerde(5000L), Serdes.Long)),
      closing
    )

  test("final results of sessions go on as Java's do, once the grace and the gap have passed since each ends, sessions merged away dropped"):
    sameEverywhere(
      KStream(Named("read-17"), in, text, text).groupByKey(text, text).windowedBy(sessions).count(Named("count-8"), Store("counts"))
        .suppress(Suppressed.untilWindowCloses("final")).toStream(Named("to-stream-17"))
        .to(Named("write-17"), out, Serde.sessionWindowed(text), count.option),
      _.stream[String, String]("in").groupByKey.windowedBy(javaSessions)
        .count(Materialized.as("counts").withKeySerde(Serdes.String).withValueSerde(Serdes.Long))
        .suppress(JavaSuppressed.untilWindowCloses(BufferConfig.unbounded()).withName("final")).toStream
        .to("out", Produced.`with`(WindowedSerdes.sessionWindowedSerdeFrom(classOf[String]), Serdes.Long)),
      sessioned
    )

  test("a table's changes held for a time limit go on as Java's do, each key's latest once the limit has passed since its first"):
    sameEverywhere(
      KStream(Named("read-18"), in, text, text).groupByKey(text, text).count(Named("count-9"), Store("counts"))
        .suppress(Suppressed.untilTimeLimit("calm", 3.seconds)).toStream(Named("to-stream-18")).to(Named("write-18"), out, text, count),
      _.stream[String, String]("in").groupByKey.count(counted("counts"))
        .suppress(JavaSuppressed.untilTimeLimit(Duration.ofSeconds(3), BufferConfig.unbounded()).withName("calm")).toStream
        .to("out", Produced.`with`(Serdes.String, Serdes.Long)),
      updates
    )

  test("a buffer of at most one change sends the earliest on early, as Java's does"):
    sameEverywhere(
      KStream(Named("read-19"), in, text, text).groupByKey(text, text).reduce(Named("reduce-7"), Store("joined"))(_ + _)
        .suppress(Suppressed.untilTimeLimit("calm", 1.minute, BufferLimit.records(1).emitEarlyWhenFull)).toStream(Named("to-stream-19"))
        .to(Named("write-19"), out, text, text),
      _.stream[String, String]("in").groupByKey.reduce((a, b) => a + b, Materialized.as("joined").withValueSerde(Serdes.String))
        .suppress(JavaSuppressed.untilTimeLimit(Duration.ofMinutes(1), BufferConfig.maxRecords(1).emitEarlyWhenFull()).withName("calm")).toStream
        .to("out", Produced.`with`(Serdes.String, Serdes.String)),
      updates
    )

  test("a buffer limited in bytes counts them as Java's does, sending the earliest on early"):
    // Each change holds 40 bytes or more, growing with its trail, its old value counted again once its key is held twice. Under the largest limits
    // nothing goes on early, and so nothing goes on within the minute.
    (40L to 200L by 8L).toList.traverse: limit =>
      sameEverywhere(
        KStream(Named("read-20"), in, text, text).groupByKey(text, text)
          .aggregate(Named("aggregate-4"), Store("trail"), text)("<")((_, v, acc) => acc + v)
          .suppress(Suppressed.untilTimeLimit("calm", 1.minute, BufferLimit.bytes(limit))).toStream(Named("to-stream-20"))
          .to(Named("write-20"), out, text, text),
        _.stream[String, String]("in").groupByKey.aggregate(() => "<", (_, v, acc) => acc + v, Materialized.as("trail").withValueSerde(Serdes.String))
          .suppress(JavaSuppressed.untilTimeLimit(Duration.ofMinutes(1), BufferConfig.maxBytes(limit).emitEarlyWhenFull()).withName("calm")).toStream
          .to("out", Produced.`with`(Serdes.String, Serdes.String)),
        updates,
        wrote = None
      )
    .map(wrote => assert(wrote.contains(true) && wrote.contains(false), wrote))

  test("a buffer limited in bytes counts an old value equal to the prior one again once its key is held again, as Java counts it"):
    // Each key keeps its first value, so a key held again after it went on holds an old value equal to its prior one.
    (40L to 200L).toList.traverse: limit =>
      sameEverywhere(
        KStream(Named("read-21"), in, text, text).groupByKey(text, text)
          .aggregate(Named("aggregate-5"), Store("first"), text)("<")((_, v, acc) => if acc == "<" then acc + v else acc)
          .suppress(Suppressed.untilTimeLimit("calm", 3.seconds, BufferLimit.bytes(limit))).toStream(Named("to-stream-21"))
          .to(Named("write-21"), out, text, text),
        _.stream[String, String]("in").groupByKey
          .aggregate(() => "<", (_, v, acc) => if acc == "<" then acc + v else acc, Materialized.as("first").withValueSerde(Serdes.String))
          .suppress(JavaSuppressed.untilTimeLimit(Duration.ofSeconds(3), BufferConfig.maxBytes(limit).emitEarlyWhenFull()).withName("calm")).toStream
          .to("out", Produced.`with`(Serdes.String, Serdes.String)),
        updates
      )
    .void

  test("a suppression after another holds what the first sends on, its old value included, as Java's does"):
    sameEverywhere(
      KStream(Named("read-22"), in, text, text).groupByKey(text, text).windowedBy(tumbling).count(Named("count-10"), Store("counts"))
        .suppress(Suppressed.untilTimeLimit("calm", 2.seconds)).suppress(Suppressed.untilWindowCloses("final")).toStream(Named("to-stream-22"))
        .to(Named("write-22"), out, Serde.timeWindowed(text, 10.seconds), count),
      _.stream[String, String]("in").groupByKey.windowedBy(javaWindows(10000L))
        .count(Materialized.as("counts").withKeySerde(Serdes.String).withValueSerde(Serdes.Long))
        .suppress(JavaSuppressed.untilTimeLimit(Duration.ofSeconds(2), BufferConfig.unbounded()).withName("calm"))
        .suppress(JavaSuppressed.untilWindowCloses(BufferConfig.unbounded()).withName("final")).toStream
        .to("out", Produced.`with`(windowedSerde(10000L), Serdes.Long)),
      updates
    )

  // A table's records, null standing for a record without a key or without a value.
  private val tabled =
    List(
      ("k1", "a", 1000L),
      ("k2", "b", 2000L),
      ("k1", "c", 3000L),
      ("k1", null, 4000L), // deletes k1
      ("k3", null, 5000L), // deletes a key the table never held
      (null, "d", 6000L),  // skipped, having no key
      ("k1", "e", 7000L),
      ("k2", "f", 1500L), // older than what the table holds for k2, yet replaces it
      ("k5", "g", 9000L),
      ("k5", null, 9500L),
      ("k5", "h", 10000L) // replaces nothing, its key deleted
    )

  test("a table read from a topic writes and logs what Java's does, a record without a value deleting its key"):
    sameEverywhere(
      KTable(Named("k-table-1"), in, text, text, Store("users")).toStream(Named("to-stream-23")).to(Named("write-23"), out, text, text.option),
      _.table[String, String]("in", Materialized.as("users")).toStream.to("out", Produced.`with`(Serdes.String, Serdes.String)),
      tabled
    )

  test("a table read from a topic, its changes held for a time limit, holds and logs its deletes as Java's buffer does"):
    sameEverywhere(
      KTable(Named("k-table-2"), in, text, text, Store("users")).suppress(Suppressed.untilTimeLimit("calm", 2.seconds))
        .toStream(Named("to-stream-24")).to(Named("write-24"), out, text, text.option),
      _.table[String, String]("in", Materialized.as("users"))
        .suppress(JavaSuppressed.untilTimeLimit(Duration.ofSeconds(2), BufferConfig.unbounded()).withName("calm")).toStream
        .to("out", Produced.`with`(Serdes.String, Serdes.String)),
      tabled :+ ("k9", "z", 60000L)
    )

  // Users and orders by user: orders for users the table holds, has deleted, and never held, and updates between orders.
  private val ordering =
    List(
      ("users", "u1", "ann", 1000L),
      ("orders", "u1", "o1", 2000L),
      ("orders", "u2", "o2", 2500L), // no such user yet
      ("users", "u2", "bob", 3000L),
      ("orders", "u2", "o3", 3500L),
      ("users", "u1", "amy", 4000L), // a new name for u1
      ("orders", "u1", "o4", 4500L),
      ("users", "u2", null, 5000L), // deletes u2
      ("orders", "u2", "o5", 5500L),
      ("orders", "u3", "o6", 1500L), // late, for a user never held
      ("users", "u3", "cy", 6000L),
      ("orders", "u3", "o7", 6500L),
      ("users", "u9", "zed", 20000L), // well after every other change
      ("orders", "u1", "o8", 20500L)
    )

  private def javaUsers(builder: StreamsBuilder) =
    builder.table[String, String]("users", Materialized.as[String, String, org.apache.kafka.streams.state.KeyValueStore[Bytes, Array[Byte]]]("users"))

  test("a stream joined with a table writes what Java's join does, each record with the value the table holds when it comes"):
    sameFrom(
      KStream(Named("orders"), Topic.from("orders").orFail, text, text)
        .join(Named("join-users"), KTable(Named("users"), Topic.from("users").orFail, text, text, Store("users")))((o, u) => s"$o:$u")
        .to(Named("write-25"), out, text, text),
      builder => builder.stream[String, String]("orders").join(javaUsers(builder), (o: String, u: String) => s"$o:$u").to("out"),
      ordering
    )

  test("a stream left joined with a table writes what Java's left join does, a record without a value in the table joined with none"):
    sameFrom(
      KStream(Named("orders"), Topic.from("orders").orFail, text, text)
        .leftJoin(Named("left-join-users"), KTable(Named("users"), Topic.from("users").orFail, text, text, Store("users")))((o, u) =>
          s"$o:${u.getOrElse("-")}"
        ).to(Named("write-26"), out, text, text),
      builder =>
        builder.stream[String, String]("orders").leftJoin(javaUsers(builder), (o: String, u: String) => s"$o:${Option(u).getOrElse("-")}").to("out"),
      ordering
    )

  test("a stream whose key changed is joined through its repartition topic as Java's is"):
    sameFrom(
      KStream(Named("orders"), Topic.from("orders").orFail, text, text).selectKey(Named("select-key-1"))((_, v) => if v < "o4" then "u1" else "u2")
        .join(Named("by-user"), KTable(Named("users"), Topic.from("users").orFail, text, text, Store("users")), text, text)((o, u) => s"$o:$u")
        .to(Named("write-27"), out, text, text),
      builder =>
        builder.stream[String, String]("orders").selectKey((_, v) => if v < "o4" then "u1" else "u2").join(
          javaUsers(builder),
          (o: String, u: String) => s"$o:$u",
          org.apache.kafka.streams.kstream.Joined.`with`(Serdes.String, Serdes.String, Serdes.String).withName("by-user")
        ).to("out"),
      ordering
    )

  test("a stream joined with a counted table writes what Java's join does, as the count changes"):
    sameFrom(
      KStream(Named("orders"), Topic.from("orders").orFail, text, text).join(
        Named("logins"),
        KStream(Named("users"), Topic.from("users").orFail, text, text).groupByKey(text, text).count(Named("count-11"), Store("logins"))
      )((o, n) => s"$o:$n").to(Named("write-28"), out, text, text),
      builder =>
        val logins = builder.stream[String, String]("users").groupByKey.count(counted("logins"))
        builder.stream[String, String]("orders").join(logins, (o: String, n: java.lang.Long) => s"$o:$n").to("out")
      ,
      ordering.filter(_._3 != null)
    )

  test("a stream joined with a suppressed table sees what Java's join sees, a key the buffer holds showing its value before the held change"):
    sameFrom(
      KStream(Named("orders"), Topic.from("orders").orFail, text, text).leftJoin(
        Named("left-join-users"),
        KTable(Named("users"), Topic.from("users").orFail, text, text, Store("users")).suppress(Suppressed.untilTimeLimit("calm", 2.seconds))
      )((o, u) => s"$o:${u.getOrElse("-")}").to(Named("write-29"), out, text, text),
      builder =>
        builder.stream[String, String]("orders").leftJoin(
          javaUsers(builder).suppress(JavaSuppressed.untilTimeLimit(Duration.ofSeconds(2), BufferConfig.unbounded()).withName("calm")),
          (o: String, u: String) => s"$o:${Option(u).getOrElse("-")}"
        ).to("out"),
      ordering
    )

  // Two tables changing in turn: keys both hold, one holds, neither holds, updates, deletes of held keys and of keys never held, and a late change.
  private val tabling =
    List(
      ("left", "k1", "a1", 1000L),
      ("right", "k1", "b1", 2000L), // k1 in both
      ("left", "k2", "a2", 2500L),  // k2 in the left only
      ("right", "k3", "b3", 3000L), // k3 in the right only
      ("left", "k1", "a1+", 3500L), // an update while both hold k1
      ("right", "k2", "b2", 1500L), // late, earlier than the left's value for k2
      ("right", "k1", null, 4000L), // deletes k1 on the right while the left holds it
      ("left", "k3", null, 4500L),  // deletes k3 on the left, which never held it, while the right does
      ("left", "k4", null, 5000L),  // deletes k4, which neither holds
      ("right", "k4", null, 5500L),
      ("left", "k2", null, 6000L),   // deletes k2 on the left while the right holds it
      ("right", "k2", "b2+", 6500L), // an update on the right after the left deleted k2
      ("left", "k1", "a1++", 7000L),
      ("right", "k1", "b1+", 20000L) // well after every other change
    )

  private def lefts  = KTable(Named("lefts"), Topic.from("left").orFail, text, text, Store("lefts"))
  private def rights = KTable(Named("rights"), Topic.from("right").orFail, text, text, Store("rights"))

  private def javaTable(builder: StreamsBuilder, topic: String, store: String) =
    builder.table[String, String](topic, Materialized.as[String, String, org.apache.kafka.streams.state.KeyValueStore[Bytes, Array[Byte]]](store))

  private def javaStored(store: String) =
    Materialized.as[String, String, org.apache.kafka.streams.state.KeyValueStore[Bytes, Array[Byte]]](store).withValueSerde(Serdes.String)

  private def either(a: String, b: String) = s"${Option(a).getOrElse("-")}+${Option(b).getOrElse("-")}"

  private def ior(both: Ior[String, String]) = either(both.left.orNull, both.right.orNull)

  test("two tables joined write what Java's join of tables does, a delete on either side deleting the result"):
    sameFrom(
      lefts.join(Named("join-1"), rights)((a, b) => s"$a+$b").toStream(Named("to-stream-25")).to(Named("write-30"), out, text, text.option),
      builder =>
        javaTable(builder, "left", "lefts").join(javaTable(builder, "right", "rights"), (a: String, b: String) => s"$a+$b").toStream.to("out"),
      tabling
    )

  test("two tables left joined write what Java's left join of tables does, a right side's delete joining the left with none"):
    sameFrom(
      lefts.leftJoin(Named("left-join-1"), rights)((a, b) => either(a, b.orNull)).toStream(Named("to-stream-26"))
        .to(Named("write-31"), out, text, text.option),
      builder => javaTable(builder, "left", "lefts").leftJoin(javaTable(builder, "right", "rights"), either).toStream.to("out"),
      tabling
    )

  test("two tables outer joined write what Java's outer join of tables does"):
    sameFrom(
      lefts.outerJoin(Named("outer-join-1"), rights)(ior).toStream(Named("to-stream-27")).to(Named("write-32"), out, text, text.option),
      builder => javaTable(builder, "left", "lefts").outerJoin(javaTable(builder, "right", "rights"), either).toStream.to("out"),
      tabling
    )

  for (kind, ours, java) <-
      List[(String, KTable.Of[IO, String, String, Option] => KTable.Of[IO, String, String, Option], JavaJoin)](
        (
          "joined",
          _.join(Named("joined"), rights, Store("joined"), text)((a, b) => s"$a+$b"),
          (l, r, m) => l.join(r, (a: String, b: String) => s"$a+$b", m)
        ),
        (
          "left joined",
          _.leftJoin(Named("joined"), rights, Store("joined"), text)((a, b) => either(a, b.orNull)),
          (l, r, m) => l.leftJoin(r, either, m)
        ),
        ("outer joined", _.outerJoin(Named("joined"), rights, Store("joined"), text)(ior), (l, r, m) => l.outerJoin(r, either, m))
      )
  do
    test(s"two tables $kind into a store write its changelog as Java's join given a store does"):
      sameFrom(
        ours(lefts).toStream(Named("to-stream-28")).to(Named("write-33"), out, text, text.option),
        builder => java(javaTable(builder, "left", "lefts"), javaTable(builder, "right", "rights"), javaStored("joined")).toStream.to("out"),
        tabling
      )

    test(s"two tables $kind into a store and suppressed hold the old values Java's join passes on to its suppression"):
      sameFrom(
        ours(lefts).suppress(Suppressed.untilTimeLimit("calm", 2.seconds)).toStream(Named("to-stream-29"))
          .to(Named("write-34"), out, text, text.option),
        builder =>
          java(javaTable(builder, "left", "lefts"), javaTable(builder, "right", "rights"), javaStored("joined"))
            .suppress(JavaSuppressed.untilTimeLimit(Duration.ofSeconds(2), BufferConfig.unbounded()).withName("calm")).toStream.to("out"),
        tabling
      )

  test("a table joined with two tables joined reads their result as Java's join value getter makes it"):
    sameFrom(
      KTable(Named("thirds"), Topic.from("third").orFail, text, text, Store("thirds"))
        .leftJoin(Named("left-join-2"), lefts.outerJoin(Named("outer-join-2"), rights)(ior))((c, ab) => either(c, ab.orNull))
        .toStream(Named("to-stream-30")).to(Named("write-35"), out, text, text.option),
      builder =>
        javaTable(builder, "third", "thirds")
          .leftJoin(javaTable(builder, "left", "lefts").outerJoin(javaTable(builder, "right", "rights"), either), either).toStream.to("out"),
      tabling ++ List(("third", "k1", "c1", 21000L), ("third", "k2", "c2", 21500L), ("third", "k5", "c5", 22000L), ("left", "k2", "a2+", 22500L))
    )

  // Late changes to the third table, so the time the joined tables' view gives wins: k1's left value is older than its right one.
  test("a table joined with two tables left joined reads their result and its time as Java's left join value getter gives them"):
    sameFrom(
      KTable(Named("thirds"), Topic.from("third").orFail, text, text, Store("thirds"))
        .join(Named("join-2"), lefts.leftJoin(Named("left-join-3"), rights)((a, b) => either(a, b.orNull)))((c, ab) => s"$c:$ab")
        .toStream(Named("to-stream-31")).to(Named("write-36"), out, text, text.option),
      builder =>
        javaTable(builder, "third", "thirds")
          .join(javaTable(builder, "left", "lefts").leftJoin(javaTable(builder, "right", "rights"), either), (c: String, ab: String) => s"$c:$ab")
          .toStream.to("out"),
      tabling ++ List(("third", "k1", "c1", 100L), ("third", "k2", "c2", 200L), ("left", "k5", "a5", 300L), ("third", "k5", "c5", 100L))
    )

  test("two tables joined and left joined with a third pass on the old values Java's left join asks of their join"):
    sameFrom(
      lefts.join(Named("join-3"), rights)((a, b) => s"$a+$b")
        .leftJoin(Named("left-join-thirds"), KTable(Named("thirds"), Topic.from("third").orFail, text, text, Store("thirds")))((ab, c) =>
          either(ab, c.orNull)
        ).toStream(Named("to-stream-32")).to(Named("write-37"), out, text, text.option),
      builder =>
        javaTable(builder, "left", "lefts").join(javaTable(builder, "right", "rights"), (a: String, b: String) => s"$a+$b")
          .leftJoin(javaTable(builder, "third", "thirds"), either).toStream.to("out"),
      List(("third", "k2", "c2", 500L)) ++ tabling
    )

  test("a stream joined with two tables joined reads their result as Java's join value getter makes it"):
    sameFrom(
      KStream(Named("orders"), Topic.from("orders").orFail, text, text)
        .join(Named("join-4"), lefts.leftJoin(Named("left-join-4"), rights)((a, b) => either(a, b.orNull)))((o, ab) => s"$o:$ab")
        .to(Named("write-38"), out, text, text),
      builder =>
        builder.stream[String, String]("orders")
          .join(javaTable(builder, "left", "lefts").leftJoin(javaTable(builder, "right", "rights"), either), (o: String, ab: String) => s"$o:$ab")
          .to("out"),
      tabling ++ List(("orders", "k1", "o1", 21000L), ("orders", "k2", "o2", 21500L), ("orders", "k3", "o3", 22000L))
    )

  test("a counted table left joined with a table passes on its old counts as Java's does"):
    sameFrom(
      KStream(Named("logins"), Topic.from("logins").orFail, text, text).groupByKey(text, text).count(Named("count-12"), Store("logins-count"))
        .leftJoin(Named("joined"), rights, Store("joined"), text)((n, b) => either(n.toString, b.orNull))
        .suppress(Suppressed.untilTimeLimit("calm", 2.seconds)).toStream(Named("to-stream-33")).to(Named("write-39"), out, text, text.option),
      builder =>
        builder.stream[String, String]("logins").groupByKey.count(counted("logins-count"))
          .leftJoin(javaTable(builder, "right", "rights"), (n: java.lang.Long, b: String) => either(n.toString, b), javaStored("joined"))
          .suppress(JavaSuppressed.untilTimeLimit(Duration.ofSeconds(2), BufferConfig.unbounded()).withName("calm")).toStream.to("out"),
      tabling.map((topic, key, value, at) => if topic == "left" then ("logins", key, value, at) else (topic, key, value, at)).filter(_._3 != null)
    )

  test("a suppressed table joined with a table passes on the old values its suppression held, as Java's does"):
    sameFrom(
      lefts.suppress(Suppressed.untilTimeLimit("calm", 2.seconds)).join(Named("joined"), rights, Store("joined"), text)((a, b) => s"$a+$b")
        .toStream(Named("to-stream-34")).to(Named("write-40"), out, text, text.option),
      builder =>
        javaTable(builder, "left", "lefts").suppress(JavaSuppressed.untilTimeLimit(Duration.ofSeconds(2), BufferConfig.unbounded()).withName("calm"))
          .join(javaTable(builder, "right", "rights"), (a: String, b: String) => s"$a+$b", javaStored("joined")).toStream.to("out"),
      // Then k5's first value goes on, its second is held, and a late change on the right joins the value from before it, of a time Java does
      // not know, so the change's own time is the result's.
      tabling ++ List(("left", "k5", "a5", 30000L), ("left", "k6", "a6", 33000L), ("left", "k5", "a5+", 33500L), ("right", "k5", "b5", 1000L))
    )

  test("a table of records grouped by a new key holds where each came from in the repartition topic, as Java's buffer does"):
    sameEverywhere(
      KStream(Named("read-23"), in, text, text).groupBy(Repartition("by-value"), text, text)((k, v) => if v < "f" then k else s"$k!")
        .count(Named("count-13"), Store("counts")).suppress(Suppressed.untilTimeLimit("calm", 5.seconds)).toStream(Named("to-stream-35"))
        .to(Named("write-41"), out, text, count),
      _.stream[String, String]("in").groupBy((k, v) => if v < "f" then k else s"$k!", Grouped.as("by-value")).count(counted("counts"))
        .suppress(JavaSuppressed.untilTimeLimit(Duration.ofSeconds(5), BufferConfig.unbounded()).withName("calm")).toStream
        .to("out", Produced.`with`(Serdes.String, Serdes.Long)),
      updates
    )

  test("a full buffer set to shut down stops the task at the record Java's stops at"):
    val held     = Suppressed.untilWindowCloses("final", BufferLimit.records(2).shutDownWhenFull)
    val pipeline =
      KStream(Named("read-24"), in, text, text).groupByKey(text, text).windowedBy(tumbling).count(Named("count-14"), Store("counts")).suppress(held)
        .toStream(Named("to-stream-36")).to(Named("write-42"), out, Serde.timeWindowed(text, 10.seconds), count)
    val java: StreamsBuilder => Unit =
      _.stream[String, String]("in").groupByKey.windowedBy(javaWindows(10000L))
        .count(Materialized.as("counts").withKeySerde(Serdes.String).withValueSerde(Serdes.Long))
        .suppress(JavaSuppressed.untilWindowCloses(BufferConfig.maxRecords(2).shutDownWhenFull()).withName("final")).toStream
        .to("out", Produced.`with`(windowedSerde(10000L), Serdes.Long))
    val stopped = updates.indices.find(n => scala.util.Try(run(java, None, updates.take(n + 1))).isFailure)
    assert(stopped.nonEmpty, "Java did not stop")
    stopped.get.pure[IO].flatMap: n =>
      ours(pipeline, updates.take(n)).attempt.map(ran => assert(ran.isRight)) *> ours(pipeline, updates.take(n + 1)).attempt.map: ran =>
        assert(ran.left.exists(_.getMessage.contains("the suppression 'final' buffer exceeded its limit: 3/2 records")), ran)

  // Two streams within five seconds of each other and two of grace: matches both ways, two records of one key and time, records matching nothing on
  // either side, some held while their windows are open and sent once they close, some looks within a second of the last, a late record, and one
  // past the grace.
  private val streaming =
    List(
      ("left", "k1", "a1", 1000L),
      ("right", "k1", "b1", 2000L),  // joins a1
      ("left", "k2", "a2", 2500L),   // matches nothing yet
      ("right", "k1", "b1*", 2000L), // a second record of k1 at 2000, joining a1
      ("left", "k1", "a1*", 8000L),  // too late for b1 and b1*, so matches nothing
      ("right", "k3", "b3", 9000L),  // matches nothing
      ("right", "k2", "b2", 6000L),  // late, still joining a2
      ("left", "k4", "a4", 12000L),  // the stream time passes the windows of a2, a1* and b3
      ("left", "k1", "a1+", 1500L),  // late, joining b1 and b1*, though their windows have closed
      ("right", "k4", "b4", 12000L), // joins a4 at its own time
      ("left", "k5", "a5", 20000L),
      ("right", "k6", "b6", 21000L),
      ("left", "k1", "a1-", 3000L), // past the grace, the stores have dropped what it would join
      ("left", "k7", "a7", 40000L), // well after every other record
      ("right", "k7", "b7", 41000L)
    )

  private def leftStream  = KStream(Named("left"), Topic.from("left").orFail, text, text)
  private def rightStream = KStream(Named("right"), Topic.from("right").orFail, text, text)
  private val within      = JoinWindows(5.seconds, grace = 2.seconds)
  private val javaWithin  = JavaJoinWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(5), Duration.ofSeconds(2))

  private def javaPaired = StreamJoined.`with`(Serdes.String, Serdes.String, Serdes.String).withName("pairs").withStoreName("pairs")

  test("two streams joined write what Java's join of streams does, each pair in a window at the later of their times"):
    sameFrom(
      leftStream.join(Named("pairs"), rightStream, within, Store("pairs"), text, text, text)((a, b) => s"$a+$b")
        .to(Named("write-43"), out, text, text),
      builder =>
        builder.stream[String, String]("left")
          .join(builder.stream[String, String]("right"), (a: String, b: String) => s"$a+$b", javaWithin, javaPaired).to("out"),
      streaming
    )

  test("two streams left joined write what Java's left join of streams does, a left record matching nothing sent alone once its window closes"):
    sameFrom(
      leftStream.leftJoin(Named("pairs"), rightStream, within, Store("pairs"), text, text, text)((a, b) => either(a, b.orNull))
        .to(Named("write-44"), out, text, text),
      builder =>
        builder.stream[String, String]("left")
          .leftJoin(builder.stream[String, String]("right"), (a: String, b: String) => either(a, b), javaWithin, javaPaired).to("out"),
      streaming
    )

  test("two streams outer joined write what Java's outer join of streams does, a record of either side matching nothing sent alone"):
    sameFrom(
      leftStream.outerJoin(Named("outer-join-3"), rightStream, within, Store("pairs"), text, text, text)(ior).to(Named("write-45"), out, text, text),
      builder =>
        builder.stream[String, String]("left")
          .outerJoin(builder.stream[String, String]("right"), (a: String, b: String) => either(a, b), javaWithin, javaPaired).to("out"),
      streaming
    )

  // Records matching nothing, held and then looked for, 600 ms apart by the wall clock, five seconds of window and two of grace: a look that ends
  // once both sides hold a record whose window is open, a look the wall clock holds back as within a second of the last, a record whose window
  // reaches the stream time exactly, which is held, and a last record that sends on what is left.
  private val looking =
    List(
      ("left", "k1", "a1", 10000L),
      ("right", "k2", "b2", 12000L),
      ("left", "k3", "a3", 12500L),
      ("right", "k4", "b4", 13000L),
      ("left", "k5", "a5", 14000L),
      ("right", "k9", "b9", 17500L),  // sends a1 on, ending at b4 once both sides are open
      ("left", "k8", "a8", 20500L),   // held back, a second not having passed since the last look
      ("left", "k10", "a10", 20600L), // looks again, from the earliest time the last look reached
      ("left", "k11", "a11", 15600L), // its window reaches 20600, the stream time, so it is held
      ("right", "k12", "b12", 30000L)
    )

  test("a join looks for held records whose windows have closed as Java's does, no sooner than a second after the last look"):
    List(
      (
        leftStream.outerJoin(Named("outer-join-4"), rightStream, within, Store("pairs"), text, text, text)(ior)
          .to(Named("write-46"), out, text, text),
        (builder: StreamsBuilder) =>
          builder.stream[String, String]("left")
            .outerJoin(builder.stream[String, String]("right"), (a: String, b: String) => either(a, b), javaWithin, javaPaired).to("out")
      ),
      (
        leftStream.leftJoin(Named("pairs"), rightStream, within, Store("pairs"), text, text, text)((a, b) => either(a, b.orNull))
          .to(Named("write-47"), out, text, text),
        (builder: StreamsBuilder) =>
          builder.stream[String, String]("left")
            .leftJoin(builder.stream[String, String]("right"), (a: String, b: String) => either(a, b), javaWithin, javaPaired).to("out")
      )
    ).traverse_((pipeline, java) => sameFrom(pipeline, java, looking))

  test("streams joined in windows reaching further one way than the other write what Java's do"):
    List(
      (within.withBefore(1.second).withAfter(6.seconds), javaWithin.before(Duration.ofSeconds(1)).after(Duration.ofSeconds(6))),
      (within.withBefore(7.seconds).withAfter(-2.seconds), javaWithin.before(Duration.ofSeconds(7)).after(Duration.ofSeconds(-2)))
    ).traverse_ : (windows, javaWindows) =>
      sameFrom(
        leftStream.outerJoin(Named("outer-join-5"), rightStream, windows, Store("pairs"), text, text, text)(ior)
          .to(Named("write-48"), out, text, text),
        builder =>
          builder.stream[String, String]("left")
            .outerJoin(builder.stream[String, String]("right"), (a: String, b: String) => either(a, b), javaWindows, javaPaired).to("out"),
        streaming
      )

  test("a stream whose key changed is joined with a stream through the repartition topic its join names, as Java's is"):
    sameFrom(
      leftStream.selectKey(Named("select-key-2"))((k, _) => s"$k!")
        .leftJoin(Named("pairs"), rightStream.selectKey(Named("select-key-3"))((k, _) => s"$k!"), within, Store("pairs"), text, text, text)((a, b) =>
          either(a, b.orNull)
        ).to(Named("write-49"), out, text, text),
      builder =>
        builder.stream[String, String]("left").selectKey((k, _) => s"$k!").leftJoin(
          builder.stream[String, String]("right").selectKey((k, _) => s"$k!"),
          (a: String, b: String) => either(a, b),
          javaWithin,
          javaPaired
        ).to("out"),
      streaming
    )

  test("a record without a key goes on alone at once from a side whose records matching nothing go on, and is otherwise dropped, as in Java"):
    val keyless               = streaming.take(4) ++ List(("left", null, "a-", 2600L), ("right", null, "b-", 2700L)) ++ streaming.drop(4)
    def stream(topic: String) = KStream(Named(topic), Topic.from(topic).orFail, text.option, text)
    List[(Pipeline[IO], StreamsBuilder => Unit)](
      (
        stream("left").join(Named("pairs"), stream("right"), within, Store("pairs"), text.option, text, text)((a, b) => s"$a+$b")
          .to(Named("write-50"), out, text.option, text),
        builder =>
          builder.stream[String, String]("left")
            .join(builder.stream[String, String]("right"), (a: String, b: String) => s"$a+$b", javaWithin, javaPaired).to("out")
      ),
      (
        stream("left").leftJoin(Named("pairs"), stream("right"), within, Store("pairs"), text.option, text, text)((a, b) => either(a, b.orNull))
          .to(Named("write-51"), out, text.option, text),
        builder =>
          builder.stream[String, String]("left")
            .leftJoin(builder.stream[String, String]("right"), (a: String, b: String) => either(a, b), javaWithin, javaPaired).to("out")
      ),
      (
        stream("left").outerJoin(Named("outer-join-6"), stream("right"), within, Store("pairs"), text.option, text, text)(ior)
          .to(Named("write-52"), out, text.option, text),
        builder =>
          builder.stream[String, String]("left")
            .outerJoin(builder.stream[String, String]("right"), (a: String, b: String) => either(a, b), javaWithin, javaPaired).to("out")
      )
    ).traverse_((pipeline, java) => sameFrom(pipeline, java, keyless))

  /** Runs `inputs` through the program ours and Java's ways and compares what each writes to every topic but repartition topics, its stores'
    * changelogs and its suppression buffers' included, record for record and byte for byte.
    */
  /** Also whether Java wrote anything downstream, which must be as `wrote` says where it says. */
  private def sameEverywhere(
      pipeline: Pipeline[IO],
      java: StreamsBuilder => Unit,
      inputs: List[(String, String, Long)],
      wrote: Option[Boolean] = Some(true)
  ): IO[Boolean] = sameFrom(pipeline, java, inputs.map(("in", _, _, _)), wrote)

  /** As `sameEverywhere`, for records of several topics. */
  private def sameFrom(
      pipeline: Pipeline[IO],
      java: StreamsBuilder => Unit,
      inputs: List[(String, String, String, Long)],
      wrote: Option[Boolean] = Some(true)
  ): IO[Boolean] =
    oursFrom(pipeline, inputs).map: (written, _) =>
      val ran     = runFrom(java, None, inputs)
      val topics  = ran.topics.keySet.filterNot(_.endsWith("-repartition"))
      val oursAll = written.groupBy(_.topic.value).view.mapValues(_.map(record)).toMap
      wrote.foreach(expected => assertEquals(ran.out.nonEmpty, expected, "whether Java wrote downstream"))
      assertEquals(oursAll.keySet, topics)
      topics.foreach(topic => assertEquals(oursAll(topic), ran.topic(topic), topic))
      ran.out.nonEmpty

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
  private def same[V, A](
      pipeline: Pipeline[IO],
      java: (String => Materialized[String, V, WindowStore[Bytes, Array[Byte]]]) => StreamsBuilder => Unit,
      store: String,
      values: Deserializer[IO, A],
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

  /** Each segment's task on partition 0, a repartitioned record going on to the next segment at once, at the next offset of its topic, and each task
    * committing after each input, as Java's test driver runs them; with the last segment's state, which holds the window store.
    */
  private def ours(pipeline: Pipeline[IO], inputs: List[(String, String, Long)]): IO[(Vector[TaskOutput], TaskState)] =
    oursFrom(pipeline, inputs.map(("in", _, _, _)))

  /** As `ours`, for records of several topics, each going to the task whose sub-topology reads its topic, at the next offset of that topic. */
  private def oursFrom(pipeline: Pipeline[IO], inputs: List[(String, String, String, Long)]): IO[(Vector[TaskOutput], TaskState)] =
    val segments              = Topology.of(pipeline).orFail.pipelines.head.segments.toList.toVector
    val tasks                 = segments.map(Task[IO]("app", _, 0))
    def reader(topic: String) = segments.indexWhere(_.origins.exists(Task.topicOf("app", _) == topic))
    type Ran = (Vector[TaskOutput], Vector[TaskState], Map[String, Long])
    def go(topic: String, record: TaskOutput, states: Vector[TaskState], offsets: Map[String, Long], at: Long): IO[Ran] =
      val index  = reader(topic)
      val offset = offsets.getOrElse(topic, 0L)
      val input  = TaskInput(record.key, record.value, record.timestamp, record.headers, topic, 0, offset)
      tasks(index).process(states(index), input, at).flatMap: (state, outputs) =>
        outputs.foldLeftM[IO, Ran]((Vector.empty, states.updated(index, state), offsets.updated(topic, offset + 1))):
          case ((done, now, offsets), written) if written.topic.value.endsWith("-repartition") =>
            go(written.topic.value, written, now, offsets, at).map((more, after, moved) => (done ++ more, after, moved))
          case ((done, now, offsets), written) => IO.pure((done :+ written, now, offsets))
    inputs.zipWithIndex.foldLeftM[IO, Ran]((Vector.empty, Vector.fill(tasks.size)(TaskState.empty), Map.empty)):
      case ((done, states, offsets), ((topic, key, value, timestamp), index)) =>
        val record =
          TaskOutput(
            Topic.from(topic).orFail,
            None,
            Option(key).map(k => Chunk.array(k.getBytes)),
            Option(value).map(v => Chunk.array(v.getBytes)),
            timestamp,
            traced(index)
          )
        go(topic, record, states, offsets, wall(index)).flatMap: (more, after, moved) =>
          after.zipWithIndex.traverse((state, task) => tasks(task).flush(state)).map: flushed =>
            (done ++ more ++ flushed.flatMap(_._2), flushed.map(_._1), moved)
    .map((written, states, _) => (written, states.last))

  /** What Java's test driver writes to each topic for `inputs`, and what the window store `kept` names holds after them. */
  private def run(build: StreamsBuilder => Unit, kept: Option[String], inputs: List[(String, String, Long)]): Ran =
    runFrom(build, kept, inputs.map(("in", _, _, _)))

  /** As `run`, for records of several topics. */
  private def runFrom(build: StreamsBuilder => Unit, kept: Option[String], inputs: List[(String, String, String, Long)]): Ran =
    val properties = Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "app")
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1")
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0")
    // The test driver looks for closed windows, and for a join's held records, on every record unless told otherwise; an application looks at most
    // once a second.
    properties.put(StreamsConfig.InternalConfig.EMIT_INTERVAL_MS_KSTREAMS_WINDOWED_AGGREGATION, 1000L)
    properties.put(StreamsConfig.InternalConfig.EMIT_INTERVAL_MS_KSTREAMS_OUTER_JOIN_SPURIOUS_RESULTS_FIX, 1000L)
    properties.put(StreamsConfig.STATE_DIR_CONFIG, java.nio.file.Files.createTempDirectory("xkafka-window-golden").toString)
    val builder = StreamsBuilder()
    build(builder)
    val driver = TopologyTestDriver(builder.build(properties), properties, java.time.Instant.EPOCH)
    try
      val topics = inputs.map(_._1).distinct.map(name => name -> driver.createInputTopic(name, StringSerializer(), StringSerializer())).toMap
      inputs.zipWithIndex.foreach:
        case ((topic, key, value, timestamp), index) =>
          driver.advanceWallClockTime(Duration.ofMillis(wall(index) - (if index == 0 then 0L else wall(index - 1))))
          val headers = org.apache.kafka.common.header.internals.RecordHeaders()
          headers.add("trace", index.toString.getBytes)
          topics(topic).pipeInput(TestRecord(key, value, headers, java.time.Instant.ofEpochMilli(timestamp)))
      val written =
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
      Ran(written.toMap, held)
    finally driver.close()

  // The test driver reads the time of a record written without one as 0.
  private def record(written: TaskOutput): Output =
    Output(
      written.key.map(_.toVector),
      written.value.map(_.toVector),
      if written.timestamp == Task.NoTimestamp then 0L else written.timestamp,
      written.headers.values.map(h => h.key -> h.value.map(v => String(v.toArray)))
    )

object WindowGoldenSuite:
  /** A record as bytes, with its headers. */
  final case class Output(key: Option[Vector[Byte]], value: Option[Vector[Byte]], timestamp: Long, headers: Vector[(String, Option[String])])

  /** What Java wrote to each topic, and what its window store kept by key and window start. */
  final case class Ran(topics: Map[String, Vector[Output]], kept: Map[(String, Long), (String, Long)]):
    def out: Vector[Output]                 = topic("out")
    def topic(name: String): Vector[Output] = topics.getOrElse(name, Vector.empty)
