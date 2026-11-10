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
package streams

import java.util.{Optional, Properties, UUID}
import java.util.function.Supplier

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

import cats.effect.IO
import munit.FunSuite
import org.apache.kafka.clients.consumer.internals.StreamsRebalanceData
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.{StreamsBuilder, StreamsConfig, Topology as JavaTopology}
import org.apache.kafka.streams.kstream.{
  Grouped, JoinWindows as JavaJoinWindows, Materialized, Named as JavaNamed, Produced, Repartitioned, SessionWindows as JavaSessionWindows,
  SlidingWindows as JavaSlidingWindows, StreamJoined, Suppressed as JavaSuppressed, TimeWindows as JavaTimeWindows, WindowedSerdes
}
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig
import org.apache.kafka.streams.processor.internals.{InternalTopologyBuilder, StreamThread, TopologyMetadata}
import org.apache.kafka.streams.state.{KeyValueStore, SessionStore, WindowStore}
import internal.protocol.StreamsGroup
import internal.streams.Declaration

/** The topology the engine declares for a program, against what Java Streams declares for the same program, field by field. */
final class TopologyGoldenSuite extends FunSuite:
  import TopologyGoldenSuite.*

  private def topic(name: String): Topic = Topic.from(name).orFail

  private val text  = Serde.utf8[IO]
  private val count = Serde.long[IO]

  private def ours(first: Pipeline[IO], rest: Pipeline[IO]*): StreamsGroup.Topology = Declaration("app", Topology.of(first, rest*).orFail)

  test("a windowed count declares its window store's changelog as Java does, kept for the windows' size, grace and a day"):
    assertEquals(
      ours(
        KStream(Named("in"), topic("in"), text, text).groupByKey(text, text).windowedBy(TimeWindows.tumbling(10.seconds, grace = 5.seconds))
          .count(Named("count-1"), Store("tumbling")).toStream(Named("to-stream-1"))
          .to(Named("write-1"), topic("out"), Windowed.timeSerde(text, 10.seconds), count),
        KStream(Named("other"), topic("other"), text, text).groupBy(Repartition("by-value"), text, text)((_, v) => v)
          .windowedBy(TimeWindows.hoppingWithNoGrace(1.minute, advance = 20.seconds)).reduce(Named("reduce-1"), Store("hopping"))(_ + _)
          .toStream(Named("to-stream-2")).to(Named("write-2"), topic("other-out"), Windowed.timeSerde(text, 1.minute), text)
      ),
      declaredByJava: builder =>
        builder.stream[String, String]("in").groupByKey
          .windowedBy(JavaTimeWindows.ofSizeAndGrace(java.time.Duration.ofSeconds(10), java.time.Duration.ofSeconds(5)))
          .count(Materialized.as[String, java.lang.Long, WindowStore[Bytes, Array[Byte]]]("tumbling")).toStream
          .to("out", Produced.`with`(WindowedSerdes.timeWindowedSerdeFrom(classOf[String], 10000L), Serdes.Long))
        builder.stream[String, String]("other").groupBy((_, v) => v, Grouped.as("by-value"))
          .windowedBy(JavaTimeWindows.ofSizeWithNoGrace(java.time.Duration.ofMinutes(1)).advanceBy(java.time.Duration.ofSeconds(20)))
          .reduce((a, b) => a + b, Materialized.as[String, String, WindowStore[Bytes, Array[Byte]]]("hopping")).toStream
          .to("other-out", Produced.`with`(WindowedSerdes.timeWindowedSerdeFrom(classOf[String], 60000L), Serdes.String))
    )

  test("a window store kept longer declares its changelog kept that long and a day, as Java declares it"):
    assertEquals(
      ours(
        KStream(Named("in"), topic("in"), text, text).groupByKey(text, text).windowedBy(TimeWindows.tumbling(10.seconds, grace = 5.seconds))
          .withRetention(1.hour).count(Named("count-2"), Store("kept")).toStream(Named("to-stream-3"))
          .to(Named("write-3"), topic("out"), Windowed.timeSerde(text, 10.seconds), count)
      ),
      declaredByJava(
        _.stream[String, String]("in").groupByKey
          .windowedBy(JavaTimeWindows.ofSizeAndGrace(java.time.Duration.ofSeconds(10), java.time.Duration.ofSeconds(5)))
          .count(Materialized.as[String, java.lang.Long, WindowStore[Bytes, Array[Byte]]]("kept").withRetention(java.time.Duration.ofHours(1)))
          .toStream.to("out", Produced.`with`(WindowedSerdes.timeWindowedSerdeFrom(classOf[String], 10000L), Serdes.Long))
      )
    )

  test("a sliding window count declares its changelog kept for twice the time difference, the grace and a day, as Java declares it"):
    assertEquals(
      ours(
        KStream(Named("in"), topic("in"), text, text).groupByKey(text, text).windowedBy(SlidingWindows(10.seconds, grace = 5.seconds))
          .count(Named("count-3"), Store("sliding")).toStream(Named("to-stream-4"))
          .to(Named("write-4"), topic("out"), Windowed.timeSerde(text, 10.seconds), count)
      ),
      declaredByJava(
        _.stream[String, String]("in").groupByKey
          .windowedBy(JavaSlidingWindows.ofTimeDifferenceAndGrace(java.time.Duration.ofSeconds(10), java.time.Duration.ofSeconds(5)))
          .count(Materialized.as[String, java.lang.Long, WindowStore[Bytes, Array[Byte]]]("sliding")).toStream
          .to("out", Produced.`with`(WindowedSerdes.timeWindowedSerdeFrom(classOf[String], 10000L), Serdes.Long))
      )
    )

  test("a session count declares its changelog kept for the inactivity gap, the grace and a day, as Java declares it"):
    assertEquals(
      ours(
        KStream(Named("in"), topic("in"), text, text).groupByKey(text, text).windowedBy(SessionWindows(10.seconds, grace = 5.seconds))
          .count(Named("count-4"), Store("sessions")).toStream(Named("to-stream-5"))
          .to(Named("write-5"), topic("out"), Windowed.sessionSerde(text), count.option)
      ),
      declaredByJava(
        _.stream[String, String]("in").groupByKey
          .windowedBy(JavaSessionWindows.ofInactivityGapAndGrace(java.time.Duration.ofSeconds(10), java.time.Duration.ofSeconds(5)))
          .count(Materialized.as[String, java.lang.Long, SessionStore[Bytes, Array[Byte]]]("sessions")).toStream
          .to("out", Produced.`with`(WindowedSerdes.sessionWindowedSerdeFrom(classOf[String]), Serdes.Long))
      )
    )

  test("a suppression declares its buffer's changelog as Java does, compacted, beside its table's store"):
    assertEquals(
      ours(
        KStream(Named("in"), topic("in"), text, text).groupByKey(text, text).windowedBy(TimeWindows.tumbling(10.seconds, grace = 5.seconds))
          .count(Named("count-5"), Store("counts")).suppress(Suppressed.untilWindowCloses("final")).toStream(Named("to-stream-6"))
          .to(Named("write-6"), topic("out"), Windowed.timeSerde(text, 10.seconds), count),
        KStream(Named("other"), topic("other"), text, text).groupByKey(text, text).count(Named("count-6"), Store("totals"))
          .suppress(Suppressed.untilTimeLimit("calm", 1.minute, BufferLimit.records(10).emitEarlyWhenFull)).toStream(Named("to-stream-7"))
          .to(Named("write-7"), topic("other-out"), text, count)
      ),
      declaredByJava: builder =>
        builder.stream[String, String]("in").groupByKey
          .windowedBy(JavaTimeWindows.ofSizeAndGrace(java.time.Duration.ofSeconds(10), java.time.Duration.ofSeconds(5)))
          .count(Materialized.as[String, java.lang.Long, WindowStore[Bytes, Array[Byte]]]("counts"))
          .suppress(JavaSuppressed.untilWindowCloses(BufferConfig.unbounded()).withName("final")).toStream
          .to("out", Produced.`with`(WindowedSerdes.timeWindowedSerdeFrom(classOf[String], 10000L), Serdes.Long))
        builder.stream[String, String]("other").groupByKey
          .count(Materialized.as[String, java.lang.Long, org.apache.kafka.streams.state.KeyValueStore[Bytes, Array[Byte]]]("totals"))
          .suppress(JavaSuppressed.untilTimeLimit(java.time.Duration.ofMinutes(1), BufferConfig.maxRecords(10)).withName("calm")).toStream
          .to("other-out", Produced.`with`(Serdes.String, Serdes.Long))
    )

  test("a table read from a topic declares its input and its store's changelog as Java does"):
    assertEquals(
      ours(
        KTable(Named("users"), topic("users"), text, text, Store("users")).toStream(Named("to-stream-8"))
          .to(Named("write-8"), topic("out"), text, text.option)
      ),
      declaredByJava(
        _.table[String, String]("users", Materialized.as[String, String, org.apache.kafka.streams.state.KeyValueStore[Bytes, Array[Byte]]]("users"))
          .toStream.to("out", Produced.`with`(Serdes.String, Serdes.String))
      )
    )

  private def counted(store: String) = Materialized.as[String, java.lang.Long, KeyValueStore[Bytes, Array[Byte]]](store).withValueSerde(Serdes.Long)

  private def javaTable(builder: StreamsBuilder, topic: String, store: String) =
    builder.table[String, String](topic, Materialized.as[String, String, org.apache.kafka.streams.state.KeyValueStore[Bytes, Array[Byte]]](store))

  test("a stream joined with a table read from a topic reads both in one sub-topology, copartitioned, as Java declares it"):
    assertEquals(
      ours(
        KStream(Named("orders"), topic("orders"), text, text)
          .join(Named("join-users"), KTable(Named("users"), topic("users"), text, text, Store("users")))((o, u) => o + u)
          .to(Named("write-9"), topic("out"), text, text)
      ),
      declaredByJava: builder =>
        builder.stream[String, String]("orders").join(javaTable(builder, "users", "users"), (o: String, u: String) => o + u)
          .to("out", Produced.`with`(Serdes.String, Serdes.String))
    )

  test("two tables joined read both in one sub-topology, copartitioned, with no store of their own, as Java declares them"):
    assertEquals(
      ours(
        KTable(Named("lefts"), topic("left"), text, text, Store("lefts"))
          .outerJoin(Named("outer-join-rights"), KTable(Named("rights"), topic("right"), text, text, Store("rights")))(_.merge)
          .toStream(Named("to-stream-9")).to(Named("write-10"), topic("out"), text, text.option)
      ),
      declaredByJava: builder =>
        javaTable(builder, "left", "lefts").outerJoin(javaTable(builder, "right", "rights"), (a: String, b: String) => a + b).toStream
          .to("out", Produced.`with`(Serdes.String, Serdes.String))
    )

  test("two tables joined into a store declare its changelog, as Java declares a join given a store"):
    assertEquals(
      ours(
        KTable(Named("lefts"), topic("left"), text, text, Store("lefts"))
          .join(Named("join-rights"), KTable(Named("rights"), topic("right"), text, text, Store("rights")), Store("joined"), text)(_ + _)
          .toStream(Named("to-stream-10")).to(Named("write-11"), topic("out"), text, text.option)
      ),
      declaredByJava: builder =>
        javaTable(builder, "left", "lefts").join(
          javaTable(builder, "right", "rights"),
          (a: String, b: String) => a + b,
          Materialized.as[String, String, org.apache.kafka.streams.state.KeyValueStore[Bytes, Array[Byte]]]("joined").withValueSerde(Serdes.String)
        ).toStream.to("out", Produced.`with`(Serdes.String, Serdes.String))
    )

  test("a table joined with two tables joined, and with a counted stream, copartitions every topic they read, as Java declares it"):
    assertEquals(
      ours(
        KStream(Named("logins"), topic("logins"), text, text).groupByKey(text, text).count(Named("count-7"), Store("logins-count")).leftJoin(
          Named("left-join-thirds"),
          KTable(Named("thirds"), topic("third"), text, text, Store("thirds")).join(
            Named("join-lefts"),
            KTable(Named("lefts"), topic("left"), text, text, Store("lefts"))
              .leftJoin(Named("left-join-rights"), KTable(Named("rights"), topic("right"), text, text, Store("rights")))((a, b) => a + b)
          )(_ + _)
        )((n, abc) => s"$n${abc.getOrElse("")}").toStream(Named("to-stream-11")).to(Named("write-12"), topic("out"), text, text.option)
      ),
      declaredByJava: builder =>
        val lefts = javaTable(builder, "left", "lefts").leftJoin(javaTable(builder, "right", "rights"), (a: String, b: String) => a + b)
        builder.stream[String, String]("logins").groupByKey.count(
          Materialized.as[String, java.lang.Long, org.apache.kafka.streams.state.KeyValueStore[Bytes, Array[Byte]]]("logins-count")
            .withValueSerde(Serdes.Long)
        ).leftJoin(
          javaTable(builder, "third", "thirds").join(lefts, (c: String, ab: String) => c + ab),
          (n: java.lang.Long, abc: String) => s"$n$abc"
        ).toStream.to("out", Produced.`with`(Serdes.String, Serdes.String))
    )

  // Java numbers sub-topologies by the first node of each it writes, and writes them roughly in the order the program makes them, receiver first; a
  // join's sub-topology holds the receiver's source, so it comes before the sub-topologies the other side made before it.
  test("a join is numbered before the sub-topologies its other side made, where its receiver is not repartitioned, as Java numbers it"):
    def counted =
      KStream(Named("words"), topic("words"), text, text).groupBy(Repartition("by-word"), text, text)((_, v) => v)
        .count(Named("count-8"), Store("counts"))
    def javaCounted(builder: StreamsBuilder) =
      builder.stream[String, String]("words").groupBy((_, v) => v, Grouped.as("by-word"))
        .count(Materialized.as[String, java.lang.Long, org.apache.kafka.streams.state.KeyValueStore[Bytes, Array[Byte]]]("counts"))
    val produced = Produced.`with`(Serdes.String, Serdes.String)
    assertEquals(
      ours(
        KStream(Named("orders"), topic("orders"), text, text).join(Named("join-1"), counted)((o, n) => s"$o$n")
          .to(Named("write-13"), topic("out"), text, text)
      ),
      declaredByJava(builder =>
        builder.stream[String, String]("orders").join(javaCounted(builder), (o: String, n: java.lang.Long) => s"$o$n").to("out", produced)
      )
    )
    assertEquals(
      ours(
        KTable(Named("users"), topic("users"), text, text, Store("users")).join(Named("join-2"), counted)((u, n) => s"$u$n")
          .toStream(Named("to-stream-12")).to(Named("write-14"), topic("out"), text, text.option)
      ),
      declaredByJava(builder =>
        javaTable(builder, "users", "users").join(javaCounted(builder), (u: String, n: java.lang.Long) => s"$u$n").toStream.to("out", produced)
      )
    )
    val within     = JoinWindows(5.seconds, grace = 2.seconds)
    val javaWithin = JavaJoinWindows.ofTimeDifferenceAndGrace(java.time.Duration.ofSeconds(5), java.time.Duration.ofSeconds(2))
    def paired     = StreamJoined.`with`(Serdes.String, Serdes.String, Serdes.String).withName("pairs").withStoreName("pairs")
    assertEquals(
      ours(
        KStream(Named("left"), topic("left"), text, text).join(
          Named("pairs"),
          KStream(Named("right"), topic("right"), text, text).selectKey(Named("select-key-1"))((_, v) => v),
          within,
          Store("pairs"),
          text,
          text,
          text
        )(_ + _).selectKey(Named("select-key-2"))((_, v) => v).groupByKey(Repartition("again"), text, text)
          .count(Named("count-9"), Store("pair-counts")).toStream(Named("to-stream-13")).to(Named("write-15"), topic("out"), text, Serde.long[IO])
      ),
      declaredByJava(builder =>
        builder.stream[String, String]("left")
          .join(builder.stream[String, String]("right").selectKey((_, v) => v), (a: String, b: String) => a + b, javaWithin, paired)
          .selectKey((_, v) => v).groupByKey(Grouped.as("again"))
          .count(Materialized.as[String, java.lang.Long, org.apache.kafka.streams.state.KeyValueStore[Bytes, Array[Byte]]]("pair-counts")).toStream
          .to("out", Produced.`with`(Serdes.String, Serdes.Long))
      )
    )

  test("each kind of stream-stream join declares its stores' changelogs and its sides' repartition topics as Java declares them"):
    val within                            = JoinWindows(5.seconds, grace = 2.seconds)
    val javaWithin                        = JavaJoinWindows.ofTimeDifferenceAndGrace(java.time.Duration.ofSeconds(5), java.time.Duration.ofSeconds(2))
    def paired                            = StreamJoined.`with`(Serdes.String, Serdes.String, Serdes.String).withName("pairs").withStoreName("pairs")
    def left                              = KStream(Named("left"), topic("left"), text, text).selectKey(Named("select-key-3"))((_, v) => v)
    def right                             = KStream(Named("right"), topic("right"), text, text)
    def javaLeft(builder: StreamsBuilder) = builder.stream[String, String]("left").selectKey((_, v) => v)
    def javaRight(builder: StreamsBuilder) = builder.stream[String, String]("right")
    val produced                           = Produced.`with`(Serdes.String, Serdes.String)
    assertEquals(
      ours(left.join(Named("pairs"), right, within, Store("pairs"), text, text, text)(_ + _).to(Named("write-16"), topic("out"), text, text)),
      declaredByJava(builder => javaLeft(builder).join(javaRight(builder), (a: String, b: String) => a + b, javaWithin, paired).to("out", produced))
    )
    assertEquals(
      ours(
        left.leftJoin(Named("pairs"), right, within, Store("pairs"), text, text, text)((a, b) => a + b.getOrElse(""))
          .to(Named("write-17"), topic("out"), text, text)
      ),
      declaredByJava(builder =>
        javaLeft(builder).leftJoin(javaRight(builder), (a: String, b: String) => a + b, javaWithin, paired).to("out", produced)
      )
    )
    assertEquals(
      ours(right.outerJoin(Named("pairs"), left, within, Store("pairs"), text, text, text)(_.merge).to(Named("write-18"), topic("out"), text, text)),
      declaredByJava(builder =>
        javaRight(builder).outerJoin(javaLeft(builder), (a: String, b: String) => a + b, javaWithin, paired).to("out", produced)
      )
    )

  test("a join's copartition group lists its topics in the order Java's set of them iterates, not the order the program reads them"):
    // Java's set of "clicks" and "pages" iterates "pages" first.
    assertEquals(
      ours(
        KStream(Named("clicks"), topic("clicks"), text, text)
          .join(Named("join-pages"), KTable(Named("pages"), topic("pages"), text, text, Store("pages")))((c, p) => c + p)
          .to(Named("write-19"), topic("out"), text, text)
      ),
      declaredByJava: builder =>
        builder.stream[String, String]("clicks").join(javaTable(builder, "pages", "pages"), (c: String, p: String) => c + p)
          .to("out", Produced.`with`(Serdes.String, Serdes.String))
    )

  // Java makes the source of a repartition topic a join needs only when it makes the join, after the other side's sub-topologies.
  test("a join that repartitions its receiver is numbered after the sub-topologies its other side made, as Java numbers it"):
    val produced = Produced.`with`(Serdes.String, Serdes.String)
    assertEquals(
      ours(
        KStream(Named("orders"), topic("orders"), text, text).selectKey(Named("key"))((_, v) => v).join(
          Named("joined"),
          KStream(Named("clicks"), topic("clicks"), text, text).groupBy(Repartition("by"), text, text)((_, v) => v)
            .count(Named("counted"), Store("counts")),
          text,
          text
        )((o, n) => s"$o$n").to(Named("write"), topic("out"), text, text)
      ),
      declaredByJava: builder =>
        val orders = builder.stream[String, String]("orders").selectKey((_, v) => v)
        val counts = builder.stream[String, String]("clicks").groupBy((_, v) => v, Grouped.as("by")).count(counted("counts"))
        orders.join(
          counts,
          (o: String, n: java.lang.Long) => s"$o$n",
          org.apache.kafka.streams.kstream.Joined.`with`(Serdes.String, Serdes.String, Serdes.Long).withName("joined")
        ).to("out", produced)
    )
    assertEquals(
      ours(
        KStream(Named("left"), topic("left"), text, text).selectKey(Named("key"))((_, v) => v).join(
          Named("pairs"),
          KStream(Named("right"), topic("right"), text, text).repartition(Repartition("first"), text, text),
          JoinWindows(5.seconds, grace = 2.seconds),
          Store("pairs"),
          text,
          text,
          text
        )(_ + _).to(Named("write"), topic("out"), text, text)
      ),
      declaredByJava: builder =>
        val left  = builder.stream[String, String]("left").selectKey((_, v) => v)
        val right = builder.stream[String, String]("right").repartition(Repartitioned.`with`(Serdes.String, Serdes.String).withName("first"))
        left.join(
          right,
          (a: String, b: String) => a + b,
          JavaJoinWindows.ofTimeDifferenceAndGrace(java.time.Duration.ofSeconds(5), java.time.Duration.ofSeconds(2)),
          StreamJoined.`with`(Serdes.String, Serdes.String, Serdes.String).withName("pairs").withStoreName("pairs")
        ).to("out", produced)
    )

  test("what follows a join keeps its stores and writes its repartition topics in the join's sub-topology, as Java declares it"):
    val produced = Produced.`with`(Serdes.String, Serdes.Long)
    assertEquals(
      ours(
        KStream(Named("left"), topic("left"), text, text).join(
          Named("pairs"),
          KStream(Named("right"), topic("right"), text, text),
          JoinWindows(5.seconds, grace = 2.seconds),
          Store("pairs"),
          text,
          text,
          text
        )(_ + _).groupByKey(text, text).count(Named("counted"), Store("counts")).toStream(Named("to-stream"))
          .to(Named("write"), topic("out"), text, count)
      ),
      declaredByJava: builder =>
        builder.stream[String, String]("left").join(
          builder.stream[String, String]("right"),
          (a: String, b: String) => a + b,
          JavaJoinWindows.ofTimeDifferenceAndGrace(java.time.Duration.ofSeconds(5), java.time.Duration.ofSeconds(2)),
          StreamJoined.`with`(Serdes.String, Serdes.String, Serdes.String).withName("pairs").withStoreName("pairs")
        ).groupByKey.count(counted("counts")).toStream.to("out", produced)
    )
    assertEquals(
      ours(
        KTable(Named("users"), topic("users"), text, text, Store("users"))
          .join(Named("joined"), KTable(Named("plans"), topic("plans"), text, text, Store("plans")))(_ + _).toStream(Named("to-stream"))
          .selectKey(Named("key"))((_, v) => v.getOrElse("")).groupByKey(Repartition("by"), text, text.option)
          .count(Named("counted"), Store("counts")).toStream(Named("to-stream-2")).to(Named("write"), topic("out"), text, count)
      ),
      declaredByJava: builder =>
        javaTable(builder, "users", "users").join(javaTable(builder, "plans", "plans"), (a: String, b: String) => a + b).toStream
          .selectKey((_, v) => v).groupByKey(Grouped.as("by")).count(counted("counts")).toStream.to("out", produced)
    )

  test("a join copartitions for a table the topics Java's table gives it, which a suppression clears and later table steps keep"):
    val produced = Produced.`with`(Serdes.String, Serdes.String)
    def calm     = Suppressed.untilTimeLimit("calm", 1.second)
    def javaCalm = JavaSuppressed.untilTimeLimit[String](java.time.Duration.ofSeconds(1), BufferConfig.unbounded()).withName("calm")
    def users    = KTable(Named("users"), topic("users"), text, text, Store("users"))
    def plans    = KTable(Named("plans"), topic("plans"), text, text, Store("plans"))
    def orders   = KStream(Named("orders"), topic("orders"), text, text)
    def write(stream: KStream.Unkeyed[IO, String, String]) = stream.to(Named("write"), topic("out"), text, text)
    // Filtered after the suppression.
    assertEquals(
      ours(write(orders.join(Named("j"), users.suppress(calm).filter(Named("f"))((_, _) => true))(_ + _))),
      declaredByJava: builder =>
        builder.stream[String, String]("orders")
          .join(javaTable(builder, "users", "users").suppress(javaCalm).filter((_, _) => true), (o: String, u: String) => o + u).to("out", produced)
    )
    // Changed after the suppression.
    assertEquals(
      ours(write(orders.join(Named("j"), users.suppress(calm).mapValues(Named("m"))(identity))(_ + _))),
      declaredByJava: builder =>
        builder.stream[String, String]("orders")
          .join(javaTable(builder, "users", "users").suppress(javaCalm).mapValues((v: String) => v), (o: String, u: String) => o + u)
          .to("out", produced)
    )
    // Made a stream and a table again after the suppression.
    assertEquals(
      ours(write(
        orders.join(
          Named("j"),
          users.suppress(calm).toStream(Named("ts")).filter(Named("present"))((_, v) => v.nonEmpty).mapValues(Named("got"))(_.getOrElse(""))
            .toTable(Named("again"), Store("again"), text, text)
        )(_ + _)
      )),
      declaredByJava: builder =>
        builder.stream[String, String]("orders").join(
          javaTable(builder, "users", "users").suppress(javaCalm).toStream.filter((_, v) => v != null).mapValues((v: String) => v)
            .toTable(Materialized.as[String, String, KeyValueStore[Bytes, Array[Byte]]]("again")),
          (o: String, u: String) => o + u
        ).to("out", produced)
    )
    // A stream of a suppressed table joining a table.
    assertEquals(
      ours(
        users.suppress(calm).toStream(Named("ts")).filter(Named("present"))((_, v) => v.nonEmpty).mapValues(Named("got"))(_.getOrElse(""))
          .join(Named("j"), plans)(_ + _).to(Named("write"), topic("out"), text, text)
      ),
      declaredByJava: builder =>
        javaTable(builder, "users", "users").suppress(javaCalm).toStream.filter((_, v) => v != null).mapValues((v: String) => v)
          .join(javaTable(builder, "plans", "plans"), (a: String, b: String) => a + b).to("out", produced)
    )
    // A stream of a suppressed table joining a stream.
    assertEquals(
      ours(
        users.suppress(calm).toStream(Named("ts")).filter(Named("present"))((_, v) => v.nonEmpty).mapValues(Named("got"))(_.getOrElse(""))
          .join(Named("pairs"), orders, JoinWindows(5.seconds, grace = 2.seconds), Store("pairs"), text, text, text)(_ + _)
          .to(Named("write"), topic("out"), text, text)
      ),
      declaredByJava: builder =>
        javaTable(builder, "users", "users").suppress(javaCalm).toStream.filter((_, v) => v != null).mapValues((v: String) => v).join(
          builder.stream[String, String]("orders"),
          (a: String, b: String) => a + b,
          JavaJoinWindows.ofTimeDifferenceAndGrace(java.time.Duration.ofSeconds(5), java.time.Duration.ofSeconds(2)),
          StreamJoined.`with`(Serdes.String, Serdes.String, Serdes.String).withName("pairs").withStoreName("pairs")
        ).to("out", produced)
    )
    // A table made of a stream joined with a suppressed table, joined in turn.
    assertEquals(
      ours(write(
        KStream(Named("pays"), topic("pays"), text, text)
          .join(Named("j2"), orders.join(Named("j"), users.suppress(calm))(_ + _).toTable(Named("again"), Store("again"), text, text))(_ + _)
      )),
      declaredByJava: builder =>
        val again =
          builder.stream[String, String]("orders").join(javaTable(builder, "users", "users").suppress(javaCalm), (o: String, u: String) => o + u)
            .toTable(Materialized.as[String, String, KeyValueStore[Bytes, Array[Byte]]]("again"))
        builder.stream[String, String]("pays").join(again, (o: String, u: String) => o + u).to("out", produced)
    )
    // The result of joining a suppressed table.
    assertEquals(
      ours(write(orders.join(Named("j"), users.suppress(calm).join(Named("joined"), plans)(_ + _))(_ + _))),
      declaredByJava: builder =>
        builder.stream[String, String]("orders").join(
          javaTable(builder, "users", "users").suppress(javaCalm).join(javaTable(builder, "plans", "plans"), (a: String, b: String) => a + b),
          (o: String, u: String) => o + u
        ).to("out", produced)
    )
    // A suppressed table that came from a join, joined with another: the first join's topics stay grouped.
    assertEquals(
      ours(
        orders.join(Named("j"), users)(_ + _).toTable(Named("again"), Store("again"), text, text).suppress(calm).join(Named("joined"), plans)(_ + _)
          .toStream(Named("ts")).to(Named("write"), topic("out"), text, text.option)
      ),
      declaredByJava: builder =>
        builder.stream[String, String]("orders").join(javaTable(builder, "users", "users"), (o: String, u: String) => o + u)
          .toTable(Materialized.as[String, String, KeyValueStore[Bytes, Array[Byte]]]("again")).suppress(javaCalm)
          .join(javaTable(builder, "plans", "plans"), (a: String, b: String) => a + b).toStream.to("out", produced)
    )

  test(
    "a join's topics take in the groups of earlier joins they overlap, and leave apart those of a table a suppression cleared, as Java declares them"
  ):
    val produced = Produced.`with`(Serdes.String, Serdes.String)
    def calm     = Suppressed.untilTimeLimit("calm", 1.second)
    def javaCalm = JavaSuppressed.untilTimeLimit[String](java.time.Duration.ofSeconds(1), BufferConfig.unbounded()).withName("calm")
    def users    = KTable(Named("users"), topic("users"), text, text, Store("users"))
    def plans    = KTable(Named("plans"), topic("plans"), text, text, Store("plans"))
    assertEquals(
      ours(
        KStream(Named("orders"), topic("orders"), text, text).join(Named("with-user"), users)(_ + _).join(
          Named("pairs"),
          KStream(Named("pays"), topic("pays"), text, text).join(Named("with-plan"), plans)(_ + _),
          JoinWindows(5.seconds, grace = 2.seconds),
          Store("pairs"),
          text,
          text,
          text
        )(_ + _).to(Named("write"), topic("out"), text, text)
      ),
      declaredByJava: builder =>
        builder.stream[String, String]("orders").join(javaTable(builder, "users", "users"), (o: String, u: String) => o + u).join(
          builder.stream[String, String]("pays").join(javaTable(builder, "plans", "plans"), (p: String, u: String) => p + u),
          (a: String, b: String) => a + b,
          JavaJoinWindows.ofTimeDifferenceAndGrace(java.time.Duration.ofSeconds(5), java.time.Duration.ofSeconds(2)),
          StreamJoined.`with`(Serdes.String, Serdes.String, Serdes.String).withName("pairs").withStoreName("pairs")
        ).to("out", produced)
    )
    // The cleared table is the join's other side.
    assertEquals(
      ours(
        plans.join(
          Named("joined"),
          KStream(Named("orders"), topic("orders"), text, text).join(Named("with-user"), users)(_ + _)
            .toTable(Named("again"), Store("again"), text, text).suppress(calm)
        )(_ + _).toStream(Named("ts")).to(Named("write"), topic("out"), text, text.option)
      ),
      declaredByJava: builder =>
        javaTable(builder, "plans", "plans").join(
          builder.stream[String, String]("orders").join(javaTable(builder, "users", "users"), (o: String, u: String) => o + u)
            .toTable(Materialized.as[String, String, KeyValueStore[Bytes, Array[Byte]]]("again")).suppress(javaCalm),
          (a: String, b: String) => a + b
        ).toStream.to("out", produced)
    )

  test("a stream joined after its key changed goes through the repartition topic named for the join, as Java declares it"):
    assertEquals(
      ours(
        KStream(Named("orders"), topic("orders"), text, text).selectKey(Named("select-key-4"))((_, v) => v)
          .leftJoin(Named("by-user"), KTable(Named("users"), topic("users"), text, text, Store("users")), text, text)((o, u) => o + u.getOrElse("-"))
          .to(Named("write-20"), topic("out"), text, text)
      ),
      declaredByJava: builder =>
        builder.stream[String, String]("orders").selectKey((_, v) => v).leftJoin(
          javaTable(builder, "users", "users"),
          (o: String, u: String) => o + Option(u).getOrElse("-"),
          org.apache.kafka.streams.kstream.Joined.`with`(Serdes.String, Serdes.String, Serdes.String).withName("by-user")
        ).to("out", Produced.`with`(Serdes.String, Serdes.String))
    )

  test("a stream joined with a counted table runs in the sub-topology that counts, as Java declares it"):
    assertEquals(
      ours(
        KStream(Named("orders"), topic("orders"), text, text).join(
          Named("join-visits"),
          KStream(Named("visits"), topic("visits"), text, text).groupBy(Repartition("by-page"), text, text)((_, v) => v)
            .count(Named("count-10"), Store("visits"))
        )((o, n) => s"$o:$n").to(Named("write-21"), topic("out"), text, text)
      ),
      // Java numbers the join's sub-topology first only where the program builds the stream before the counted table, as xkafka takes it.
      declaredByJava: builder =>
        val orders  = builder.stream[String, String]("orders")
        val counted =
          builder.stream[String, String]("visits").groupBy((_, v) => v, Grouped.as("by-page"))
            .count(Materialized.as[String, java.lang.Long, org.apache.kafka.streams.state.KeyValueStore[Bytes, Array[Byte]]]("visits"))
        orders.join(counted, (o: String, n: java.lang.Long) => s"$o:$n").to("out", Produced.`with`(Serdes.String, Serdes.String))
    )

  test("a stream joined with a suppressed table declares the table's store and buffer beside it, as Java declares it"):
    assertEquals(
      ours(
        KStream(Named("orders"), topic("orders"), text, text).join(
          Named("join-users"),
          KTable(Named("users"), topic("users"), text, text, Store("users")).suppress(Suppressed.untilTimeLimit("calm", 1.second))
        )((o, u) => o + u).to(Named("write-22"), topic("out"), text, text)
      ),
      declaredByJava: builder =>
        builder.stream[String, String]("orders").join(
          javaTable(builder, "users", "users")
            .suppress(JavaSuppressed.untilTimeLimit(java.time.Duration.ofSeconds(1), BufferConfig.unbounded()).withName("calm")),
          (o: String, u: String) => o + u
        ).to("out", Produced.`with`(Serdes.String, Serdes.String))
    )

  test("a stateless pipeline is one sub-topology reading its input"):
    assertEquals(
      ours(KStream(Named("in"), topic("in"), text, text).mapValues(Named("map-values-1"))(_ + "!").to(Named("write-23"), topic("out"), text, text)),
      declaredByJava(_.stream[String, String]("in").mapValues(v => v + "!").to("out"))
    )

  test("counting by key keeps one sub-topology, with the store's changelog"):
    assertEquals(
      ours(
        KStream(Named("in"), topic("in"), text, text).groupByKey(text, text).count(Named("count-11"), Store("counts")).toStream(Named("to-stream-14"))
          .to(Named("write-24"), topic("out"), text, count)
      ),
      declaredByJava(
        _.stream[String, String]("in").groupByKey.count(Materialized.as("counts")).toStream.to("out", Produced.`with`(Serdes.String, Serdes.Long))
      )
    )

  test("grouping by a new key splits the pipeline at a repartition topic"):
    assertEquals(
      ours(
        KStream(Named("in"), topic("in"), text, text).groupBy(Repartition("by-value"), text, text)((_, v) => v)
          .count(Named("count-12"), Store("counts")).toStream(Named("to-stream-15")).to(Named("write-25"), topic("out"), text, count)
      ),
      declaredByJava(
        _.stream[String, String]("in").groupBy((_, v) => v, Grouped.as("by-value")).count(Materialized.as("counts")).toStream
          .to("out", Produced.`with`(Serdes.String, Serdes.Long))
      )
    )

  test("sub-topologies are numbered in the order the pipelines are made, a repartitioned segment before a later pipeline"):
    assertEquals(
      ours(
        KStream(Named("b"), topic("b"), text, text).groupBy(Repartition("regroup"), text, text)((_, v) => v)
          .reduce(Named("reduce-2"), Store("joined"))(_ + _).toStream(Named("to-stream-16")).to(Named("write-26"), topic("out-b"), text, text),
        KStream(Named("a"), topic("a"), text, text).to(Named("write-27"), topic("out-a"), text, text)
      ),
      declaredByJava: builder =>
        builder.stream[String, String]("b").groupBy((_, v) => v, Grouped.as("regroup")).reduce((x, y) => x + y, Materialized.as("joined")).toStream
          .to("out-b")
        builder.stream[String, String]("a").to("out-a")
    )

  test("a key changed before grouping by key needs the named repartition"):
    assertEquals(
      ours(
        KStream(Named("in"), topic("in"), text, text).selectKey(Named("select-key-5"))((_, v) => v).groupByKey(Repartition("rekeyed"), text, text)
          .aggregate(Named("aggregate-1"), Store("agg"), text)("")((_, v, a) => a + v).toStream(Named("to-stream-17"))
          .to(Named("write-28"), topic("out"), text, text)
      ),
      declaredByJava(
        _.stream[String, String]("in").selectKey((_, v) => v).groupByKey(Grouped.as("rekeyed"))
          .aggregate(() => "", (_, v, a) => a + v, Materialized.as("agg")).toStream.to("out")
      )
    )

  test("every stateless step keeps its sub-topology, and a key changed by flatMap is repartitioned"):
    assertEquals(
      ours(
        KStream(Named("in"), topic("in"), text, text).filter(Named("filter-2"))((_, v) => v.nonEmpty).peek(Named("peek-1"))((_, _) => IO.unit)
          .evalMapValues(Named("eval-map-values-1"))(v => IO.pure(v.trim)).flatMap(Named("flat-map-1"))((k, v) => List(v -> k))
          .mapValues(Named("map-values-2"))(identity).groupByKey(Repartition("flipped"), text, text)
          .reduce(Named("reduce-3"), Store("latest"))((_, v) => v).toStream(Named("to-stream-19")).map(Named("map-1"))((k, v) => (v, k))
          .to(Named("write-30"), topic("out"), text, text)
      ),
      declaredByJava(
        _.stream[String, String]("in").filter((_, v) => v.nonEmpty).peek((_, _) => ()).mapValues(v => v.trim)
          .flatMap((k, v) => java.util.List.of(org.apache.kafka.streams.KeyValue(v, k))).mapValues(v => v).groupByKey(Grouped.as("flipped"))
          .reduce((_, v) => v, Materialized.as("latest")).toStream.map((k, v) => org.apache.kafka.streams.KeyValue(v, k)).to("out")
      )
    )

  test("a key an effect may have changed is repartitioned, as Java repartitions a key map changes"):
    assertEquals(
      ours(
        KStream(Named("in"), topic("in"), text, text).evalFilter(Named("eval-filter-1"))((_, v) => IO.pure(v.nonEmpty))
          .evalMap(Named("eval-map-1"))((k, v) => IO.pure((v, k))).groupByKey(Repartition("remapped"), text, text)
          .count(Named("count-14"), Store("counts")).toStream(Named("to-stream-20")).to(Named("write-31"), topic("out"), text, count)
      ),
      declaredByJava(
        _.stream[String, String]("in").filter((_, v) => v.nonEmpty).map((k, v) => org.apache.kafka.streams.KeyValue(v, k))
          .groupByKey(Grouped.as("remapped")).count(Materialized.as("counts")).toStream.to("out", Produced.`with`(Serdes.String, Serdes.Long))
      )
    )

  test("grouping a table's changes again makes a third sub-topology"):
    assertEquals(
      ours(
        KStream(Named("in"), topic("in"), text, text).groupBy(Repartition("first"), text, text)((_, v) => v).count(Named("count-15"), Store("counts"))
          .toStream(Named("to-stream-21")).mapValues(Named("map-values-3"))(_.toString).groupBy(Repartition("second"), text, text)((_, n) => n)
          .reduce(Named("reduce-4"), Store("by-count"))((_, v) => v).toStream(Named("to-stream-22")).to(Named("write-32"), topic("out"), text, text)
      ),
      declaredByJava(
        _.stream[String, String]("in").groupBy((_, v) => v, Grouped.as("first")).count(Materialized.as("counts")).toStream.mapValues(n => n.toString)
          .groupBy((_, n) => n, Grouped.as("second")).reduce((_, v) => v, Materialized.as("by-count")).toStream.to("out")
      )
    )

  test("two stores in one sub-topology are listed by name, not in the order the program makes them"):
    assertEquals(
      ours(
        KStream(Named("in"), topic("in"), text, text).groupByKey(text, text).count(Named("count-16"), Store("zeta")).toStream(Named("to-stream-23"))
          .groupByKey(text, count).reduce(Named("reduce-5"), Store("alpha"))((_, n) => n).toStream(Named("to-stream-24"))
          .to(Named("write-33"), topic("out"), text, count)
      ),
      declaredByJava(
        _.stream[String, String]("in").groupByKey.count(Materialized.as("zeta")).toStream.groupByKey(Grouped.valueSerde(Serdes.Long))
          .reduce((_, n) => n, Materialized.as("alpha")).toStream.to("out", Produced.`with`(Serdes.String, Serdes.Long))
      )
    )

  test("a repartition asked for, a stream made a table, and tables kept by filter and mapValues declare what Java declares"):
    assertEquals(
      ours(
        KStream(Named("read-1"), topic("in-1"), text, text).repartition(Repartition("through"), text, text).filterNot(Named("not"))((_, _) => false)
          .to(Named("write-1"), topic("out-1"), text, text),
        KStream(Named("read-2"), topic("in-2"), text, text).selectKey(Named("key"))((_, v) => v).toTable(Named("latest"), Store("latest"), text, text)
          .filter(Named("kept"), Store("kept"), text)((_, _) => true).mapValues(Named("loud"), Store("loud"), text)(identity)
          .mapValues(Named("unkept"))(identity).toStream(Named("to-stream-1")).to(Named("write-2"), topic("out-2"), text, text.option),
        KStream(Named("read-3"), topic("in-3"), text, text).foreach(Named("act"))((_, _) => IO.unit)
      ),
      declaredByJava: builder =>
        builder.stream[String, String]("in-1").repartition(Repartitioned.`with`(Serdes.String, Serdes.String).withName("through"))
          .filterNot((_, _) => false).to("out-1")
        builder.stream[String, String]("in-2").selectKey((_, v) => v)
          .toTable(JavaNamed.as("latest"), Materialized.as[String, String, KeyValueStore[Bytes, Array[Byte]]]("latest"))
          .filter((_, _) => true, Materialized.as[String, String, KeyValueStore[Bytes, Array[Byte]]]("kept"))
          .mapValues((v: String) => v, Materialized.as[String, String, KeyValueStore[Bytes, Array[Byte]]]("loud")).mapValues((v: String) => v)
          .toStream.to("out-2")
        builder.stream[String, String]("in-3").foreach((_, _) => ())
    )

object TopologyGoldenSuite:
  /** What Java Streams 4.4.0 declares for the program `build` makes, under application id `app`, taken from its own code
    * (`StreamThread.initStreamsRebalanceData`) and sorted as its client sorts it before sending (`StreamsGroupHeartbeatRequestManager`).
    */
  def declaredByJava(build: StreamsBuilder => Unit): StreamsGroup.Topology =
    val properties = Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "app")
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1")
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    val builder = StreamsBuilder()
    build(builder)
    val topology = builder.build(properties)
    val field    = classOf[JavaTopology].getDeclaredField("internalTopologyBuilder")
    field.setAccessible(true)
    val config   = StreamsConfig(properties)
    val metadata = TopologyMetadata(field.get(topology).asInstanceOf[InternalTopologyBuilder], config)
    metadata.buildAndRewriteTopology()
    val init = classOf[StreamThread].getDeclaredMethods.find(_.getName == "initStreamsRebalanceData").get
    init.setAccessible(true)
    val none: Supplier[java.util.Map[StreamsRebalanceData.TaskId, java.lang.Long]] = () => java.util.Map.of()
    val data = init.invoke(null, UUID.randomUUID, config, Optional.empty, Optional.empty, metadata, none, none).asInstanceOf[StreamsRebalanceData]
    def infos(topics: java.util.Map[String, StreamsRebalanceData.TopicInfo], partitions: Boolean) =
      topics.asScala.toList.sortBy(_._1).map: (name, info) =>
        StreamsGroup.TopicInfo(
          name,
          if partitions then info.numPartitions.orElse(0) else 0,
          info.replicationFactor.map(_.toShort).orElse(0.toShort),
          info.topicConfigs.asScala.toList.sortBy(_._1)
        )
    val subtopologies =
      data.subtopologies.asScala.toList.sortBy(_._1).map: (id, subtopology) =>
        StreamsGroup.Subtopology(
          id,
          subtopology.sourceTopics.asScala.toList.sorted,
          Nil,
          infos(subtopology.stateChangelogTopics, partitions = false),
          subtopology.repartitionSinkTopics.asScala.toList.sorted,
          infos(subtopology.repartitionSourceTopics, partitions = true),
          // As Java's heartbeat sends them: each group's topics, in the order its set iterates, as indexes into the sorted topic lists.
          subtopology.copartitionGroups.asScala.toList.map: group =>
            val sources      = subtopology.sourceTopics.asScala.toList.sorted
            val repartitions = subtopology.repartitionSourceTopics.asScala.keys.toList.sorted
            val names        = group.asScala.toList
            StreamsGroup.CopartitionGroup(
              names.map(sources.indexOf).filter(_ >= 0).map(_.toShort),
              Nil,
              names.map(repartitions.indexOf).filter(_ >= 0).map(_.toShort)
            )
        )
    StreamsGroup.Topology(0, subtopologies)
