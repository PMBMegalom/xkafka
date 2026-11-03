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
  Grouped, JoinWindows as JavaJoinWindows, Materialized, Produced, SessionWindows as JavaSessionWindows, SlidingWindows as JavaSlidingWindows,
  StreamJoined, Suppressed as JavaSuppressed, TimeWindows as JavaTimeWindows, WindowedSerdes
}
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig
import org.apache.kafka.streams.processor.internals.{InternalTopologyBuilder, StreamThread, TopologyMetadata}
import org.apache.kafka.streams.state.{SessionStore, WindowStore}
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
        KStream(topic("in"), text, text).groupByKey(text, text).windowedBy(TimeWindows.tumbling(10.seconds, grace = 5.seconds))
          .count(Store("tumbling")).toStream.to(topic("out"), Serde.timeWindowed(text, 10.seconds), count),
        KStream(topic("other"), text, text).groupBy(Repartition("by-value"), text, text)((_, v) => v)
          .windowedBy(TimeWindows.hoppingWithNoGrace(1.minute, advance = 20.seconds)).reduce(Store("hopping"))(_ + _).toStream
          .to(topic("other-out"), Serde.timeWindowed(text, 1.minute), text)
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
        KStream(topic("in"), text, text).groupByKey(text, text).windowedBy(TimeWindows.tumbling(10.seconds, grace = 5.seconds)).withRetention(1.hour)
          .count(Store("kept")).toStream.to(topic("out"), Serde.timeWindowed(text, 10.seconds), count)
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
        KStream(topic("in"), text, text).groupByKey(text, text).windowedBy(SlidingWindows(10.seconds, grace = 5.seconds)).count(Store("sliding"))
          .toStream.to(topic("out"), Serde.timeWindowed(text, 10.seconds), count)
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
        KStream(topic("in"), text, text).groupByKey(text, text).windowedBy(SessionWindows(10.seconds, grace = 5.seconds)).count(Store("sessions"))
          .toStream.to(topic("out"), Serde.sessionWindowed(text), count.option)
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
        KStream(topic("in"), text, text).groupByKey(text, text).windowedBy(TimeWindows.tumbling(10.seconds, grace = 5.seconds)).count(Store("counts"))
          .suppress(Suppressed.untilWindowCloses("final")).toStream.to(topic("out"), Serde.timeWindowed(text, 10.seconds), count),
        KStream(topic("other"), text, text).groupByKey(text, text).count(Store("totals"))
          .suppress(Suppressed.untilTimeLimit("calm", 1.minute, BufferLimit.records(10).emitEarlyWhenFull)).toStream
          .to(topic("other-out"), text, count)
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
      ours(KTable(topic("users"), text, text, Store("users")).toStream.to(topic("out"), text, text.option)),
      declaredByJava(
        _.table[String, String]("users", Materialized.as[String, String, org.apache.kafka.streams.state.KeyValueStore[Bytes, Array[Byte]]]("users"))
          .toStream.to("out", Produced.`with`(Serdes.String, Serdes.String))
      )
    )

  private def javaTable(builder: StreamsBuilder, topic: String, store: String) =
    builder.table[String, String](topic, Materialized.as[String, String, org.apache.kafka.streams.state.KeyValueStore[Bytes, Array[Byte]]](store))

  test("a stream joined with a table read from a topic reads both in one sub-topology, copartitioned, as Java declares it"):
    assertEquals(
      ours(
        KStream(topic("orders"), text, text).join(KTable(topic("users"), text, text, Store("users")))((o, u) => o + u).to(topic("out"), text, text)
      ),
      declaredByJava: builder =>
        builder.stream[String, String]("orders").join(javaTable(builder, "users", "users"), (o: String, u: String) => o + u)
          .to("out", Produced.`with`(Serdes.String, Serdes.String))
    )

  test("two tables joined read both in one sub-topology, copartitioned, with no store of their own, as Java declares them"):
    assertEquals(
      ours(
        KTable(topic("left"), text, text, Store("lefts")).outerJoin(KTable(topic("right"), text, text, Store("rights")))(_.merge).toStream
          .to(topic("out"), text, text.option)
      ),
      declaredByJava: builder =>
        javaTable(builder, "left", "lefts").outerJoin(javaTable(builder, "right", "rights"), (a: String, b: String) => a + b).toStream
          .to("out", Produced.`with`(Serdes.String, Serdes.String))
    )

  test("two tables joined into a store declare its changelog, as Java declares a join given a store"):
    assertEquals(
      ours(
        KTable(topic("left"), text, text, Store("lefts")).join(KTable(topic("right"), text, text, Store("rights")), Store("joined"), text)(_ + _)
          .toStream.to(topic("out"), text, text.option)
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
        KStream(topic("logins"), text, text).groupByKey(text, text).count(Store("logins-count")).leftJoin(
          KTable(topic("third"), text, text, Store("thirds")).join(
            KTable(topic("left"), text, text, Store("lefts")).leftJoin(KTable(topic("right"), text, text, Store("rights")))((a, b) => a + b)
          )(_ + _)
        )((n, abc) => s"$n${abc.getOrElse("")}").toStream.to(topic("out"), text, text.option)
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
    def counted = KStream(topic("words"), text, text).groupBy(Repartition("by-word"), text, text)((_, v) => v).count(Store("counts"))
    def javaCounted(builder: StreamsBuilder) =
      builder.stream[String, String]("words").groupBy((_, v) => v, Grouped.as("by-word"))
        .count(Materialized.as[String, java.lang.Long, org.apache.kafka.streams.state.KeyValueStore[Bytes, Array[Byte]]]("counts"))
    val produced = Produced.`with`(Serdes.String, Serdes.String)
    assertEquals(
      ours(KStream(topic("orders"), text, text).join(counted)((o, n) => s"$o$n").to(topic("out"), text, text)),
      declaredByJava(builder =>
        builder.stream[String, String]("orders").join(javaCounted(builder), (o: String, n: java.lang.Long) => s"$o$n").to("out", produced)
      )
    )
    assertEquals(
      ours(KTable(topic("users"), text, text, Store("users")).join(counted)((u, n) => s"$u$n").toStream.to(topic("out"), text, text.option)),
      declaredByJava(builder =>
        javaTable(builder, "users", "users").join(javaCounted(builder), (u: String, n: java.lang.Long) => s"$u$n").toStream.to("out", produced)
      )
    )
    val within     = JoinWindows(5.seconds, grace = 2.seconds)
    val javaWithin = JavaJoinWindows.ofTimeDifferenceAndGrace(java.time.Duration.ofSeconds(5), java.time.Duration.ofSeconds(2))
    def paired     = StreamJoined.`with`(Serdes.String, Serdes.String, Serdes.String).withName("pairs").withStoreName("pairs")
    assertEquals(
      ours(
        KStream(topic("left"), text, text)
          .join(KStream(topic("right"), text, text).selectKey((_, v) => v), within, Store("pairs"), text, text, text)(_ + _).selectKey((_, v) => v)
          .groupByKey(Repartition("again"), text, text).count(Store("pair-counts")).toStream.to(topic("out"), text, Serde.long[IO])
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
    def left                              = KStream(topic("left"), text, text).selectKey((_, v) => v)
    def right                             = KStream(topic("right"), text, text)
    def javaLeft(builder: StreamsBuilder) = builder.stream[String, String]("left").selectKey((_, v) => v)
    def javaRight(builder: StreamsBuilder) = builder.stream[String, String]("right")
    val produced                           = Produced.`with`(Serdes.String, Serdes.String)
    assertEquals(
      ours(left.join(right, within, Store("pairs"), text, text, text)(_ + _).to(topic("out"), text, text)),
      declaredByJava(builder => javaLeft(builder).join(javaRight(builder), (a: String, b: String) => a + b, javaWithin, paired).to("out", produced))
    )
    assertEquals(
      ours(left.leftJoin(right, within, Store("pairs"), text, text, text)((a, b) => a + b.getOrElse("")).to(topic("out"), text, text)),
      declaredByJava(builder =>
        javaLeft(builder).leftJoin(javaRight(builder), (a: String, b: String) => a + b, javaWithin, paired).to("out", produced)
      )
    )
    assertEquals(
      ours(right.outerJoin(left, within, Store("pairs"), text, text, text)(_.merge).to(topic("out"), text, text)),
      declaredByJava(builder =>
        javaRight(builder).outerJoin(javaLeft(builder), (a: String, b: String) => a + b, javaWithin, paired).to("out", produced)
      )
    )

  test("a join's copartition group lists its topics in the order Java's set of them iterates, not the order the program reads them"):
    // Java's set of "clicks" and "pages" iterates "pages" first.
    assertEquals(
      ours(
        KStream(topic("clicks"), text, text).join(KTable(topic("pages"), text, text, Store("pages")))((c, p) => c + p).to(topic("out"), text, text)
      ),
      declaredByJava: builder =>
        builder.stream[String, String]("clicks").join(javaTable(builder, "pages", "pages"), (c: String, p: String) => c + p)
          .to("out", Produced.`with`(Serdes.String, Serdes.String))
    )

  test("a stream joined after its key changed goes through the repartition topic named for the join, as Java declares it"):
    assertEquals(
      ours(
        KStream(topic("orders"), text, text).selectKey((_, v) => v)
          .leftJoin(KTable(topic("users"), text, text, Store("users")), Repartition("by-user"), text, text)((o, u) => o + u.getOrElse("-"))
          .to(topic("out"), text, text)
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
        KStream(topic("orders"), text, text)
          .join(KStream(topic("visits"), text, text).groupBy(Repartition("by-page"), text, text)((_, v) => v).count(Store("visits")))((o, n) =>
            s"$o:$n"
          ).to(topic("out"), text, text)
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
        KStream(topic("orders"), text, text)
          .join(KTable(topic("users"), text, text, Store("users")).suppress(Suppressed.untilTimeLimit("calm", 1.second)))((o, u) => o + u)
          .to(topic("out"), text, text)
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
      ours(KStream(topic("in"), text, text).mapValues(_ + "!").to(topic("out"), text, text)),
      declaredByJava(_.stream[String, String]("in").mapValues(v => v + "!").to("out"))
    )

  test("counting by key keeps one sub-topology, with the store's changelog"):
    assertEquals(
      ours(KStream(topic("in"), text, text).groupByKey(text, text).count(Store("counts")).toStream.to(topic("out"), text, count)),
      declaredByJava(
        _.stream[String, String]("in").groupByKey.count(Materialized.as("counts")).toStream.to("out", Produced.`with`(Serdes.String, Serdes.Long))
      )
    )

  test("grouping by a new key splits the pipeline at a repartition topic"):
    assertEquals(
      ours(
        KStream(topic("in"), text, text).groupBy(Repartition("by-value"), text, text)((_, v) => v).count(Store("counts")).toStream
          .to(topic("out"), text, count)
      ),
      declaredByJava(
        _.stream[String, String]("in").groupBy((_, v) => v, Grouped.as("by-value")).count(Materialized.as("counts")).toStream
          .to("out", Produced.`with`(Serdes.String, Serdes.Long))
      )
    )

  test("sub-topologies are numbered in the order the pipelines are made, a repartitioned segment before a later pipeline"):
    assertEquals(
      ours(
        KStream(topic("b"), text, text).groupBy(Repartition("regroup"), text, text)((_, v) => v).reduce(Store("joined"))(_ + _).toStream
          .to(topic("out-b"), text, text),
        KStream(topic("a"), text, text).to(topic("out-a"), text, text)
      ),
      declaredByJava: builder =>
        builder.stream[String, String]("b").groupBy((_, v) => v, Grouped.as("regroup")).reduce((x, y) => x + y, Materialized.as("joined")).toStream
          .to("out-b")
        builder.stream[String, String]("a").to("out-a")
    )

  test("a key changed before grouping by key needs the named repartition"):
    assertEquals(
      ours(
        KStream(topic("in"), text, text).selectKey((_, v) => v).groupByKey(Repartition("rekeyed"), text, text)
          .aggregate(Store("agg"), text)("")((_, v, a) => a + v).toStream.to(topic("out"), text, text)
      ),
      declaredByJava(
        _.stream[String, String]("in").selectKey((_, v) => v).groupByKey(Grouped.as("rekeyed"))
          .aggregate(() => "", (_, v, a) => a + v, Materialized.as("agg")).toStream.to("out")
      )
    )

  test("a repartition name given for a key that did not change makes no repartition topic"):
    assertEquals(
      ours(
        KStream(topic("in"), text, text).filter((_, v) => v.nonEmpty).groupByKey(Repartition("unused"), text, text).count(Store("counts")).toStream
          .to(topic("out"), text, count)
      ),
      declaredByJava(
        _.stream[String, String]("in").filter((_, v) => v.nonEmpty).groupByKey(Grouped.as("unused")).count(Materialized.as("counts")).toStream
          .to("out", Produced.`with`(Serdes.String, Serdes.Long))
      )
    )

  test("every stateless step keeps its sub-topology, and a key changed by flatMap is repartitioned"):
    assertEquals(
      ours(
        KStream(topic("in"), text, text).filter((_, v) => v.nonEmpty).peek((_, _) => IO.unit).evalMapValues(v => IO.pure(v.trim))
          .flatMap((k, v) => List(v -> k)).mapValues(identity).groupByKey(Repartition("flipped"), text, text).reduce(Store("latest"))((_, v) => v)
          .toStream.map((k, v) => (v, k)).to(topic("out"), text, text)
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
        KStream(topic("in"), text, text).evalFilter((_, v) => IO.pure(v.nonEmpty)).evalMap((k, v) => IO.pure((v, k)))
          .groupByKey(Repartition("remapped"), text, text).count(Store("counts")).toStream.to(topic("out"), text, count)
      ),
      declaredByJava(
        _.stream[String, String]("in").filter((_, v) => v.nonEmpty).map((k, v) => org.apache.kafka.streams.KeyValue(v, k))
          .groupByKey(Grouped.as("remapped")).count(Materialized.as("counts")).toStream.to("out", Produced.`with`(Serdes.String, Serdes.Long))
      )
    )

  test("grouping a table's changes again makes a third sub-topology"):
    assertEquals(
      ours(
        KStream(topic("in"), text, text).groupBy(Repartition("first"), text, text)((_, v) => v).count(Store("counts")).toStream.mapValues(_.toString)
          .groupBy(Repartition("second"), text, text)((_, n) => n).reduce(Store("by-count"))((_, v) => v).toStream.to(topic("out"), text, text)
      ),
      declaredByJava(
        _.stream[String, String]("in").groupBy((_, v) => v, Grouped.as("first")).count(Materialized.as("counts")).toStream.mapValues(n => n.toString)
          .groupBy((_, n) => n, Grouped.as("second")).reduce((_, v) => v, Materialized.as("by-count")).toStream.to("out")
      )
    )

  test("two stores in one sub-topology are listed by name, not in the order the program makes them"):
    assertEquals(
      ours(
        KStream(topic("in"), text, text).groupByKey(text, text).count(Store("zeta")).toStream.groupByKey(text, count)
          .reduce(Store("alpha"))((_, n) => n).toStream.to(topic("out"), text, count)
      ),
      declaredByJava(
        _.stream[String, String]("in").groupByKey.count(Materialized.as("zeta")).toStream.groupByKey(Grouped.valueSerde(Serdes.Long))
          .reduce((_, n) => n, Materialized.as("alpha")).toStream.to("out", Produced.`with`(Serdes.String, Serdes.Long))
      )
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
