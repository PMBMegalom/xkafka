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

import java.time.Duration

import scala.concurrent.duration.*

import cats.effect.IO
import munit.FunSuite
import org.apache.kafka.common.message.StreamsGroupTopologyDescriptionUpdateRequestData
import org.apache.kafka.common.protocol.MessageUtil
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.{KeyValue, StreamsBuilder}
import org.apache.kafka.streams.kstream.{
  Consumed, EmitStrategy, Grouped, Joined, JoinWindows as JavaJoinWindows, KStream as JavaKStream, KTable as JavaKTable, Materialized,
  Named as JavaNamed, Produced, SessionWindows as JavaSessionWindows, SlidingWindows as JavaSlidingWindows, StreamJoined,
  Suppressed as JavaSuppressed, TimeWindows as JavaTimeWindows, Windowed as JavaWindowed
}
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig
import org.apache.kafka.streams.state.KeyValueStore
import scodec.bits.ByteVector
import internal.protocol.StreamsGroupTopologyDescriptionUpdateRequest
import internal.streams.Declaration

/** Programs described as Java describes the same programs, every step named alike and built in the same order: the text of `describe`, and the
  * description a member pushes to the coordinator.
  */
final class DescribeGoldenSuite extends FunSuite:
  private val text  = Serde.utf8[IO]
  private val count = Serde.long[IO]
  private val s     = Serdes.String
  private val l     = Serdes.Long

  private def topic(name: String) = Topic.from(name).orFail

  /** A description's text, and the body of the request that pushes it for the application `app`. */
  private final case class Described(text: String, pushed: ByteVector)

  private def ours(first: Pipeline[IO], rest: Pipeline[IO]*): Described =
    val topology = Topology.of(first, rest*).orFail
    Described(
      topology.describe.toString,
      StreamsGroupTopologyDescriptionUpdateRequest("app", "member", 3, Declaration.described("app", topology)).body
    )

  /** Java's description, and the request its client pushes it in, as its stream thread makes it for the application `app`. */
  private def javas(build: StreamsBuilder => Unit): Described =
    val (topology, data) = JavaStreams.prepared("app")(build)
    val request          =
      new StreamsGroupTopologyDescriptionUpdateRequestData().setGroupId("app").setMemberId("member").setTopologyEpoch(3)
        .setTopologyDescription(data.wireTopologyDescription())
    Described(topology.describe().toString, ByteVector(MessageUtil.toByteBufferAccessor(request, 0.toShort).buffer()))

  private def stored[V](name: String) = Materialized.as[String, V, KeyValueStore[Bytes, Array[Byte]]](name)

  private def read(builder: StreamsBuilder, topic: String, name: String): JavaKStream[String, String] =
    builder.stream[String, String](topic, Consumed.`with`(s, s).withName(name))

  private def table(builder: StreamsBuilder, topic: String, name: String): JavaKTable[String, String] =
    builder.table[String, String](topic, Consumed.`with`(s, s).withName(name), stored[String](name).withKeySerde(s).withValueSerde(s))

  private def written[V](name: String, value: org.apache.kafka.common.serialization.Serde[V]) = Produced.`with`(s, value).withName(name)

  test("stateless steps, then records grouped by a key they were given, through a repartition topic"):
    assertEquals(
      ours(
        KStream(Named("read"), topic("in"), text, text).filter(Named("filter"))((_, v) => v.nonEmpty).mapValues(Named("map-values"))(identity)
          .peek(Named("peek"))((_, _) => IO.unit).flatMap(Named("flat-map"))((k, v) => List(v -> k)).selectKey(Named("select-key"))((_, v) => v)
          .groupByKey(Repartition("rekeyed"), text, text).count(Named("count"), Store("counts")).toStream(Named("to-stream"))
          .to(Named("write"), topic("out"), text, count)
      ),
      javas: builder =>
        read(builder, "in", "read").filter((_, v) => v.nonEmpty, JavaNamed.as("filter")).mapValues((v: String) => v, JavaNamed.as("map-values"))
          .peek((_, _) => (), JavaNamed.as("peek")).flatMap((k, v) => java.util.List.of(KeyValue.pair(v, k)), JavaNamed.as("flat-map"))
          .selectKey((_, v) => v, JavaNamed.as("select-key")).groupByKey(Grouped.`with`("rekeyed", s, s))
          .count(JavaNamed.as("count"), stored[java.lang.Long]("counts")).toStream(JavaNamed.as("to-stream")).to("out", written("write", l))
    )

  test("records grouped by a new key, reduced, the key-select step named after the repartition"):
    assertEquals(
      ours(
        KStream(Named("read"), topic("in"), text, text).groupBy(Repartition("by-value"), text, text)((_, v) => v)
          .reduce(Named("reduce"), Store("latest"))(_ + _).toStream(Named("to-stream")).to(Named("write"), topic("out"), text, text)
      ),
      javas: builder =>
        read(builder, "in", "read").groupBy((_, v) => v, Grouped.`with`("by-value", s, s))
          .reduce(_ + _, JavaNamed.as("reduce"), stored[String]("latest")).toStream(JavaNamed.as("to-stream")).to("out", written("write", s))
    )

  test("a table read from a topic, its source named after it"):
    assertEquals(
      ours(
        KTable(Named("users"), topic("users"), text, text, Store("users")).toStream(Named("to-stream"))
          .to(Named("write"), topic("out"), text, text.option)
      ),
      javas(builder => table(builder, "users", "users").toStream(JavaNamed.as("to-stream")).to("out", written("write", s)))
    )

  test("a stream joined with a table, the table's nodes in the stream's sub-topology"):
    assertEquals(
      ours(
        KStream(Named("orders"), topic("orders"), text, text)
          .join(Named("with-user"), KTable(Named("users"), topic("users"), text, text, Store("users")))(_ + _)
          .to(Named("write"), topic("out"), text, text)
      ),
      javas: builder =>
        val orders = read(builder, "orders", "orders")
        orders.join(table(builder, "users", "users"), (o: String, u: String) => o + u, Joined.`with`(s, s, s).withName("with-user"))
          .to("out", written("write", s))
    )

  test("a stream joined with a table after its key changed, through a repartition topic named after the join"):
    assertEquals(
      ours(
        KStream(Named("orders"), topic("orders"), text, text).selectKey(Named("by-user"))((_, v) => v)
          .leftJoin(Named("with-user"), KTable(Named("users"), topic("users"), text, text, Store("users")), text, text)((o, u) => o + u.getOrElse(""))
          .to(Named("write"), topic("out"), text, text)
      ),
      javas: builder =>
        val orders = read(builder, "orders", "orders").selectKey((_, v) => v, JavaNamed.as("by-user"))
        orders.leftJoin(
          table(builder, "users", "users"),
          (o: String, u: String) => o + Option(u).getOrElse(""),
          Joined.`with`(s, s, s).withName("with-user")
        ).to("out", written("write", s))
    )

  test("two tables joined, each side reading the other's store, and their join merged"):
    assertEquals(
      ours(
        KTable(Named("users"), topic("users"), text, text, Store("users"))
          .outerJoin(Named("accounts"), KTable(Named("plans"), topic("plans"), text, text, Store("plans")))(_.fold(identity, identity, _ + _))
          .toStream(Named("to-stream")).to(Named("write"), topic("out"), text, text.option)
      ),
      javas: builder =>
        val users = table(builder, "users", "users")
        users.outerJoin(table(builder, "plans", "plans"), (u: String, p: String) => u + p, JavaNamed.as("accounts"))
          .toStream(JavaNamed.as("to-stream")).to("out", written("write", s))
    )

  test("two tables joined into a store, then suppressed into a buffer of its own"):
    assertEquals(
      ours(
        KTable(Named("users"), topic("users"), text, text, Store("users"))
          .join(Named("accounts"), KTable(Named("plans"), topic("plans"), text, text, Store("plans")), Store("accounts"), text)(_ + _)
          .suppress(Suppressed.untilTimeLimit("calm", 1.second)).toStream(Named("to-stream")).to(Named("write"), topic("out"), text, text.option)
      ),
      javas: builder =>
        val users = table(builder, "users", "users")
        users.join(table(builder, "plans", "plans"), (u: String, p: String) => u + p, JavaNamed.as("accounts"), stored[String]("accounts"))
          .suppress(JavaSuppressed.untilTimeLimit(Duration.ofSeconds(1), BufferConfig.unbounded()).withName("calm"))
          .toStream(JavaNamed.as("to-stream")).to("out", written("write", s))
    )

  test("a stream joined with two tables joined without a store, reading both tables' stores"):
    assertEquals(
      ours(
        KStream(Named("orders"), topic("orders"), text, text).join(
          Named("with-account"),
          KTable(Named("users"), topic("users"), text, text, Store("users"))
            .join(Named("accounts"), KTable(Named("plans"), topic("plans"), text, text, Store("plans")))(_ + _)
        )(_ + _).to(Named("write"), topic("out"), text, text)
      ),
      javas: builder =>
        val orders   = read(builder, "orders", "orders")
        val users    = table(builder, "users", "users")
        val accounts = users.join(table(builder, "plans", "plans"), (u: String, p: String) => u + p, JavaNamed.as("accounts"))
        orders.join(accounts, (o: String, a: String) => o + a, Joined.`with`(s, s, s).withName("with-account")).to("out", written("write", s))
    )

  test("a stream joined with a suppressed table, reading the buffer before the table's store"):
    assertEquals(
      ours(
        KStream(Named("orders"), topic("orders"), text, text).join(
          Named("with-user"),
          KTable(Named("users"), topic("users"), text, text, Store("users")).suppress(Suppressed.untilTimeLimit("calm", 1.second))
        )(_ + _).to(Named("write"), topic("out"), text, text)
      ),
      javas: builder =>
        val orders = read(builder, "orders", "orders")
        val users  =
          table(builder, "users", "users").suppress(JavaSuppressed.untilTimeLimit(Duration.ofSeconds(1), BufferConfig.unbounded()).withName("calm"))
        orders.join(users, (o: String, u: String) => o + u, Joined.`with`(s, s, s).withName("with-user")).to("out", written("write", s))
    )

  // `calm-8-store` and `users` share a bucket of Java's set of the join's stores, which then keeps them in the order the table's value getter names them.
  test("a stream joined with a suppressed table whose buffer shares a bucket with the table's store"):
    assertEquals(
      ours(
        KStream(Named("orders"), topic("orders"), text, text).join(
          Named("with-user"),
          KTable(Named("users"), topic("users"), text, text, Store("users")).suppress(Suppressed.untilTimeLimit("calm-8", 1.second))
        )(_ + _).to(Named("write"), topic("out"), text, text)
      ),
      javas: builder =>
        val orders = read(builder, "orders", "orders")
        val users  =
          table(builder, "users", "users").suppress(JavaSuppressed.untilTimeLimit(Duration.ofSeconds(1), BufferConfig.unbounded()).withName("calm-8"))
        orders.join(users, (o: String, u: String) => o + u, Joined.`with`(s, s, s).withName("with-user")).to("out", written("write", s))
    )

  // `pay-14-outer-other-join-store` and `pay-14-outer-shared-join-store` share a bucket of the set of the left side's join stores.
  test("two streams outer joined with stores that share a bucket of Java's set of a join's stores"):
    assertEquals(
      ours(
        KStream(Named("left"), topic("left"), text, text).outerJoin(
          Named("pay"),
          KStream(Named("right"), topic("right"), text, text),
          JoinWindows(5.seconds, grace = 1.second),
          Store("pay-14"),
          text,
          text,
          text
        )(_.fold(identity, identity, _ + _)).to(Named("write"), topic("out"), text, text)
      ),
      javas: builder =>
        val left    = read(builder, "left", "left")
        val windows = JavaJoinWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(5), Duration.ofSeconds(1))
        left.outerJoin(
          read(builder, "right", "right"),
          (a: String, b: String) => a + b,
          windows,
          StreamJoined.`with`(s, s, s).withName("pay").withStoreName("pay-14")
        ).to("out", written("write", s))
    )

  for (kind, joined) <-
      List[
        (String, (KStream.Of[IO, String, String, KeyState.Changed], KStream.Of[IO, String, String, KeyState.AsRead]) => KStream[IO, String, String])
      ](
        "join" ->
          ((left, right) => left.join(Named("pay"), right, JoinWindows(5.seconds, grace = 1.second), Store("payments"), text, text, text)(_ + _)),
        "leftJoin" ->
          ((left, right) =>
            left.leftJoin(Named("pay"), right, JoinWindows(5.seconds, grace = 1.second), Store("payments"), text, text, text)((a, b) =>
              a + b.getOrElse("")
            )
          ),
        "outerJoin" ->
          ((left, right) =>
            left.outerJoin(Named("pay"), right, JoinWindows(5.seconds, grace = 1.second), Store("payments"), text, text, text)(
              _.fold(identity, identity, _ + _)
            )
          )
      )
  do
    test(s"two streams joined by $kind in windows, the left side repartitioned"):
      val javaJoined: (JavaKStream[String, String], JavaKStream[String, String]) => JavaKStream[String, String] =
        val windows = JavaJoinWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(5), Duration.ofSeconds(1))
        val named   = StreamJoined.`with`(s, s, s).withName("pay").withStoreName("payments")
        kind match
          case "join"     => (left, right) => left.join(right, (a: String, b: String) => a + b, windows, named)
          case "leftJoin" => (left, right) => left.leftJoin(right, (a: String, b: String) => a + b, windows, named)
          case _          => (left, right) => left.outerJoin(right, (a: String, b: String) => a + b, windows, named)
      assertEquals(
        ours(
          joined(
            KStream(Named("left"), topic("left"), text, text).selectKey(Named("by-value"))((_, v) => v),
            KStream(Named("right"), topic("right"), text, text)
          ).to(Named("write"), topic("out"), text, text)
        ),
        javas: builder =>
          val left = read(builder, "left", "left").selectKey((_, v) => v, JavaNamed.as("by-value"))
          javaJoined(left, read(builder, "right", "right")).to("out", written("write", s))
      )

  test("windows of each kind, each in its own pipeline, one emitting on close and one suppressed"):
    val windowed = Serde.timeWindowed(text, 1.minute)
    assertEquals(
      ours(
        KStream(Named("read-1"), topic("in-1"), text, text).groupByKey(text, text).windowedBy(TimeWindows.tumbling(1.minute, grace = 10.seconds))
          .emitOnWindowClose.count(Named("count-tumbling"), Store("tumbling")).toStream(Named("to-stream-1"))
          .to(Named("write-1"), topic("out-1"), windowed, count),
        KStream(Named("read-2"), topic("in-2"), text, text).groupByKey(text, text).windowedBy(SlidingWindows(5.minutes, grace = 30.seconds))
          .count(Named("count-sliding"), Store("sliding")).suppress(Suppressed.untilWindowCloses("final")).toStream(Named("to-stream-2"))
          .to(Named("write-2"), topic("out-2"), Serde.timeWindowed(text, 5.minutes), count),
        KStream(Named("read-3"), topic("in-3"), text, text).groupByKey(text, text).windowedBy(SessionWindows(5.minutes, grace = 1.minute))
          .count(Named("count-session"), Store("sessions")).toStream(Named("to-stream-3"))
          .to(Named("write-3"), topic("out-3"), Serde.sessionWindowed(text), count.option)
      ),
      javas: builder =>
        def produced = Produced.`with`[JavaWindowed[String], java.lang.Long](null, l)
        read(builder, "in-1", "read-1").groupByKey(Grouped.`with`(s, s))
          .windowedBy(JavaTimeWindows.ofSizeAndGrace(Duration.ofMinutes(1), Duration.ofSeconds(10))).emitStrategy(EmitStrategy.onWindowClose())
          .count(JavaNamed.as("count-tumbling"), Materialized.as("tumbling")).toStream(JavaNamed.as("to-stream-1"))
          .to("out-1", produced.withName("write-1"))
        read(builder, "in-2", "read-2").groupByKey(Grouped.`with`(s, s))
          .windowedBy(JavaSlidingWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(5), Duration.ofSeconds(30)))
          .count(JavaNamed.as("count-sliding"), Materialized.as("sliding"))
          .suppress(JavaSuppressed.untilWindowCloses(BufferConfig.unbounded()).withName("final")).toStream(JavaNamed.as("to-stream-2"))
          .to("out-2", produced.withName("write-2"))
        read(builder, "in-3", "read-3").groupByKey(Grouped.`with`(s, s))
          .windowedBy(JavaSessionWindows.ofInactivityGapAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(1)))
          .count(JavaNamed.as("count-session"), Materialized.as("sessions")).toStream(JavaNamed.as("to-stream-3"))
          .to("out-3", produced.withName("write-3"))
    )

  test("a table counted after a repartition, joined with a table read from a topic, numbered receiver first"):
    assertEquals(
      ours(
        KTable(Named("users"), topic("users"), text, text, Store("users")).leftJoin(
          Named("visited"),
          KStream(Named("visits"), topic("visits"), text, text).groupBy(Repartition("by-user"), text, text)((_, v) => v)
            .count(Named("count"), Store("visits"))
        )((u, n) => s"$u${n.getOrElse(0L)}").toStream(Named("to-stream")).to(Named("write"), topic("out"), text, text.option)
      ),
      javas: builder =>
        val users  = table(builder, "users", "users")
        val visits =
          read(builder, "visits", "visits").groupBy((_, v) => v, Grouped.`with`("by-user", s, s))
            .count(JavaNamed.as("count"), stored[java.lang.Long]("visits"))
        users.leftJoin(visits, (u: String, n: java.lang.Long) => s"$u$n", JavaNamed.as("visited")).toStream(JavaNamed.as("to-stream"))
          .to("out", written("write", s))
    )
