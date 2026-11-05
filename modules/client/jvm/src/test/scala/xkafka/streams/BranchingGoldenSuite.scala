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
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.{
  Branched, Consumed, Grouped, Joined, JoinWindows as JavaJoinWindows, KStream as JavaKStream, KTable as JavaKTable, Materialized, Named as JavaNamed,
  Produced, Repartitioned, StreamJoined
}
import org.apache.kafka.streams.state.KeyValueStore
import scodec.bits.ByteVector
import internal.protocol.{StreamsGroup, StreamsGroupTopologyDescriptionUpdateRequest}
import internal.streams.Declaration

/** Programs that branch, against the same programs in Java, every step named alike and built in the same order: the text of `describe`, the
  * description a member pushes, and the topology it declares.
  */
final class BranchingGoldenSuite extends FunSuite:
  private val text  = Serde.utf8[IO]
  private val count = Serde.long[IO]
  private val s     = Serdes.String
  private val l     = Serdes.Long

  private def topic(name: String) = Topic.from(name).orFail

  private final case class Declared(text: String, pushed: ByteVector, topology: StreamsGroup.Topology)

  private def ours(first: Pipeline[IO], rest: Pipeline[IO]*): Declared =
    val topology = Topology.of(first, rest*).orFail
    Declared(
      topology.describe.toString,
      StreamsGroupTopologyDescriptionUpdateRequest("app", "member", 3, Declaration.described("app", topology)).body,
      Declaration("app", topology)
    )

  private def javas(build: StreamsBuilder => Unit): Declared =
    val (topology, data) = JavaStreams.prepared("app")(build)
    val request          =
      new StreamsGroupTopologyDescriptionUpdateRequestData().setGroupId("app").setMemberId("member").setTopologyEpoch(3)
        .setTopologyDescription(data.wireTopologyDescription())
    Declared(
      topology.describe().toString,
      ByteVector(MessageUtil.toByteBufferAccessor(request, 0.toShort).buffer()),
      TopologyGoldenSuite.declaredByJava(build)
    )

  private def read(name: String) = KStream(Named(name), topic(name), text, text)

  private def users(name: String = "users") = KTable(Named(name), topic(name), text, text, Store(name))

  private def stored[V](name: String) = Materialized.as[String, V, KeyValueStore[Bytes, Array[Byte]]](name)

  private def javaRead(builder: StreamsBuilder, name: String): JavaKStream[String, String] =
    builder.stream[String, String](name, Consumed.`with`(s, s).withName(name))

  private def javaTable(builder: StreamsBuilder, name: String = "users"): JavaKTable[String, String] =
    builder.table[String, String](name, Consumed.`with`(s, s).withName(name), stored[String](name).withKeySerde(s).withValueSerde(s))

  private def written[V](name: String, value: org.apache.kafka.common.serialization.Serde[V] = s) = Produced.`with`(s, value).withName(name)

  private def tableJoined(name: String) = Joined.`with`(s, s, s).withName(name)

  private val within     = JoinWindows(5.seconds, grace = 1.second)
  private val javaWithin = JavaJoinWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(5), Duration.ofSeconds(1))

  private def pair(name: String, store: String = "payments") = StreamJoined.`with`(s, s, s).withName(name).withStoreName(store)

  test("a stream that forks, each path in turn, whichever is the larger"):
    assertEquals(
      ours(read("in").filter(Named("big"))((_, v) => v != "small").fork(
        _.to(Named("all"), topic("all"), text, text),
        _.mapValues(Named("audit"))(v => s"audit:$v").to(Named("audited"), topic("audited"), text, text)
      )),
      javas: builder =>
        val big = javaRead(builder, "in").filter((_, v) => v != "small", JavaNamed.as("big"))
        big.to("all", written("all"))
        big.mapValues((v: String) => s"audit:$v", JavaNamed.as("audit")).to("audited", written("audited"))
    )

  test("a node with many successors lists them as Java's tree of them does, each compared as it was when it was added"):
    val names = List("delta", "alpha", "golf", "charlie", "foxtrot", "bravo", "echo", "hotel")
    def path(name: String)(from: KStream.Flowing[IO, String, String, KeyState.AsRead, Into[IO, String, String, KeyState.AsRead]]) =
      // The paths differ in length, so that the sizes of their subtrees order them, and they grow as they are connected.
      name.length match
        case 4 => from.to(Named(s"w-$name"), topic(name), text, text)
        case 5 => from.filter(Named(s"f-$name"))((_, _) => true).to(Named(s"w-$name"), topic(name), text, text)
        case _ => from.filter(Named(s"f-$name"))((_, _) => true).mapValues(Named(s"m-$name"))(identity).to(Named(s"w-$name"), topic(name), text, text)
    assertEquals(
      ours(read("in").fork(path(names.head), names.tail.map(path)*)),
      javas: builder =>
        val in = javaRead(builder, "in")
        names.foreach: name =>
          name.length match
            case 4 => in.to(name, written(s"w-$name"))
            case 5 => in.filter((_, _) => true, JavaNamed.as(s"f-$name")).to(name, written(s"w-$name"))
            case _ => in.filter((_, _) => true, JavaNamed.as(s"f-$name")).mapValues((v: String) => v, JavaNamed.as(s"m-$name"))
                .to(name, written(s"w-$name"))
    )

  test("two streams merged, in one sub-topology, in the order the program builds them"):
    assertEquals(
      ours(read("orders").merge(Named("all"), read("refunds")).to(Named("write"), topic("out"), text, text)),
      javas(builder => javaRead(builder, "orders").merge(javaRead(builder, "refunds"), JavaNamed.as("all")).to("out", written("write")))
    )
    assertEquals(
      ours(read("refunds").merge(Named("all"), read("orders")).to(Named("write"), topic("out"), text, text)),
      javas: builder =>
        val refunds = javaRead(builder, "refunds")
        refunds.merge(javaRead(builder, "orders"), JavaNamed.as("all")).to("out", written("write"))
    )

  test("a stream that forks and merges back, the merge reading both paths"):
    assertEquals(
      ours(
        read("in").forkMerge(Named("back"))(_.filter(Named("a"))((_, _) => true), _.mapValues(Named("b"))(_.toUpperCase))
          .to(Named("write"), topic("out"), text, text)
      ),
      javas: builder =>
        val in = javaRead(builder, "in")
        in.filter((_, _) => true, JavaNamed.as("a")).merge(in.mapValues((v: String) => v.toUpperCase, JavaNamed.as("b")), JavaNamed.as("back"))
          .to("out", written("write"))
    )

  test("a merge one of whose sides changed its key goes through a repartition topic to be grouped"):
    assertEquals(
      ours(
        read("in").forkMerge(Named("back"))(_.filter(Named("a"))((_, _) => true), _.selectKey(Named("b"))((_, v) => v))
          .groupByKey(Repartition("rekeyed"), text, text).count(Named("counted"), Store("counts")).toStream(Named("changes"))
          .to(Named("write"), topic("out"), text, count)
      ),
      javas: builder =>
        val in = javaRead(builder, "in")
        in.filter((_, _) => true, JavaNamed.as("a")).merge(in.selectKey((_, v) => v, JavaNamed.as("b")), JavaNamed.as("back"))
          .groupByKey(Grouped.`with`("rekeyed", s, s))
          .count(JavaNamed.as("counted"), stored[java.lang.Long]("counts").withKeySerde(s).withValueSerde(l)).toStream(JavaNamed.as("changes"))
          .to("out", written("write", l))
    )

  test("a stream merged with itself reads its records once, as Java connects a node to a successor once"):
    assertEquals(
      ours(read("in").forkMerge(Named("back"))(identity, identity).to(Named("write"), topic("out"), text, text)),
      javas: builder =>
        val in = javaRead(builder, "in")
        in.merge(in, JavaNamed.as("back")).to("out", written("write"))
    )

  test("a merge reading a repartitioned side comes where its last parent does, whichever side receives it"):
    assertEquals(
      ours(
        read("a").selectKey(Named("sk"))((_, v) => v).repartition(Repartition("r"), text, text).merge(Named("m"), read("b"))
          .to(Named("write"), topic("out"), text, text)
      ),
      javas: builder =>
        val a = javaRead(builder, "a").selectKey((_, v) => v, JavaNamed.as("sk")).repartition(Repartitioned.`with`(s, s).withName("r"))
        a.merge(javaRead(builder, "b"), JavaNamed.as("m")).to("out", written("write"))
    )
    assertEquals(
      ours(
        read("b").merge(Named("m"), read("a").selectKey(Named("sk"))((_, v) => v).repartition(Repartition("r"), text, text))
          .to(Named("write"), topic("out"), text, text)
      ),
      javas: builder =>
        val b = javaRead(builder, "b")
        val a = javaRead(builder, "a").selectKey((_, v) => v, JavaNamed.as("sk")).repartition(Repartitioned.`with`(s, s).withName("r"))
        b.merge(a, JavaNamed.as("m")).to("out", written("write"))
    )

  test("a path that goes to two merges, each merge coming when its last parent does"):
    val orders = read("orders")
    val x      = orders.flow.merged(Named("x")).to(Named("w-x"), topic("x"), text, text)
    val y      = orders.flow.merged(Named("y")).to(Named("w-y"), topic("y"), text, text)
    val a      = orders.flow.filter(Named("a"))((_, _) => true).into(x, y)
    val b      = orders.flow.filter(Named("b"))((_, _) => true).into(x)
    val c      = orders.flow.filter(Named("c"))((_, _) => true).into(y)
    assertEquals(
      ours(orders.into(a, b, c)),
      javas: builder =>
        val in = javaRead(builder, "orders")
        val a  = in.filter((_, _) => true, JavaNamed.as("a"))
        val b  = in.filter((_, _) => true, JavaNamed.as("b"))
        val c  = in.filter((_, _) => true, JavaNamed.as("c"))
        a.merge(b, JavaNamed.as("x")).to("x", written("w-x"))
        a.merge(c, JavaNamed.as("y")).to("y", written("w-y"))
    )

  test("a stream enriched with its own count, the table made before the join reads it"):
    val clicks                            = read("clicks")
    val counts                            = clicks.flow.groupByKey(text, text).count(Named("counted"), Store("counts"))
    def expected(builder: StreamsBuilder) =
      val in     = javaRead(builder, "clicks")
      val counts =
        in.groupByKey(Grouped.`with`(s, s)).count(JavaNamed.as("counted"), stored[java.lang.Long]("counts").withKeySerde(s).withValueSerde(l))
      in.join(counts, (v: String, n: java.lang.Long) => s"$v#$n", Joined.`with`(s, s, l).withName("enrich")).to("enriched", written("write"))
    val enrich = clicks.flow.join(Named("enrich"), counts)((v, n) => s"$v#$n").to(Named("write"), topic("enriched"), text, text)
    assertEquals(ours(clicks.into(counts.consumer, enrich)), javas(expected))
    // Sent to the join first, the walk still comes to the join after the table it reads.
    assertEquals(ours(clicks.into(enrich, counts.consumer)), javas(expected))

  for (kind, makeOurs, makeJava) <-
      List[
        (
            String,
            (
                KStream.Flowing[IO, String, String, KeyState.AsRead, Into[IO, String, String, KeyState.AsRead]],
                KStream.Flowing[IO, String, String, KeyState.AsRead, Into[IO, String, String, KeyState.AsRead]]
            ) => KStream.Flowing[IO, String, String, KeyState.AsRead, Into[IO, String, String, KeyState.AsRead]],
            (JavaKStream[String, String], JavaKStream[String, String]) => JavaKStream[String, String]
        )
      ](
        (
          "join",
          (x, y) => x.join(Named("pay"), y, within, Store("payments"), text, text, text)(_ + _),
          (x, y) => x.join(y, (a: String, b: String) => a + b, javaWithin, pair("pay"))
        ),
        (
          "leftJoin",
          (x, y) => x.leftJoin(Named("pay"), y, within, Store("payments"), text, text, text)((a, b) => a + b.getOrElse("")),
          (x, y) => x.leftJoin(y, (a: String, b: String) => a + b, javaWithin, pair("pay"))
        ),
        (
          "outerJoin",
          (x, y) => x.outerJoin(Named("pay"), y, within, Store("payments"), text, text, text)(_.fold(identity, identity, _ + _)),
          (x, y) => x.outerJoin(y, (a: String, b: String) => a + b, javaWithin, pair("pay"))
        )
      )
  do
    test(s"a stream $kind with itself, both sides reading its one source"):
      val in = read("in")
      assertEquals(
        ours(in.into(makeOurs(in.flow, in.flow).to(Named("write"), topic("out"), text, text))),
        javas: builder =>
          val in = javaRead(builder, "in")
          makeJava(in, in).to("out", written("write"))
      )

  test("a stream whose key changed joined with itself, each side through a repartition topic of its own"):
    val rekeyed = read("in").selectKey(Named("sk"))((_, v) => v)
    assertEquals(
      ours(rekeyed.into(
        rekeyed.flow.join(Named("pay"), rekeyed.flow, within, Store("payments"), text, text, text)(_ + _).to(Named("write"), topic("out"), text, text)
      )),
      javas: builder =>
        val rekeyed = javaRead(builder, "in").selectKey((_, v) => v, JavaNamed.as("sk"))
        rekeyed.join(rekeyed, (a: String, b: String) => a + b, javaWithin, pair("pay")).to("out", written("write"))
    )

  test("a table joined with itself, each side reading the one store"):
    val table = users()
    assertEquals(
      ours(
        table.into(table.flow.join(Named("self"), table.flow)(_ + _).toStream(Named("changes")).to(Named("write"), topic("out"), text, text.option))
      ),
      javas: builder =>
        val table = javaTable(builder)
        table.join(table, (a: String, b: String) => a + b, JavaNamed.as("self")).toStream(JavaNamed.as("changes")).to("out", written("write"))
    )

  test("one table two pipelines read, all in one sub-topology, whose topics Java groups as one"):
    val table = users()
    assertEquals(
      ours(
        read("a").join(Named("a-joined"), table)(_ + _).to(Named("w-a"), topic("out-a"), text, text),
        read("b").join(Named("b-joined"), table)(_ + _).to(Named("w-b"), topic("out-b"), text, text)
      ),
      javas: builder =>
        val a     = javaRead(builder, "a")
        val table = javaTable(builder)
        a.join(table, (x: String, y: String) => x + y, tableJoined("a-joined")).to("out-a", written("w-a"))
        javaRead(builder, "b").join(table, (x: String, y: String) => x + y, tableJoined("b-joined")).to("out-b", written("w-b"))
    )

  test("a stream that forks to join two tables, whose overlapping topic groups Java merges"):
    val first  = users("first")
    val second = users("second")
    assertEquals(
      ours(read("in").fork(
        _.join(Named("with-first"), first)(_ + _).to(Named("w-first"), topic("out-first"), text, text),
        _.join(Named("with-second"), second)(_ + _).to(Named("w-second"), topic("out-second"), text, text)
      )),
      javas: builder =>
        val in     = javaRead(builder, "in")
        val first  = javaTable(builder, "first")
        val second = javaTable(builder, "second")
        in.join(first, (x: String, y: String) => x + y, tableJoined("with-first")).to("out-first", written("w-first"))
        in.join(second, (x: String, y: String) => x + y, tableJoined("with-second")).to("out-second", written("w-second"))
    )

  test("a table whose changes are written and which a join reads, whichever comes first in the program"):
    val table     = users()
    def pipelines =
      List(
        table.toStream(Named("changes")).to(Named("w-changes"), topic("changes"), text, text.option),
        read("in").join(Named("joined"), table)(_ + _).to(Named("w-joined"), topic("out"), text, text)
      )
    assertEquals(
      ours(pipelines.head, pipelines.last),
      javas: builder =>
        val table = javaTable(builder)
        table.toStream(JavaNamed.as("changes")).to("changes", written("w-changes"))
        javaRead(builder, "in").join(table, (x: String, y: String) => x + y, tableJoined("joined")).to("out", written("w-joined"))
    )
    assertEquals(
      ours(pipelines.last, pipelines.head),
      javas: builder =>
        val in    = javaRead(builder, "in")
        val table = javaTable(builder)
        in.join(table, (x: String, y: String) => x + y, tableJoined("joined")).to("out", written("w-joined"))
        table.toStream(JavaNamed.as("changes")).to("changes", written("w-changes"))
    )

  test("a table that forks, its filter and mapValues each read by a join of its own"):
    val table    = users()
    val filtered = table.flow.filter(Named("filtered"))((_, v) => v.nonEmpty)
    val mapped   = table.flow.mapValues(Named("mapped"))(_.toUpperCase)
    assertEquals(
      ours(
        table.into(filtered.consumer, mapped.consumer),
        read("a").join(Named("a-joined"), filtered)(_ + _).to(Named("w-a"), topic("out-a"), text, text),
        read("b").join(Named("b-joined"), mapped)(_ + _).to(Named("w-b"), topic("out-b"), text, text)
      ),
      javas: builder =>
        val table    = javaTable(builder)
        val filtered = table.filter((_, v) => v.nonEmpty, JavaNamed.as("filtered"))
        val mapped   = table.mapValues((v: String) => v.toUpperCase, JavaNamed.as("mapped"))
        javaRead(builder, "a").join(filtered, (x: String, y: String) => x + y, tableJoined("a-joined")).to("out-a", written("w-a"))
        javaRead(builder, "b").join(mapped, (x: String, y: String) => x + y, tableJoined("b-joined")).to("out-b", written("w-b"))
    )

  test("a stream whose paths each go through a repartition topic, numbered in the order they come"):
    assertEquals(
      ours(read("in").fork(
        _.groupBy(Repartition("r2"), text, text)((_, v) => v).count(Named("c2"), Store("s2")).toStream(Named("t2"))
          .to(Named("w2"), topic("o2"), text, count),
        _.groupBy(Repartition("r1"), text, text)((_, v) => v).count(Named("c1"), Store("s1")).toStream(Named("t1"))
          .to(Named("w1"), topic("o1"), text, count)
      )),
      javas: builder =>
        val in                 = javaRead(builder, "in")
        def counted(n: String) =
          in.groupBy((_, v) => v, Grouped.`with`(s"r$n", s, s).withName(s"r$n"))
            .count(JavaNamed.as(s"c$n"), stored[java.lang.Long](s"s$n").withKeySerde(s).withValueSerde(l)).toStream(JavaNamed.as(s"t$n"))
            .to(s"o$n", written(s"w$n", l))
        counted("2")
        counted("1")
    )

  test("what a repartition topic's records go on to, down several paths"):
    assertEquals(
      ours(read("in").selectKey(Named("sk"))((_, v) => v).repartition(Repartition("r"), text, text).fork(
        _.to(Named("wa"), topic("a"), text, text),
        _.groupByKey(text, text).count(Named("counted"), Store("counts")).toStream(Named("changes")).to(Named("wb"), topic("b"), text, count)
      )),
      javas: builder =>
        val repartitioned = javaRead(builder, "in").selectKey((_, v) => v, JavaNamed.as("sk")).repartition(Repartitioned.`with`(s, s).withName("r"))
        repartitioned.to("a", written("wa"))
        repartitioned.groupByKey(Grouped.`with`(s, s))
          .count(JavaNamed.as("counted"), stored[java.lang.Long]("counts").withKeySerde(s).withValueSerde(l)).toStream(JavaNamed.as("changes"))
          .to("b", written("wb", l))
    )

  test("a path that changes its key to join a table beside one that goes straight on, the join's repartition source made at the join"):
    val table = users()
    assertEquals(
      ours(read("in").fork(
        _.selectKey(Named("sk"))((_, v) => v).join(Named("joined"), table, text, text)(_ + _).to(Named("w-joined"), topic("out"), text, text),
        _.to(Named("w-all"), topic("all"), text, text)
      )),
      javas: builder =>
        val in    = javaRead(builder, "in")
        val table = javaTable(builder)
        in.selectKey((_, v) => v, JavaNamed.as("sk")).join(table, (x: String, y: String) => x + y, tableJoined("joined"))
          .to("out", written("w-joined"))
        in.to("all", written("w-all"))
    )

  test("a split with a default branch, each branch a node of the split's name and its own"):
    assertEquals(
      ours(
        read("in").split(Named("route-")).branch(Named("big"))((_, v) => v.length > 3)(_.to(Named("w-big"), topic("big"), text, text))
          .branch(Named("mid"))((_, v) => v.length > 1)(_.mapValues(Named("m"))(_.toUpperCase).to(Named("w-mid"), topic("mid"), text, text))
          .defaultBranch(Named("rest"))(_.to(Named("w-rest"), topic("rest"), text, text))
      ),
      javas: builder =>
        val _ =
          javaRead(builder, "in").split(JavaNamed.as("route-"))
            .branch((_, v) => v.length > 3, Branched.withConsumer((b: JavaKStream[String, String]) => b.to("big", written("w-big")), "big")).branch(
              (_, v) => v.length > 1,
              Branched.withConsumer(
                (b: JavaKStream[String, String]) => b.mapValues((v: String) => v.toUpperCase, JavaNamed.as("m")).to("mid", written("w-mid")),
                "mid"
              )
            ).defaultBranch(Branched.withConsumer((b: JavaKStream[String, String]) => b.to("rest", written("w-rest")), "rest"))
    )

  test("a split with no default branch, whose branches merge back"):
    val back = Flow.asRead[IO, String, String].merged(Named("back")).to(Named("write"), topic("out"), text, text)
    assertEquals(
      ours(
        read("in").split(Named("route-")).branch(Named("a"))((_, v) => v.startsWith("a"))(_.into(back))
          .branch(Named("b"))((_, v) => v.startsWith("b"))(_.into(back)).noDefaultBranch
      ),
      javas: builder =>
        val branches =
          javaRead(builder, "in").split(JavaNamed.as("route-")).branch((_, v) => v.startsWith("a"), Branched.as("a"))
            .branch((_, v) => v.startsWith("b"), Branched.as("b")).noDefaultBranch()
        branches.get("route-a").merge(branches.get("route-b"), JavaNamed.as("back")).to("out", written("write"))
    )

  test("a merge of a stream and a repartition of another, as Java numbers it, says it depends on what the program builds first"):
    val topology =
      Topology.of(
        read("b").merge(Named("m"), read("a").selectKey(Named("sk"))((_, v) => v).repartition(Repartition("r"), text, text))
          .to(Named("write"), topic("out"), text, text)
      ).orFail
    assertEquals(topology.subtopologies.flatMap(_.buildOrdered), List(List(Source.Input(topic("b")), Source.Repartitioned(Repartition("r")))))
