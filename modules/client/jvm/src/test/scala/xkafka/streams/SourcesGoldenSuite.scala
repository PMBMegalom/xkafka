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

import scala.jdk.CollectionConverters.*

import cats.data.NonEmptyList
import cats.effect.IO
import munit.FunSuite
import org.apache.kafka.common.message.StreamsGroupTopologyDescriptionUpdateRequestData
import org.apache.kafka.common.protocol.MessageUtil
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.{Consumed, Grouped, Joined, KStream as JavaKStream, Materialized, Named as JavaNamed, Produced}
import org.apache.kafka.streams.state.KeyValueStore
import scodec.bits.ByteVector
import internal.protocol.{StreamsGroup, StreamsGroupTopologyDescriptionUpdateRequest}
import internal.streams.Declaration

/** Programs that read several topics as one stream, against the same programs in Java: the text of `describe`, the description a member pushes, and
  * the topology it declares.
  */
final class SourcesGoldenSuite extends FunSuite:
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

  private def several(names: List[String]) = KStream(Named("read"), NonEmptyList.fromListUnsafe(names.map(topic)), text, text)

  private def javaSeveral(builder: StreamsBuilder, names: List[String]): JavaKStream[String, String] =
    builder.stream[String, String](names.asJava, Consumed.`with`(s, s).withName("read"))

  private def written[V](name: String, value: org.apache.kafka.common.serialization.Serde[V] = s) = Produced.`with`(s, value).withName(name)

  private def stored[V](name: String) = Materialized.as[String, V, KeyValueStore[Bytes, Array[Byte]]](name)

  test("several topics are one source, listed as Java's set of them lists them, whatever the order they are given in"):
    val names = List("orders", "payments", "returns", "a", "b", "z", "in-1", "in-2")
    List(names, names.reverse).foreach: order =>
      assertEquals(ours(several(order).to(Named("write"), topic("out"), text, text)), javas(javaSeveral(_, order).to("out", written("write"))))

  test("any number of topics, in the order Java's set of them holds them"):
    // A Java before 19 sizes a copied set of exactly 12 or 24 for more than adding them would reach, so it lists those otherwise.
    (1 to 30).filter(size => Runtime.version.feature >= 19 || !Set(12, 24)(size)).foreach: size =>
      val names = (1 to size).toList.map(n => s"topic-${n * 7}")
      assertEquals(ours(several(names).to(Named("write"), topic("out"), text, text)), javas(javaSeveral(_, names).to("out", written("write"))))

  test("several topics joined with a table are all copartitioned with it"):
    val table = KTable(Named("users"), topic("users"), text, text, Store("users"))
    assertEquals(
      ours(several(List("one", "two", "three")).join(Named("joined"), table)((v, u) => s"$v$u").to(Named("write"), topic("out"), text, text)),
      javas: builder =>
        val users =
          builder.table[String, String]("users", Consumed.`with`(s, s).withName("users"), stored[String]("users").withKeySerde(s).withValueSerde(s))
        javaSeveral(builder, List("one", "two", "three")).join(users, (v: String, u: String) => s"$v$u", Joined.`with`(s, s, s).withName("joined"))
          .to("out", written("write"))
    )

  test("several topics numbered where their source is built, their records grouped through a repartition topic"):
    assertEquals(
      ours(
        KStream(Named("first"), topic("first"), text, text).to(Named("write-first"), topic("out-first"), text, text),
        several(List("one", "two")).selectKey(Named("key"))((_, v) => v).groupByKey(Repartition("by-value"), text, text)
          .count(Named("counted"), Store("counts")).toStream(Named("changes")).to(Named("write"), topic("out"), text, count)
      ),
      javas: builder =>
        builder.stream[String, String]("first", Consumed.`with`(s, s).withName("first")).to("out-first", written("write-first"))
        javaSeveral(builder, List("one", "two")).selectKey((_, v) => v, JavaNamed.as("key")).groupByKey(Grouped.`with`(s, s).withName("by-value"))
          .count(JavaNamed.as("counted"), stored[java.lang.Long]("counts").withKeySerde(s).withValueSerde(l)).toStream(JavaNamed.as("changes"))
          .to("out", written[java.lang.Long]("write", l))
    )

  test("a topic listed twice is read once"):
    assertEquals(
      ours(several(List("in", "more", "in")).to(Named("write"), topic("out"), text, text)),
      javas(javaSeveral(_, List("in", "more", "in")).to("out", written("write")))
    )
