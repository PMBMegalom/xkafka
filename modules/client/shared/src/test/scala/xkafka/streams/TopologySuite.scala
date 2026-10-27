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

import cats.effect.IO
import fs2.Chunk
import munit.CatsEffectSuite
import internal.protocol.StreamsGroup
import internal.streams.Declaration

/** How a streams program is checked when it is built, and what it declares to the coordinator, on every platform. */
final class TopologySuite extends CatsEffectSuite:
  private def topic(name: String): Topic = Topic.from(name).orFail

  private val text  = Serde.utf8[IO]
  private val count = Serde.long[IO]

  private def counting(input: String, repartition: String, store: String): Pipeline[IO] =
    KStream(topic(input), text, text).groupBy(Repartition(repartition), text, text)((_, v) => v).count(Store(store)).toStream
      .to(topic(s"$input-out"), text, count)

  test("a grouped program declares its input, its repartition topic and its changelog, with the configs Java gives them"):
    val declared = Topology.of(counting("in", "by-value", "counts")).map(Declaration("app", _))
    assertEquals(
      declared,
      Right(StreamsGroup.Topology(
        0,
        List(
          StreamsGroup.Subtopology("0", List("in"), Nil, Nil, List("app-by-value-repartition"), Nil, Nil),
          StreamsGroup.Subtopology(
            "1",
            Nil,
            Nil,
            List(
              StreamsGroup.TopicInfo("app-counts-changelog", 0, -1, List("cleanup.policy" -> "compact", "message.timestamp.type" -> "CreateTime"))
            ),
            Nil,
            List(StreamsGroup.TopicInfo(
              "app-by-value-repartition",
              0,
              -1,
              List("cleanup.policy" -> "delete", "message.timestamp.type" -> "CreateTime", "retention.ms" -> "-1", "segment.bytes" -> "52428800")
            )),
            Nil
          )
        )
      ))
    )

  test("sub-topologies are listed by their ids as text, as Java sorts them, so the eleventh comes before the third"):
    val pipelines = (0 until 11).toList.map(n => KStream(topic(s"in-$n"), text, text).to(topic(s"out-$n"), text, text))
    val declared  = Topology.of(pipelines.head, pipelines.tail*).map(Declaration("app", _).subtopologies.map(s => (s.id, s.sourceTopics)))
    assertEquals(declared.map(_.take(3)), Right(List(("0", List("in-0")), ("1", List("in-1")), ("10", List("in-10")))))

  test("the replication factor declared for internal topics is the one asked for"):
    val declared = Topology.of(counting("in", "by-value", "counts")).map(Declaration("app", _, replicationFactor = 3))
    assertEquals(
      declared.map(_.subtopologies.flatMap(s => s.stateChangelogTopics ++ s.repartitionSourceTopics).map(_.replicationFactor)),
      Right(List[Short](3, 3))
    )

  test("a program is refused with every reason at once: names reused, a topic read twice, and names no topic can carry"):
    val refused = Topology.of(counting("in", "same", "counts"), counting("in", "same", "counts"), counting("other", "bad name", ""))
    assertEquals(
      refused.left.map(_.toList.toSet),
      Left(Set(
        TopologyError.InvalidName("bad name", "it may contain only [a-zA-Z0-9._-]"),
        TopologyError.InvalidName("", "it is empty"),
        TopologyError.DuplicateStore("counts"),
        TopologyError.DuplicateRepartition("same"),
        TopologyError.DuplicateSource(topic("in"))
      ))
    )

  test("a name longer than Java allows is refused"):
    val long = "n" * (Topology.MaxNameLength + 1)
    assertEquals(
      Topology.of(counting("in", "fine", long)).left.map(_.toList),
      Left(List(TopologyError.InvalidName(long, s"it is longer than ${Topology.MaxNameLength} characters")))
    )
    assert(Topology.of(counting("in", "fine", "n" * Topology.MaxNameLength)).isRight)

  test("a name used twice that no topic can carry is reported once as invalid and once as reused"):
    val twice =
      KStream(topic("in"), text, text).groupByKey(text, text).count(Store("bad name")).toStream.groupByKey(text, count)
        .reduce(Store("bad name"))((_, n) => n)
    assertEquals(
      Topology.of(twice.toStream.to(topic("out"), text, count)).left.map(_.toList),
      Left(List(TopologyError.InvalidName("bad name", "it may contain only [a-zA-Z0-9._-]"), TopologyError.DuplicateStore("bad name")))
    )

  test("records whose key changed cannot be grouped by key without a repartition name, and can with one"):
    val rekeyed = KStream(topic("in"), text, text).selectKey((_, v) => v)
    assertEquals(
      Topology.of(rekeyed.groupByKey(text, text).count(Store("counts")).toStream.to(topic("out"), text, count)).left.map(_.toList),
      Left(List(TopologyError.UnnamedRepartition))
    )
    assert(Topology.of(rekeyed.groupByKey(Repartition("rekeyed"), text, text).count(Store("counts")).toStream.to(topic("out"), text, count)).isRight)

  test("records grouped by the key they were read with need no repartition name"):
    val grouped = KStream(topic("in"), text, text).filter((_, v) => v.nonEmpty).mapValues(_.trim).groupByKey(text, text).count(Store("counts"))
    assert(Topology.of(grouped.toStream.to(topic("out"), text, count)).isRight)

  test("a table's changes start with the key they were grouped by, so grouping them by key again needs no repartition"):
    val again =
      KStream(topic("in"), text, text).groupBy(Repartition("by-value"), text, text)((_, v) => v).count(Store("counts")).toStream
        .groupByKey(text, count).reduce(Store("latest"))((_, n) => n)
    assertEquals(Topology.of(again.toStream.to(topic("out"), text, count)).map(Declaration("app", _).subtopologies.size), Right(2))

  test("a long is written as eight big-endian bytes and read back, and other lengths or no value are refused"):
    val headers = Headers.empty
    for
      written <- count.serializer.serialize(topic("t"), headers, 258L)
      read    <- count.deserializer.deserialize(topic("t"), headers, written)
      short   <- count.deserializer.deserialize(topic("t"), headers, Some(Chunk[Byte](1, 2))).attempt
      missing <- count.deserializer.deserialize(topic("t"), headers, None).attempt
    yield
      assertEquals(written, Some(Chunk[Byte](0, 0, 0, 0, 0, 0, 1, 2)))
      assertEquals(read, 258L)
      assert(short.isLeft)
      assert(missing.left.exists(_.isInstanceOf[Deserializer.NullValueException]))
