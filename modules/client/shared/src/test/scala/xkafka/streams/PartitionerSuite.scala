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
import xkafka.internal.streams.{Declaration, Task, TaskInput, TaskOutput, TaskState}

/** Where a partitioner is given, what a task writes says which partition it chose, and what `Topology.of` refuses and the application declares. */
final class PartitionerSuite extends CatsEffectSuite:
  private val text  = Serde.utf8[IO]
  private val in    = Topic.from("in").orFail
  private val out   = Topic.from("out").orFail
  private val users = Topic.from("users").orFail

  private def bytes(value: String): Chunk[Byte] = Chunk.array(value.getBytes)

  private def input(topic: String, key: String, value: String): TaskInput =
    TaskInput(Some(bytes(key)), Some(bytes(value)), 1000L, Headers.empty, topic, 0, 0L)

  private val byLength = Partitioner.byKey((key: String, count: Int) => key.length % count)

  private def written(topology: Topology[IO], topic: String, from: TaskInput): IO[Vector[TaskOutput]] =
    val subtopology = topology.subtopologies.find(_.origins.exists(Task.topicOf("app", _) == topic)).get
    Task.inMemory[IO]("app", subtopology, 0).flatMap(_.process(TaskState.empty, from, 0L)).map(_._2)

  private def chosen(outputs: Vector[TaskOutput], topic: String, count: Int) =
    outputs.filter(_.topic.value == topic).map(_.choose.map(_(count).getOrElse(-1)))

  test("a partitioner given to `to` chooses from the key, the value and the headers, and without one the default partitioning stays"):
    val byAll  = Partitioner[String, String]((key, value, headers, count) => (key.length + value.length + headers.values.size) % count)
    val placed = Topology.of(KStream(Named("read"), in, text, text).to(Named("write"), out, text, text, Some(byAll))).orFail
    val plain  = Topology.of(KStream(Named("read"), in, text, text).to(Named("write"), out, text, text)).orFail
    for
      some <- written(placed, "in", input("in", "ab", "cde"))
      none <- written(plain, "in", input("in", "ab", "cde"))
    yield
      assertEquals(chosen(some, "out", 4), Vector(Some(1)))
      assertEquals(chosen(none, "out", 4), Vector(None))

  test("a repartition's partitioner chooses each record's partition, and its partition count is declared to the broker"):
    val topology =
      Topology.of(
        KStream(Named("read"), in, text, text).repartition(Repartition("spread"), text, text, partitions = Some(3), partitioner = Some(byLength))
          .to(Named("write"), out, text, text)
      ).orFail
    val declared = Declaration("app", topology).subtopologies.flatMap(_.repartitionSourceTopics).map(topic => topic.name -> topic.partitions)
    written(topology, "in", input("in", "abcd", "v")).map: outputs =>
      assertEquals(chosen(outputs, "app-spread-repartition", 3), Vector(Some(1)))
      assertEquals(declared, List("app-spread-repartition" -> 3))

  test("a repartition with no partition count is declared with none, for the broker to work out"):
    val topology =
      Topology.of(KStream(Named("read"), in, text, text).repartition(Repartition("spread"), text, text).to(Named("write"), out, text, text)).orFail
    assertEquals(Declaration("app", topology).subtopologies.flatMap(_.repartitionSourceTopics).map(_.partitions), List(0))

  test("a repartition topic with fewer than one partition is refused"):
    def asked(count: Int) =
      Topology.of(
        KStream(Named("read"), in, text, text).repartition(Repartition("spread"), text, text, partitions = Some(count))
          .to(Named("write"), out, text, text)
      )
    assertEquals(asked(0).left.toOption.map(_.toList), Some(List(TopologyError.InvalidPartitionCount("spread", 0))))
    assertEquals(asked(-2).left.toOption.map(_.toList), Some(List(TopologyError.InvalidPartitionCount("spread", -2))))
    assert(asked(1).isRight)

  test("a stream joined with a table its key has changed for goes through the repartition topic the table's way"):
    val table    = KTable(Named("users"), users, text, text, Store("users"), partitioner = Some(byLength))
    val topology =
      Topology.of(
        KStream(Named("read"), in, text, text).selectKey(Named("by-user"))((_, user) => user)
          .join(Named("with-user"), table, text, text)((a, b) => s"$a$b").to(Named("write"), out, text, text)
      ).orFail
    written(topology, "in", input("in", "order", "abcde"))
      .map(outputs => assertEquals(chosen(outputs, "app-with-user-repartition", 3), Vector(Some(2))))

  test(
    "a foreign-key join sends each subscription where the other table's partitioner puts its key, and a table read the default way leaves it to Kafka"
  ):
    def program(partitioner: Option[Partitioner[String, Any]]) =
      val other = KTable(Named("users"), users, text, text, Store("users"), partitioner)
      Topology.of(
        KTable(Named("rows"), in, text, text, Store("rows"))
          .joinOnForeignKey(Named("fk"), other, text, text, text)((_, user) => Some(user))((a, b) => s"$a$b").toStream(Named("changes"))
          .to(Named("write"), out, text, text.option)
      ).orFail
    for
      placed <- written(program(Some(byLength)), "in", input("in", "row", "abcd"))
      plain  <- written(program(None), "in", input("in", "row", "abcd"))
    yield
      assertEquals(chosen(placed, "app-fk-subscription-registration-topic", 3), Vector(Some(1)))
      assertEquals(chosen(plain, "app-fk-subscription-registration-topic", 3), Vector(None))

  test("imapK keeps every partitioner a program gives"):
    val id       = cats.arrow.FunctionK.id[IO]
    val topology =
      Topology.of(
        KStream(Named("read"), in, text, text).repartition(Repartition("spread"), text, text, partitioner = Some(byLength))
          .to(Named("write"), out, text, text, Some(byLength))
      ).orFail.imapK(id)(id)
    for
      first  <- written(topology, "in", input("in", "ab", "v"))
      second <- written(topology, "app-spread-repartition", input("app-spread-repartition", "abcd", "v"))
    yield
      assertEquals(chosen(first, "app-spread-repartition", 3), Vector(Some(2)))
      assertEquals(chosen(second, "out", 3), Vector(Some(1)))

  test("a table made from a table read with a partitioner keeps it"):
    val table = KTable(Named("users"), users, text, text, Store("users"), partitioner = Some(byLength))
    assert(table.mapValues(Named("upper"))(_.toUpperCase).filter(Named("kept"))((_, _) => true).partitioner.isDefined)
