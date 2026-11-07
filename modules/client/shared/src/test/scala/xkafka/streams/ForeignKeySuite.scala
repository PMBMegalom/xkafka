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

import scala.concurrent.duration.*

import cats.arrow.FunctionK
import cats.effect.IO
import fs2.Chunk
import munit.CatsEffectSuite
import xkafka.internal.streams.{Declaration, ForeignKeyWire, Murmur3, Task, TaskInput, TaskOutput, TaskState}

/** What a foreign-key join does that needs more than one partition, an answer in flight, or a program `Topology.of` refuses, through tasks. */
final class ForeignKeySuite extends CatsEffectSuite:
  import ForeignKeyWire.*

  private val text  = Serde.utf8[IO]
  private val left  = Topic.from("left").orFail
  private val other = Topic.from("other").orFail
  private val out   = Topic.from("out").orFail

  private def bytes(value: String): Chunk[Byte] = Chunk.array(value.getBytes)

  private def rows   = KTable(Named("rows"), left, text, text, Store("rows"))
  private def users  = KTable(Named("users"), other, text, text, Store("users"))
  private def joined = rows.joinOnForeignKey(Named("fk"), users, text, text, text)((_, value) => Some(value))((l, r) => s"$l+$r")
  private def outer  = rows.leftJoinOnForeignKey(Named("fk"), users, text, text, text)((_, value) => Some(value))((l, r) => s"$l+${r.getOrElse("?")}")

  private def program(table: KTable.Flowing[IO, String, String, Option, Pipeline[IO]]) =
    Topology.of(table.toStream(Named("changes")).to(Named("write"), out, text, text.option)).orFail

  private def input(topic: String, key: String, value: Option[Chunk[Byte]], at: Long = 1000L): TaskInput =
    TaskInput(Some(bytes(key)), value, at, Headers.empty, topic, 0, 0L)

  private val requests  = "app-fk-subscription-registration-topic"
  private val responses = "app-fk-subscription-response-topic"

  private def taskReading(topology: Topology[IO], topic: String, partition: Int): IO[Task[IO]] =
    val subtopology = topology.subtopologies.find(_.origins.exists(Task.topicOf("app", _) == topic)).get
    Task.inMemory[IO]("app", subtopology, partition)

  private def to(written: Vector[TaskOutput], topic: String) = written.filter(_.topic.value == topic)

  test("a subscription carries its row's partition, and the answer goes back to that partition, however the other table's key is partitioned"):
    val topology = program(joined)
    for
      rowsTask  <- taskReading(topology, "left", 3)
      usersTask <- taskReading(topology, "other", 1)
      known     <- usersTask.process(TaskState.empty, input("other", "a", Some(bytes("A"))), 0L)
      sent      <- rowsTask.process(TaskState.empty, input("left", "k1", Some(bytes("a"))), 0L)
      request = to(sent._2, requests).head
      answered <- usersTask.process(known._1, TaskInput(request.key, request.value, request.timestamp, request.headers, requests, 1, 0L), 0L)
      changed  <- usersTask.process(answered._1, input("other", "a", Some(bytes("A2"))), 0L)
    yield
      assertEquals(request.partition, None)
      assertEquals(request.key, Some(bytes("a")))
      assertEquals(Subscription.decoded(request.value.get).map(_.primaryPartition), Some(3))
      assertEquals(to(answered._2, responses).map(_.partition), Vector(Some(3)))
      assertEquals(to(changed._2, responses).map(_.partition), Vector(Some(3)))
      assertEquals(to(changed._2, responses).map(_.key), Vector(Some(bytes("k1"))))

  test("an answer for a value the row has since changed from is dropped, and one for the value it holds makes the result"):
    val topology                   = program(joined)
    val hashOf                     = (value: String) => Some(Murmur3.hash128(bytes(value)))
    def answer(hash: Option[Hash]) = input("app-fk-subscription-response-topic", "k1", Some(Response(hash, Some(bytes("A"))).encoded), 2000L)
    for
      task    <- taskReading(topology, "left", 0)
      first   <- task.process(TaskState.empty, input("left", "k1", Some(bytes("a"))), 0L)
      second  <- task.process(first._1, input("left", "k1", Some(bytes("b"))), 0L)
      stale   <- task.process(second._1, answer(hashOf("a")), 0L)
      current <- task.process(stale._1, answer(hashOf("b")), 0L)
    yield
      assertEquals(to(stale._2, "out"), Vector.empty)
      assertEquals(to(current._2, "out").map(_.value), Vector(Some(bytes("b+A"))))
      assertEquals(to(current._2, "out").map(_.timestamp), Vector(2000L))

  test("an answer for a row since deleted is dropped, and one that says nothing was found for a row with no hash deletes the result"):
    val topology                   = program(joined)
    def answer(hash: Option[Hash]) = input("app-fk-subscription-response-topic", "k1", Some(Response(hash, None).encoded), 2000L)
    for
      task    <- taskReading(topology, "left", 0)
      gone    <- task.process(TaskState.empty, answer(Some(Murmur3.hash128(bytes("a")))), 0L)
      deleted <- task.process(TaskState.empty, answer(None), 0L)
    yield
      assertEquals(to(gone._2, "out"), Vector.empty)
      assertEquals(to(deleted._2, "out").map(_.value), Vector(None))

  test("a subscription or an answer that cannot be read, in a version Java no longer writes or cut short, stops the task and names the join"):
    val topology = program(outer)
    val old      = Chunk.array(Array[Byte](0, 3, 1, 2))
    for
      rowsTask  <- taskReading(topology, "left", 0)
      usersTask <- taskReading(topology, "other", 0)
      request   <- usersTask.process(TaskState.empty, input(requests, "a", Some(old)), 0L).attempt
      response  <- rowsTask.process(TaskState.empty, input(responses, "k1", Some(Chunk.array(Array[Byte](1)))), 0L).attempt
      empty     <- rowsTask.process(TaskState.empty, input(responses, "k1", None), 0L).attempt
    yield List(request, response, empty).foreach: result =>
      assert(result.left.toOption.exists(_.getMessage.contains("foreign-key join 'fk'")), result.toString)

  test("a subscription with no foreign key is dropped unless it asks for an answer, and is never kept"):
    val topology                   = program(outer)
    val hash                       = Some(Murmur3.hash128(bytes("a")))
    def keyless(instruction: Byte) =
      TaskInput(None, Some(Subscription(hash, instruction, bytes("k1"), 2).encoded), 1000L, Headers.empty, requests, 0, 0L)
    taskReading(topology, "other", 0).flatMap: task =>
      for
        delete   <- task.process(TaskState.empty, keyless(DeleteAndPropagate), 0L)
        silent   <- task.process(TaskState.empty, keyless(DeleteNoPropagate), 0L)
        found    <- task.process(TaskState.empty, keyless(PropagateOnlyIfFound), 0L)
        answered <- task.process(TaskState.empty, keyless(PropagateNullIfMissing), 0L)
      yield
        assertEquals(delete._2 ++ silent._2 ++ found._2, Vector.empty)
        assertEquals(answered._2.map(output => (output.topic.value, output.partition, output.key)), Vector((responses, Some(2), Some(bytes("k1")))))

  test("what the two sides send each other reads back as it was written"):
    val hash = Some((1L, -2L))
    val key  = bytes("primary")
    assertEquals(
      Subscription.decoded(Subscription(hash, PropagateOnlyIfFound, key, 7).encoded),
      Some(Subscription(hash, PropagateOnlyIfFound, key, 7))
    )
    assertEquals(Subscription.decoded(Subscription(None, DeleteAndPropagate, key, 0).encoded), Some(Subscription(None, DeleteAndPropagate, key, 0)))
    assertEquals(Response.decoded(Response(hash, Some(key)).encoded), Some(Response(hash, Some(key))))
    assertEquals(Response.decoded(Response(None, None).encoded), Some(Response(None, None)))
    assertEquals(Response.decoded(Response(None, Some(Chunk.empty)).encoded), Some(Response(None, None)))

  test("a subscription's store key starts with its foreign key's length and bytes, so one key's prefix finds no other's"):
    val short = combinedOf(bytes("a"), bytes("k1"))
    val long  = combinedOf(bytes("ab"), bytes("k1"))
    assertEquals(primaryOf(short, bytes("a")), bytes("k1"))
    assertEquals(short.take(prefixOf(bytes("a")).size), prefixOf(bytes("a")))
    assertNotEquals(long.take(prefixOf(bytes("a")).size), prefixOf(bytes("a")))

  test("the application declares the join's two topics, its store's changelog, and each topic copartitioned with the table that reads it"):
    val declared = Declaration("app", program(joined))
    val all      = declared.subtopologies
    val users    = all.find(_.sourceTopics.contains("other")).get
    val rows     = all.find(_.sourceTopics.contains("left")).get
    assertEquals(users.repartitionSourceTopics.map(_.name), List(requests))
    assertEquals(users.repartitionSinkTopics, List(responses))
    assertEquals(rows.repartitionSourceTopics.map(_.name), List(responses))
    assertEquals(rows.repartitionSinkTopics, List(requests))
    assert(users.stateChangelogTopics.map(_.name).contains("app-fk-subscription-store-changelog"))
    assert(users.copartitionGroups.exists(group => group.sourceTopics.nonEmpty && group.repartitionSourceTopics.nonEmpty))
    assert(rows.copartitionGroups.exists(group => group.sourceTopics.nonEmpty && group.repartitionSourceTopics.nonEmpty))

  test("a result kept in no store cannot be grouped or joined, and one kept in a store can"):
    val unkept = joined
    val kept   = rows.joinOnForeignKey(Named("fk"), users, text, text, text, Store("result"), text)((_, value) => Some(value))((l, r) => s"$l+$r")
    def grouped(table: KTable.Flowing[IO, String, String, Option, Pipeline[IO]]) =
      table.groupBy(Repartition("by-value"), text, text)((key, value) => (value, key)).count(Named("count"), Store("counts"))
        .toStream(Named("changes")).to(Named("write"), out, text, Serde.long[IO].option)
    def rejoined(table: KTable.Flowing[IO, String, String, Option, Pipeline[IO]]) =
      KStream(Named("orders"), Topic.from("orders").orFail, text, text).join(Named("with-result"), table)((a, b) => s"$a$b")
        .to(Named("write"), out, text, text)
    assertEquals(Topology.of(grouped(unkept)).left.toOption.map(_.toList), Some(List(TopologyError.UnkeptForeignKeyJoin("fk"))))
    assertEquals(Topology.of(rejoined(unkept)).left.toOption.map(_.toList), Some(List(TopologyError.WindowedJoin)))
    assert(Topology.of(grouped(kept)).isRight)
    assert(Topology.of(rejoined(kept)).isRight)

  test("a windowed table cannot join on a foreign key, on either side"):
    val windowed =
      KStream(Named("clicks"), Topic.from("clicks").orFail, text, text).groupByKey(text, text)
        .windowedBy(TimeWindows.tumbling(1.minute, grace = 1.second)).count(Named("counted"), Store("counts"))
    val onLeft = windowed.joinOnForeignKey(Named("fk"), users, Serde.long[IO], text, text)((_, _) => Some("a"))((_, r) => r)
    assertEquals(
      Topology.of(onLeft.toStream(Named("changes")).to(Named("write"), out, Serde.timeWindowed(text, 1.minute), text.option)).left.toOption
        .map(_.toList.contains(TopologyError.WindowedJoin)),
      Some(true)
    )

  test("the same store or step name twice is refused"):
    val twice =
      rows.joinOnForeignKey(Named("fk"), users, text, text, text)((_, value) => Some(value))((l, _) => l)
        .joinOnForeignKey(Named("fk"), KTable(Named("more"), Topic.from("more").orFail, text, text, Store("more")), text, text, text)((_, value) =>
          Some(value)
        )((l, _) => l)
    assert(Topology.of(twice.toStream(Named("changes")).to(Named("write"), out, text, text.option)).isLeft)

  test("imapK carries a join through to both sides"):
    val id       = FunctionK.id[IO]
    val topology = program(joined).imapK(id)(id)
    for
      rowsTask  <- taskReading(topology, "left", 0)
      usersTask <- taskReading(topology, "other", 0)
      known     <- usersTask.process(TaskState.empty, input("other", "a", Some(bytes("A"))), 0L)
      sent      <- rowsTask.process(TaskState.empty, input("left", "k1", Some(bytes("a"))), 0L)
      request = to(sent._2, requests).head
      answered <- usersTask.process(known._1, TaskInput(request.key, request.value, request.timestamp, request.headers, requests, 0, 0L), 0L)
      answer = to(answered._2, responses).head
      result <- rowsTask.process(sent._1, TaskInput(answer.key, answer.value, answer.timestamp, answer.headers, responses, 0, 0L), 0L)
    yield assertEquals(to(result._2, "out").map(_.value), Vector(Some(bytes("a+A"))))
