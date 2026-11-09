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

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import fs2.Chunk
import fs2.concurrent.SignallingRef
import munit.CatsEffectSuite
import protocol.StreamsGroup
import xkafka.streams.{Endpoint, Forward, KTable, Named, QueryableStore, QueryProblem, QueryRequest, Serde, Store, StreamsPhase, Topology}

/** Queries answered by two members in one process, each asking the other through its `answer`, over a table read from a topic of four partitions. */
final class QueryingSuite extends CatsEffectSuite:
  private val text     = Serde.utf8[IO]
  private val in       = Topic.from("in").orFail
  private val topology =
    Topology.of(
      KTable(Named("read"), in, text, text, Store("latest")).toStream(Named("changes"))
        .to(Named("write"), Topic.from("out").orFail, text, text.option)
    ).orFail
  private val latest  = QueryableStore.keyValue(Store("latest"), text, text)
  private val counted = 4

  private val here  = Endpoint.from("here", 1).orFail
  private val there = Endpoint.from("there", 2).orFail

  private def bytes(value: String) = Chunk.array(value.getBytes)

  /** The partition a key's records go to. */
  private def partitionOf(key: String) = Murmur2.partition(bytes(key), counted)

  /** Keys whose records go to each partition. */
  private val keys = (0 until 500).map(n => s"key-$n").groupBy(partitionOf).view.mapValues(_.take(2).toList).toMap

  private def endpointOf(at: Endpoint, active: Set[Int], standby: Set[Int]) =
    StreamsGroup.EndpointPartitions(
      StreamsGroup.Endpoint(at.host, at.port),
      List(StreamsGroup.TopicPartitions("in", active.toList.sorted)),
      List(StreamsGroup.TopicPartitions("in", standby.toList.sorted))
    )

  /** One member holding a store for each partition of `active` and `standby`, each holding its keys valued by the member's name and the key. */
  private final case class Holder(name: String, active: Set[Int], standby: Set[Int] = Set.empty)

  private def member(holder: Holder, everyone: List[StreamsGroup.EndpointPartitions], phase: StreamsPhase, forward: Forward[IO], at: Endpoint) =
    val identity =
      MemberIdentity("app", holder.name, holder.name, Declaration("app", topology), 300000, None, Nil, Some(StreamsGroup.Endpoint(at.host, at.port)))
    val joined =
      Member(identity)
        .copy(owned = Assignment((holder.active).map(TaskId("0", _)), holder.standby.map(TaskId("0", _)), Set.empty), endpoints = everyone)
    for
      stores <- (holder.active ++ holder.standby).toList.traverse: partition =>
        val held = keys.getOrElse(partition, Nil).map(key => bytes(key) -> Stored(bytes(s"${holder.name}:$key"), 1L))
        TaskStore.inMemory[IO](StoreContents.of(held)).map(store => TaskId("0", partition) -> Map("latest" -> store))
      holding <- Ref.of[IO, Map[TaskId, Map[String, TaskStore[IO]]]](stores.toMap)
      signal  <- SignallingRef[IO].of(joined)
      phased  <- SignallingRef[IO].of(phase)
    yield Querying[IO](
      "app",
      topology.numbered.map((id, s) => id.toString -> s).toMap,
      Map.empty,
      holding,
      signal,
      phased,
      _ => IO.pure(counted),
      Some(at),
      forward
    )

  /** Two members, the first asked, the second answering what it forwards through its own `answer`. */
  private def pair(first: Holder, second: Holder, phase: StreamsPhase = StreamsPhase.Running, broken: Option[Forward[IO]] = None) =
    val everyone = List(endpointOf(here, first.active, first.standby), endpointOf(there, second.active, second.standby))
    member(second, everyone, StreamsPhase.Running, Forward.none[IO], there).flatMap: answering =>
      val forward: Forward[IO] =
        broken.getOrElse((to, request) => if to == there then answering.answer(request) else IO.raiseError(new RuntimeException(s"no $to")))
      member(first, everyone, phase, forward, here)

  test("a key whose partition this member holds is read here, and one another member holds is asked of it"):
    val (mine, theirs) = (keys(0).head, keys(3).head)
    pair(Holder("a", Set(0, 1)), Holder("b", Set(2, 3))).flatMap: asking =>
      (asking.query(latest.get(mine), false), asking.query(latest.get(theirs), false), asking.query(latest.get("absent"), false)).tupled.map:
        (local, remote, missing) =>
          assertEquals(local, Right(Some(s"a:$mine")))
          assertEquals(remote, Right(Some(s"b:$theirs")))
          assertEquals(missing, Right(None))

  test("a range takes each partition from the member holding it, in the order of the keys' bytes"):
    pair(Holder("a", Set(0, 1)), Holder("b", Set(2, 3))).flatMap: asking =>
      asking.query(latest.all, false).map: all =>
        val expected = keys.toList.flatMap((partition, held) => held.map(key => key -> s"${if partition < 2 then "a" else "b"}:$key"))
        assertEquals(all.map(_.map(_._1)), Right(expected.map(_._1).sortBy(bytes)(using StoreContents.keys).toVector))
        assertEquals(all.map(_.toMap), Right(expected.toMap))

  test("a range from one key to another keeps only the keys between them, both included"):
    pair(Holder("a", Set(0, 1)), Holder("b", Set(2, 3))).flatMap: asking =>
      val ordered = keys.values.flatten.toVector.sortBy(bytes)(using StoreContents.keys)
      asking.query(latest.range(ordered(1), ordered(4)), false).map(found => assertEquals(found.map(_.map(_._1)), Right(ordered.slice(1, 5))))

  test("a member still filling its stores, or a partition no member holds, answers as rebalancing"):
    for
      restoring <- pair(Holder("a", Set(0, 1)), Holder("b", Set(2, 3)), StreamsPhase.Restoring)
      early     <- restoring.query(latest.get(keys(0).head), false)
      unheld    <- pair(Holder("a", Set(0, 1)), Holder("b", Set(2)))
      missing   <- unheld.query(latest.get(keys(3).head), false)
      ranged    <- unheld.query(latest.all, false)
    yield assertEquals(
      List(early, missing, ranged),
      List(Left(QueryProblem.Rebalancing), Left(QueryProblem.Rebalancing), Left(QueryProblem.Rebalancing))
    )

  test("a key asked of a partition it does not hash to, as a miscounting asker sends it, answers as rebalancing, and of its own is read"):
    val key                        = keys(3).head
    def ask(asked: Asked)          = (answering: Querying[IO]) => answering.answer(QueryWire.encode(asked)).map(QueryWire.answered)
    def get(partitions: List[Int]) = Asked(QueryRequest.Get("latest", bytes(key)), partitions, false)
    member(Holder("b", Set(2, 3)), Nil, StreamsPhase.Running, Forward.none[IO], there).flatMap: answering =>
      (ask(get(List(2)))(answering), ask(get(List(3)))(answering), ask(Asked(QueryRequest.Range("latest", None, None), List(2), false))(answering))
        .tupled.map: (elsewhere, own, ranged) =>
          assertEquals(elsewhere, Some(Answered.Refused(QueryProblem.Rebalancing)))
          assertEquals(own, Some(Answered.Found(Vector(bytes(key) -> bytes(s"b:$key")))))
          assertEquals(
            ranged,
            Some(Answered.Found(keys(2).map(held => bytes(held) -> bytes(s"b:$held")).toVector.sortBy(_._1)(using StoreContents.keys)))
          )

  test("a member that cannot be reached, or answers in another format, says which, and a store the program lacks is named"):
    val failing: Forward[IO] = (_, _) => IO.raiseError(new RuntimeException("refused"))
    val foreign: Forward[IO] = (_, _) => IO.pure(bytes("{\"error\":\"not found\"}"))
    for
      unreachable <- pair(Holder("a", Set(0, 1)), Holder("b", Set(2, 3)), broken = Some(failing)).flatMap(_.query(latest.get(keys(3).head), false))
      java        <- pair(Holder("a", Set(0, 1)), Holder("b", Set(2, 3)), broken = Some(foreign)).flatMap(_.query(latest.get(keys(3).head), false))
      unknown     <-
        pair(Holder("a", Set(0, 1)), Holder("b", Set(2, 3))).flatMap(_.query(QueryableStore.keyValue(Store("other"), text, text).get("k"), false))
    yield
      assertEquals(unreachable, Left(QueryProblem.Unreachable(there, "refused")))
      assertEquals(java, Left(QueryProblem.NotXkafka(there)))
      assertEquals(unknown, Left(QueryProblem.UnknownStore("other")))

  test("a standby copy answers only where the query allows it, and the active task's member otherwise"):
    val key = keys(3).head
    pair(Holder("a", Set(0, 1), standby = Set(3)), Holder("b", Set(2, 3))).flatMap: asking =>
      (asking.query(latest.get(key), false), asking.query(latest.get(key), true)).tupled.map: (active, standby) =>
        assertEquals(active, Right(Some(s"b:$key")))
        assertEquals(standby, Right(Some(s"a:$key")))

  test("a key only another member's standby holds is asked of it where standbys may answer, and is rebalancing where they may not"):
    val key = keys(3).head
    pair(Holder("a", Set(0, 1)), Holder("b", Set(2), standby = Set(3))).flatMap: asking =>
      (asking.query(latest.get(key), false), asking.query(latest.get(key), true)).tupled.map: (active, standby) =>
        assertEquals(active, Left(QueryProblem.Rebalancing))
        assertEquals(standby, Right(Some(s"b:$key")))

  test("a partition the group says this member holds, but which it is not running yet, is rebalancing, not asked of itself"):
    val key      = keys(1).head
    val everyone = List(endpointOf(here, Set(0, 1), Set.empty), endpointOf(there, Set(2, 3), Set.empty))
    // This member runs only partition 0 so far, though the group already counts 1 as its.
    member(Holder("a", Set(0)), everyone, StreamsPhase.Running, (to, _) => IO.raiseError(new RuntimeException(s"asked $to")), here)
      .flatMap(_.query(latest.get(key), false)).map(found => assertEquals(found, Left(QueryProblem.Rebalancing)))

  test("a global table's store is read here, every member holding it whole, and a processor's store is found as a table's is"):
    val numbers   = Serde.long[IO]
    val processed = xkafka.streams.KeyValueStore.of(Store("tally"), text, numbers)
    val program   =
      xkafka.streams.KStream(Named("read-tally"), in, text, text).process(
        Named("tally"),
        xkafka.streams.Processor.withStore(processed)(kept => xkafka.streams.Processor.onRecord(record => kept.put(record.key, 1L).as(List(record))))
      ).to(Named("write-tally"), Topic.from("tallied").orFail, text, text)
    val subtopologies = Topology.of(program).orFail.numbered.map((id, s) => id.toString -> s).toMap
    val key           = keys(1).head
    for
      global <- TaskStore.inMemory[IO](StoreContents.of(Map(bytes("k") -> Stored(bytes("v"), 1L))))
      tally <- TaskStore.inMemory[IO](StoreContents.of(Map(bytes(key) -> Stored(Chunk.array(java.nio.ByteBuffer.allocate(8).putLong(5L).array), 1L))))
      holding <- Ref.of[IO, Map[TaskId, Map[String, TaskStore[IO]]]](Map(TaskId("0", 1) -> Map("tally" -> tally)))
      identity = MemberIdentity("app", "m", "m", Declaration("app", topology), 300000, None, Nil, None)
      signal <- SignallingRef[IO].of(Member(identity).copy(owned = Assignment(Set(TaskId("0", 1)), Set.empty, Set.empty)))
      phased <- SignallingRef[IO].of[StreamsPhase](StreamsPhase.Running)
      asking = Querying[IO]("app", subtopologies, Map("gs" -> global), holding, signal, phased, _ => IO.pure(counted), None, Forward.none[IO])
      fromGlobal <- asking.query(QueryableStore.keyValue(Store("gs"), text, text).get("k"), false)
      fromTally  <- asking.query(QueryableStore.keyValue(Store("tally"), text, numbers).get(key), false)
    yield
      assertEquals(fromGlobal, Right(Some("v")))
      assertEquals(fromTally, Right(Some(5L)))

  test("queries and answers read back as they were written, and bytes that are neither read as nothing"):
    val asked =
      List(
        Asked(QueryRequest.Get("s", bytes("k")), List(3), false),
        Asked(QueryRequest.Range("s", None, Some(bytes("z"))), List(0, 1, 2), true),
        Asked(QueryRequest.Range("s", Some(Chunk.empty), None), Nil, false)
      )
    val answered =
      List(
        Answered.Found(Vector(bytes("a") -> bytes("1"), bytes("b") -> Chunk.empty)),
        Answered.Found(Vector.empty),
        Answered.Refused(QueryProblem.Rebalancing),
        Answered.Refused(QueryProblem.UnknownStore("s"))
      )
    assertEquals(asked.map(QueryWire.encode).map(QueryWire.asked), asked.map(Some(_)))
    assertEquals(answered.map(QueryWire.encode).map(QueryWire.answered), answered.map(Some(_)))
    assertEquals(QueryWire.asked(QueryWire.encode(asked.head).dropRight(1)), None)
    assertEquals(QueryWire.answered(QueryWire.encode(answered.head) ++ Chunk.singleton(0.toByte)), None)
    assertEquals((QueryWire.asked(bytes("GET /")), QueryWire.answered(bytes("{}"))), (None, None))
