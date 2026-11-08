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

import scala.concurrent.duration.*

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import fs2.Chunk
import fs2.concurrent.SignallingRef
import munit.CatsEffectSuite
import protocol.StreamsGroup
import xkafka.streams.{
  Endpoint, Forward, KStream, Named, QueryableStore, QueryProblem, Serde, SessionWindows, Store, StreamsPhase, TimeWindows, Topology, Window
}

/** Window and session queries answered by two members in one process, each asking the other through its `answer`, over a topic of four partitions.
  */
final class WindowQueriesSuite extends CatsEffectSuite:
  private val text    = Serde.utf8[IO]
  private val numbers = Serde.long[IO]
  private val in      = Topic.from("in").orFail
  private val counted = 4

  private val windowed =
    KStream(Named("read"), in, text, text).groupByKey(text, text).windowedBy(TimeWindows.tumbling(10.seconds, grace = 1.hour))
      .count(Named("count-windows"), Store("windows")).toStream(Named("windows-changes"))
      .to(Named("write-windows"), Topic.from("out").orFail, Serde.timeWindowed(text, 10.seconds), numbers)
  private val sessioned =
    KStream(Named("read-s"), Topic.from("in-s").orFail, text, text).groupByKey(text, text).windowedBy(SessionWindows.withNoGrace(5.seconds))
      .count(Named("count-sessions"), Store("sessions")).toStream(Named("sessions-changes"))
      .to(Named("write-sessions"), Topic.from("out-s").orFail, Serde.sessionWindowed(text), numbers.option)
  private val topology = Topology.of(windowed, sessioned).orFail

  private val here  = Endpoint.from("here", 1).orFail
  private val there = Endpoint.from("there", 2).orFail

  private def bytes(value: String) = Chunk.array(value.getBytes)
  private def count(n: Long)       = Stored(Chunk.array(java.nio.ByteBuffer.allocate(8).putLong(n).array), 1L)

  /** Two keys of each partition, the first starting the second. */
  private val keys = (0 until 2000).map(n => s"k$n").groupBy(key => Murmur2.partition(bytes(key), counted)).view.mapValues(_.head).toMap

  /** What each store of a task holds: each key's windows starting at 0, 10 s and 20 s, counted 1, 2 and 3, and its sessions from 0 to 1 s and from 30
    * s to 31 s, counted 4 and 5; and, beside the key, windows and sessions of a key it starts, counted 9, which the key's queries must not see.
    */
  private def held(partition: Int, subtopology: String): IO[(TaskId, Map[String, TaskStore[IO]])] =
    val key    = keys(partition)
    val longer = s"${key}x"
    val store  =
      if subtopology == "0" then
        StoreContents.of(
          List(0L -> 1L, 10000L -> 2L, 20000L -> 3L).map((start, n) => WindowStore.key(bytes(key), start) -> count(n)) :+
            (WindowStore.key(bytes(longer), 10000L) -> count(9L))
        )
      else
        StoreContents.of(List(
          SessionStore.key(bytes(key), 30000L, 31000L) -> count(5L),
          SessionStore.key(bytes(key), 0L, 1000L)      -> count(4L),
          SessionStore.key(bytes(longer), 0L, 1000L)   -> count(9L)
        ))
    val time: TaskStore.TimeOf = if subtopology == "0" then WindowStore.startOf else SessionStore.endOf
    TaskStore.inMemory[IO](store, Some(time))
      .map(made => TaskId(subtopology, partition) -> Map((if subtopology == "0" then "windows" else "sessions") -> made))

  private def endpointOf(at: Endpoint, partitions: Set[Int]) =
    StreamsGroup.EndpointPartitions(
      StreamsGroup.Endpoint(at.host, at.port),
      List("in", "in-s").map(topic => StreamsGroup.TopicPartitions(topic, partitions.toList.sorted)),
      Nil
    )

  private def member(name: String, partitions: Set[Int], everyone: List[StreamsGroup.EndpointPartitions], forward: Forward[IO], at: Endpoint) =
    val identity = MemberIdentity("app", name, name, Declaration("app", topology), 300000, None, Nil, None)
    val tasks    = partitions.flatMap(partition => Set(TaskId("0", partition), TaskId("1", partition)))
    for
      stores  <- tasks.toList.traverse(task => held(task.partition, task.subtopology))
      holding <- Ref.of[IO, Map[TaskId, Map[String, TaskStore[IO]]]](stores.toMap)
      signal  <- SignallingRef[IO].of(Member(identity).copy(owned = Assignment(tasks, Set.empty, Set.empty), endpoints = everyone))
      phased  <- SignallingRef[IO].of[StreamsPhase](StreamsPhase.Running)
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

  /** The member holding partitions 0 and 1, asking the one holding 2 and 3 through its answer. */
  private val asking =
    val everyone = List(endpointOf(here, Set(0, 1)), endpointOf(there, Set(2, 3)))
    member("b", Set(2, 3), everyone, Forward.none[IO], there).flatMap: answering =>
      member(
        "a",
        Set(0, 1),
        everyone,
        (to, request) => if to == there then answering.answer(request) else IO.raiseError(new RuntimeException(s"no $to")),
        here
      )

  private val windows  = QueryableStore.window(Store("windows"), text, numbers)
  private val sessions = QueryableStore.session(Store("sessions"), text, numbers)

  test("a key's windows starting in a time range come from the member holding the key, by their starts, without those of a longer key"):
    asking.flatMap: querying =>
      (
        querying.query(windows.fetch(keys(0), Timestamp.fromEpochMillis(5000L), Timestamp.fromEpochMillis(20000L)), false),
        querying.query(windows.fetch(keys(3), Timestamp.fromEpochMillis(0L), Timestamp.fromEpochMillis(10000L)), false)
      ).tupled.map: (local, remote) =>
        assertEquals(local, Right(Vector(10000L -> 2L, 20000L -> 3L)))
        assertEquals(remote, Right(Vector(0L -> 1L, 10000L -> 2L)))

  test("the windows of every key in a time range come from every member, by their starts and then by their keys"):
    asking.flatMap: querying =>
      querying.query(windows.fetchAll(Timestamp.fromEpochMillis(0L), Timestamp.fromEpochMillis(20000L)), false).map: found =>
        val byBytes  = Ordering.by(bytes)(using StoreContents.keys)
        val plain    = (0 until counted).toList.map(keys)
        val longer   = plain.map(key => s"${key}x")
        val expected =
          List(0L -> 1L, 10000L -> 2L, 20000L -> 3L).flatMap: (start, n) =>
            val here = plain.map(_ -> n) ++ (if start == 10000L then longer.map(_ -> 9L) else Nil)
            here.sortBy(_._1)(using byBytes).map((key, count) => (key, start, count))
        assertEquals(found, Right(expected.toVector))

  test("a key's sessions come from the member holding the key, by their ends, without those of a longer key"):
    asking.flatMap: querying =>
      (querying.query(sessions.fetch(keys(1)), false), querying.query(sessions.fetch(keys(2)), false)).tupled.map: (local, remote) =>
        val both = Vector(Window(0L, 1000L) -> 4L, Window(30000L, 31000L) -> 5L)
        assertEquals((local, remote), (Right(both), Right(both)))

  test("a query of another kind than its store's names the store as unknown"):
    asking.flatMap: querying =>
      (
        querying.query(QueryableStore.keyValue(Store("windows"), text, numbers).get("k"), false),
        querying.query(
          QueryableStore.window(Store("sessions"), text, numbers).fetch("k", Timestamp.fromEpochMillis(0L), Timestamp.fromEpochMillis(1L)),
          false
        ),
        querying.query(QueryableStore.session(Store("windows"), text, numbers).fetch("k"), false)
      ).tupled.map: (keyed, windowedSessions, sessionedWindows) =>
        assertEquals(
          List(keyed, windowedSessions, sessionedWindows).map(_.left.toOption),
          List("windows", "sessions", "windows").map(name => Some(QueryProblem.UnknownStore(name)))
        )
