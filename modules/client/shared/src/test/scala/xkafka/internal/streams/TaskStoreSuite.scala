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

import cats.effect.IO
import cats.syntax.all.*
import fs2.Chunk
import munit.CatsEffectSuite
import xkafka.streams.*

final class TaskStoreSuite extends CatsEffectSuite:
  private def bytes(value: String): Chunk[Byte] = Chunk.array(value.getBytes)

  private def input(key: String, value: Option[String], at: Long): TaskInput = TaskInput(Some(bytes(key)), value.map(bytes), at, Headers.empty)

  private val in   = Topic.from("in").orFail
  private val out  = Topic.from("out").orFail
  private val text = Serde.utf8[IO]
  private val long = Serde.long[IO]

  private val counting =
    KStream(Named("read"), in, text, text).groupByKey(text, text).count(Named("count"), Store("counts")).toStream(Named("to-stream"))
      .to(Named("write"), out, text, long)

  private def over(pipeline: Pipeline[IO], held: Map[String, StoreContents] = Map.empty): IO[Task[IO]] =
    Task.inMemory[IO]("app", Topology.of(pipeline).orFail.subtopologies.head, 0, held)

  test("a store in memory gives a key, the keys starting with a prefix in order, and all it holds, and applies changes as one"):
    val one = Stored(bytes("1"), 1L)
    val two = Stored(bytes("2"), 2L)
    for
      store <- TaskStore.inMemory[IO](StoreContents.of(Map(bytes("ab") -> one, bytes("b") -> one)))
      _     <- store.write(StoreContents.unchanged.updated(bytes("a"), Some(two)).updated(bytes("b"), None))
      found <- store.get(bytes("a"))
      gone  <- store.get(bytes("b"))
      under <- store.prefixed(bytes("a"))
      all   <- store.all
    yield
      assertEquals(found, Some(two))
      assertEquals(gone, None)
      assertEquals(under, Vector(bytes("a") -> two, bytes("ab") -> one))
      assertEquals(all.keySet, Set(bytes("a"), bytes("ab")))

  test("a task reads what it wrote before it is stored, and its store holds that only once it is stored"):
    for
      task   <- over(counting)
      first  <- task.process(TaskState.empty, input("k", Some("v"), 1000L), 0L)
      second <- task.process(first._1, input("k", Some("v"), 2000L), 0L)
      before <- task.contents(TaskState.empty, "counts")
      stored <- task.stored(second._1)
      after  <- task.contents(TaskState.empty, "counts")
    yield
      assertEquals(second._2.filter(_.topic == out).flatMap(_.value), Vector(Chunk.array(java.nio.ByteBuffer.allocate(8).putLong(2L).array)))
      assertEquals(before, StoreContents.empty)
      assertEquals(stored.pending, Map.empty)
      assertEquals(after.keySet, Set(bytes("k")))

  test("what a task wrote and never stored leaves its store as it was, for a task that starts again from it"):
    for
      task   <- over(counting)
      first  <- task.process(TaskState.empty, input("k", Some("v"), 1000L), 0L)
      stored <- task.stored(first._1)
      lost   <- task.process(stored, input("k", Some("v"), 2000L), 0L)
      again  <- task.process(stored, input("k", Some("v"), 3000L), 0L)
    yield
      val two = Chunk.array(java.nio.ByteBuffer.allocate(8).putLong(2L).array)
      assertEquals(lost._2.filter(_.topic == out).flatMap(_.value), Vector(two))
      assertEquals(again._2.filter(_.topic == out).flatMap(_.value), Vector(two))

  test("a key removed since the last commit reads as absent though its store still holds it"):
    val table =
      KTable(Named("read-table"), in, text, text, Store("latest")).toStream(Named("to-stream-table")).to(Named("write-table"), out, text, text.option)
    val held = Map("latest" -> StoreContents.of(Map(bytes("k") -> Stored(bytes("old"), 1L))))
    for
      task    <- over(table, held)
      removed <- task.process(TaskState.empty, input("k", None, 2000L), 0L)
      read    <- task.contents(removed._1, "latest")
      kept    <- task.contents(TaskState.empty, "latest")
      again   <- task.process(removed._1, input("k", None, 3000L), 0L)
      stored  <- task.stored(removed._1)
      after   <- task.contents(stored, "latest")
    yield
      assertEquals(read, StoreContents.empty)
      assertEquals(kept.keySet, Set(bytes("k")))
      assertEquals(after, StoreContents.empty)
      assert(again._2.nonEmpty)

  test("sessions of a key merge across a commit as within one, the store's and the unstored together"):
    val sessions =
      KStream(Named("read-s"), in, text, text).groupByKey(text, text).windowedBy(SessionWindows.withNoGrace(10.seconds))
        .count(Named("count-s"), Store("sessions")).toStream(Named("to-stream-s")).to(Named("write-s"), out, Serde.sessionWindowed(text), long.option)
    val records             = List(("k", 1000L), ("j", 1500L), ("k", 5000L), ("k", 40000L), ("k", 45000L))
    def run(store: Boolean) =
      over(sessions).flatMap: task =>
        records.foldLeftM((TaskState.empty, Vector.empty[TaskOutput])):
          case ((state, written), (key, at)) => task.process(state, input(key, Some("v"), at), 0L).flatMap: (after, more) =>
              (if store then task.stored(after) else IO.pure(after)).map(_ -> (written ++ more))
        .flatMap((state, written) => task.contents(state, "sessions").map(written -> _))
    (run(true), run(false)).mapN((stored, unstored) => assertEquals(stored, unstored))

  test("a task writing to a store it does not have fails as it stores, keeping nothing back"):
    val task = Task[IO]("app", Topology.of(counting).orFail.subtopologies.head, 0)
    task.process(TaskState.empty, input("k", Some("v"), 1000L), 0L).flatMap((state, _) => task.stored(state)).attempt.map: result =>
      assert(result.left.exists(_.getMessage.contains("no store 'counts'")), result)
