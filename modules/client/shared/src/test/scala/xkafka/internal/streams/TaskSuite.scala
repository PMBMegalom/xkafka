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

import cats.arrow.FunctionK
import cats.effect.IO
import cats.syntax.all.*
import fs2.Chunk
import munit.CatsEffectSuite
import xkafka.streams.{JoinWindows, KStream, KTable, Named, Pipeline, Serde, Store, TimeWindows, Topology}

/** What a task writes to its changelogs and keeps between records, on every platform. */
final class TaskSuite extends CatsEffectSuite:
  private val text  = Serde.utf8[IO]
  private val count = Serde.long[IO]
  private val in    = Topic.from("in").orFail
  private val out   = Topic.from("out").orFail

  private def bytes(text: String): Option[Chunk[Byte]] = Some(Chunk.array(text.getBytes))
  private def long(n: Long): Option[Chunk[Byte]]       = Some(Chunk.array(java.nio.ByteBuffer.allocate(8).putLong(n).array))
  private val trace                                    = Headers(Header("trace", bytes("1")))

  private def task(pipeline: Pipeline[IO], partition: Int): Task[IO] = Task("app", Topology.of(pipeline).orFail.subtopologies.head, partition)

  private val counting =
    KStream(Named("read-1"), in, text, text).groupByKey(text, text).count(Named("count-1"), Store("counts")).toStream(Named("to-stream-1"))
      .to(Named("write-1"), out, text, count)

  // A member taking over a task restores the window store, and its clock with it, but its windowing step's stream time starts again, as Java's does.
  // A record the last member found past the grace can then reach a window the store has expired: Java's in-memory store reads it as absent and does
  // not keep the write, which its changelog still logs (InMemoryWindowStore.fetch and put, ChangeLoggingWindowBytesStore.put).
  test("a join side's store drops a record once its clock is a whole retention past the record's time, and keeps one a millisecond younger"):
    val joined =
      KStream(Named("left"), Topic.from("left").orFail, text, text).join(
        Named("pairs"),
        KStream(Named("right"), Topic.from("right").orFail, text, text),
        JoinWindows(5.seconds, grace = 1.second),
        Store("pairs"),
        text,
        text,
        text
      )(_ + _).to(Named("write-pairs"), out, text, text)
    val subtopology                 = Topology.of(joined).orFail.subtopologies.head
    val retention                   = subtopology.retentions("pairs-this-join-store")
    def left(key: String, at: Long) = TaskInput(bytes(key), bytes("v"), at, Headers.empty, "left")
    Task.inMemory[IO]("app", subtopology, 0).flatMap: task =>
      List(left("a", 0L), left("b", 1L), left("c", retention)).foldLeftM(TaskState.empty)((state, input) => task.process(state, input, 0L).map(_._1))
        .flatMap(task.contents(_, "pairs-this-join-store")).map: held =>
          assertEquals(held.keysIterator.map(WindowStore.startOf).toList.sorted, List(1L, retention))

  test("after a task moves, a record for a window its store has expired starts the window again, logged but not kept"):
    val windowed =
      KStream(Named("read-2"), in, text, text).groupByKey(text, text).windowedBy(TimeWindows.tumbling(10.seconds, grace = 5.seconds))
        .count(Named("count-2"), Store("windows")).toStream(Named("to-stream-2"))
        .to(Named("write-2"), out, Serde.timeWindowed(text, 10.seconds), count)
    val old    = WindowStore.key(Chunk.array("k".getBytes), 0L)
    val latest = WindowStore.key(Chunk.array("other".getBytes), 30000L)
    // The store as restoring leaves it: its clock is at 30000, so the window starting at 0 has expired and gone, as the engine and Java's store drop it.
    val restored = Map("windows" -> StoreContents.of(Map(latest -> Stored(long(1).get, 30000L))))
    Task.inMemory[IO]("app", Topology.of(windowed).orFail.subtopologies.head, 0, restored).flatMap: running =>
      running.process(TaskState(Map.empty, Map("in" -> 30000L)), TaskInput(bytes("k"), bytes("v"), 2000L, Headers.empty), 0L).flatMap:
        (state, written) =>
          assertEquals(written.head, TaskOutput(Topic.from("app-windows-changelog").orFail, Some(0), Some(old), long(1), 2000L, Headers.empty))
          assertEquals(written(1).value, long(1))
          running.contents(state, "windows").map(held => assertEquals(held.keySet, Set(latest)))

  // Read in Java's AbstractKStreamTimeWindowAggregateProcessor and KStreamWindowAggregate: an emission ends at the window starting at the close
  // time less the size, the next starts just after it, and a look that would end at the same windows emits nothing and keeps the close time.
  test("windows emitted once they close come out once each, and a look that finds no new window keeps the close time last emitted for"):
    val emitting =
      KStream(Named("read-3"), in, text, text).groupByKey(text, text).windowedBy(TimeWindows.tumbling(10.seconds, grace = 5.seconds))
        .withRetention(1.minute).emitOnWindowClose.count(Named("count-3"), Store("windows")).toStream(Named("to-stream-3"))
        .to(Named("write-3"), out, Serde.timeWindowed(text, 10.seconds), count)
    val counted = task(emitting, 0)
    val inputs  = List(1000L, 12000L, 25000L, 31000L, 36000L)
    inputs.zipWithIndex.foldLeftM((TaskState.empty, Vector.empty[Long], Vector.empty[Option[Long]])):
      case ((state, emitted, lasts), (timestamp, index)) => counted
          .process(state, TaskInput(bytes("k"), bytes("v"), timestamp, Headers.empty), index * 2000L).map: (after, written) =>
            val starts = written.filter(_.topic == out).flatMap(_.key).map(key => java.nio.ByteBuffer.wrap(key.drop(key.size - 8).toArray).getLong)
            (after, emitted ++ starts, lasts :+ after.lastEmitted.get("windows"))
    .map: (_, emitted, lasts) =>
      assertEquals(emitted, Vector(0L, 10000L, 20000L))
      // The window at 10000 ends the emission at close time 20000; the look at 26000 would end at it again, so emits nothing and keeps 20000.
      assertEquals(lasts, Vector(None, None, Some(20000L), Some(20000L), Some(31000L)))

  // The example in the docs: a record at 2:05 both closes the window from 0:00 and starts the one from 2:00, past the default retention of 70 s.
  test("a window kept only for its size and grace is removed before its result is sent when record time jumps, and kept longer it is sent"):
    def emitted(retention: Option[FiniteDuration]) =
      val windowed = KStream(Named("read-4"), in, text, text).groupByKey(text, text).windowedBy(TimeWindows.tumbling(1.minute, grace = 10.seconds))
      val kept     = retention.fold(windowed)(windowed.withRetention)
      val counted  =
        task(
          kept.emitOnWindowClose.count(Named("count-4"), Store("windows")).toStream(Named("to-stream-4"))
            .to(Named("write-4"), out, Serde.timeWindowed(text, 1.minute), count),
          0
        )
      for
        first  <- counted.process(TaskState.empty, TaskInput(bytes("k"), bytes("v"), 30000L, Headers.empty), 0L)
        second <- counted.process(first._1, TaskInput(bytes("k"), bytes("v"), 125000L, Headers.empty), 2000L)
      yield second._2.filter(_.topic == out).flatMap(_.key).map(key => java.nio.ByteBuffer.wrap(key.drop(key.size - 8).toArray).getLong)
    (emitted(None), emitted(Some(10.minutes))).mapN: (lost, sent) =>
      assertEquals(lost, Vector.empty)
      assertEquals(sent, Vector(0L))

  // Java partitions a windowed key by the key alone (WindowedStreamPartitioner), whatever effect the program runs in.
  test("a windowed key's records are partitioned by its key alone, and still are once the topology's effects are mapped"):
    val windowed =
      KStream(Named("read-5"), in, text, text).groupByKey(text, text).windowedBy(TimeWindows.tumbling(10.seconds, grace = 5.seconds))
        .count(Named("count-5"), Store("windows")).toStream(Named("to-stream-5"))
        .to(Named("write-5"), out, Serde.timeWindowed(text, 10.seconds), count)
    val mapped = Task("app", Topology.of(windowed).orFail.imapK(FunctionK.id[IO])(FunctionK.id[IO]).subtopologies.head, 0)
    List(task(windowed, 0), mapped).traverse(_.process(TaskState.empty, TaskInput(bytes("k"), bytes("v"), 2000L, Headers.empty), 0L))
      .map(found => assertEquals(found.map(_._2.filter(_.topic == out).map(_.partitionBy)), List(Vector(bytes("k")), Vector(bytes("k")))))

  test("what a task writes for a record comes in the order it runs: each record's change to a store, then what that record goes on to write"):
    val fanned =
      KStream(Named("read-9"), in, text, text).flatMapValues(Named("flat-map-values-1"))(v => List(s"${v}1", s"${v}2")).groupByKey(text, text)
        .count(Named("count-9"), Store("counts")).toStream(Named("to-stream-9")).to(Named("write-9"), out, text, count)
    task(fanned, 0).process(TaskState.empty, TaskInput(bytes("k"), bytes("v"), 1000L, Headers.empty), 0L).map: (_, written) =>
      assertEquals(written.map(_.topic.value), Vector("app-counts-changelog", "out", "app-counts-changelog", "out"))
      assertEquals(written.map(_.value), Vector(long(1), long(1), long(2), long(2)))

  test("each change to a store goes to its changelog on the task's own partition, without the record's headers"):
    task(counting, 3).process(TaskState.empty, TaskInput(bytes("k"), bytes("v"), 1000L, trace), 0L).map: (_, written) =>
      assertEquals(
        written,
        Vector(
          TaskOutput(Topic.from("app-counts-changelog").orFail, Some(3), bytes("k"), long(1), 1000L, Headers.empty),
          TaskOutput(out, None, bytes("k"), long(1), 1000L, trace)
        )
      )

  test("the partition time is the largest timestamp read, kept between records"):
    val counted = task(counting, 0)
    for
      first  <- counted.process(TaskState.empty, TaskInput(bytes("k"), bytes("v"), 3000L, Headers.empty, "in"), 0L)
      second <- counted.process(first._1, TaskInput(bytes("k"), bytes("v"), 2000L, Headers.empty, "in"), 0L)
    yield
      assertEquals((first._1.partitionTime("in"), second._1.partitionTime("in")), (3000L, 3000L))
      assertEquals(second._1.store("counts").get(Chunk.array("k".getBytes)), Some(Stored(long(2).get, 3000L)))

  test("a value its serializer writes as none removes the key from the store and writes a tombstone to the changelog"):
    val emptyIsNone =
      Serde[IO, String](Serializer.instance((t, h, v) => if v.isEmpty then IO.pure(None) else text.serializer.serialize(t, h, v)), text.deserializer)
    val latest =
      KStream(Named("read-6"), in, text, text).groupByKey(text, text)
        .aggregate(Named("aggregate-1"), Store("latest"), emptyIsNone)("")((_, v, _) => v).toStream(Named("to-stream-6"))
        .to(Named("write-6"), out, text, emptyIsNone)
    val running = task(latest, 0)
    for
      first  <- running.process(TaskState.empty, TaskInput(bytes("k"), bytes("v"), 1000L, Headers.empty), 0L)
      second <- running.process(first._1, TaskInput(bytes("k"), bytes(""), 2000L, Headers.empty), 0L)
    yield
      assertEquals(first._1.store("latest").keySet, Set(Chunk.array("k".getBytes)))
      assertEquals(second._1.store("latest"), StoreContents.empty)
      assertEquals(second._2.head, TaskOutput(Topic.from("app-latest-changelog").orFail, Some(0), bytes("k"), None, 2000L, Headers.empty))

  test("a record whose key its serializer writes as none is not counted, as Java skips a record with a null key"):
    val dashIsNone =
      Serde[IO, String](Serializer.instance((t, h, k) => if k == "-" then IO.pure(None) else text.serializer.serialize(t, h, k)), text.deserializer)
    val counted =
      task(
        KStream(Named("read-7"), in, text, text).groupByKey(dashIsNone, text).count(Named("count-6"), Store("counts")).toStream(Named("to-stream-7"))
          .to(Named("write-7"), out, text, count),
        0
      )
    counted.process(TaskState.empty, TaskInput(bytes("-"), bytes("v"), 1000L, Headers.empty), 0L).map: (state, written) =>
      assertEquals((state.store("counts"), written), (StoreContents.empty, Vector.empty))

  // The values and times Java's outer join of tables writes for these changes (WindowGoldenSuite runs Java's against ours): a delete on the left
  // joins the right's value alone, at the later time, and the right's delete after it, finding neither side, deletes the result, since the outer
  // join has the right side's old value.
  test("two tables outer joined into a store write each change's result to the store's changelog and on, a delete where neither side holds the key"):
    val joined =
      KTable(Named("lefts"), Topic.from("left").orFail, text, text, Store("lefts")).outerJoin(
        Named("outer-join-rights"),
        KTable(Named("rights"), Topic.from("right").orFail, text, text, Store("rights")),
        Store("joined"),
        text
      )(_.fold(a => s"$a+-", b => s"-+$b", (a, b) => s"$a+$b")).toStream(Named("to-stream-8")).to(Named("write-8"), out, text, text.option)
    val running = task(joined, 0)
    val changes = List(("left", "a1", 1000L), ("right", "b1", 2000L), ("left", null, 1500L), ("right", null, 3000L))
    changes.foldLeftM((TaskState.empty, Vector.empty[TaskOutput])):
      case ((state, written), (topic, value, timestamp)) => running
          .process(state, TaskInput(bytes("k1"), Option(value).flatMap(bytes), timestamp, Headers.empty, topic), 0L)
          .map((after, more) => (after, written ++ more))
    .map: (state, written) =>
      val results = written.filter(_.topic == out).map(record => (record.value.map(v => new String(v.toArray)), record.timestamp))
      assertEquals(results, Vector((Some("a1+-"), 1000L), (Some("a1+b1"), 2000L), (Some("-+b1"), 2000L), (None, 3000L)))
      val logged = written.filter(_.topic.value == "app-joined-changelog").map(record => (record.value.isDefined, record.timestamp))
      assertEquals(logged, Vector((true, 1000L), (true, 2000L), (true, 2000L), (false, 3000L)))
      assertEquals(state.store("joined"), StoreContents.empty)
