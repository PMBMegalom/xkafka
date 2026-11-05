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

import cats.effect.IO
import cats.syntax.all.*
import fs2.Chunk
import munit.CatsEffectSuite
import internal.streams.{Task, TaskInput, TaskState}

/** The parts of a cogroup that do not need Java to compare with, on every platform: what tasks make of records, and which programs are refused. */
final class CogroupSuite extends CatsEffectSuite:
  private val text = Serde.utf8[IO]

  private def topic(name: String) = Topic.from(name).orFail

  private def group(name: String) = KStream(Named(name), topic(name), text, text).groupByKey(text, text)

  private val add: (String, String, String) => String = (_, v, total) => s"$total+$v"
  private val mul: (String, String, String) => String = (_, v, total) => s"$total*$v"

  private def two = group("a").cogroup(add).cogroup(group("b"))(mul)

  private def refused(pipeline: Pipeline[IO]) = Topology.of(pipeline).left.toOption.fold(Nil)(_.toList)

  private def written(pipeline: Pipeline[IO], inputs: List[(String, String, Long)]): IO[Vector[(String, Long)]] =
    val topology = Topology.of(pipeline).orFail
    val task     = Task[IO]("app", topology.subtopologies.head, 0)
    inputs.foldLeftM((TaskState.empty, Vector.empty[(String, Long)])):
      case ((state, out), (name, value, at)) =>
        val input = TaskInput(Some(Chunk.array("k".getBytes)), Some(Chunk.array(value.getBytes)), at, Headers.empty, name)
        task.process(state, input, 0L).map: (after, records) =>
          (after, out ++ records.filter(_.topic.value == "out").map(r => (new String(r.value.get.toArray), r.timestamp)))
    .map(_._2)

  test("two groups fold into one store, each by its own function, the time being the latest of the values folded"):
    val program = two.aggregate(Named("co"), Store("kept"), text)("s").toStream(Named("c")).to(Named("w"), topic("out"), text, text)
    written(program, List(("a", "1", 1000L), ("b", "2", 3000L), ("a", "3", 2000L)))
      .assertEquals(Vector(("s+1", 1000L), ("s+1*2", 3000L), ("s+1*2+3", 3000L)))

  test("a record is late by the latest time read from its own group, in time windows, sliding windows and sessions"):
    val timed =
      two.windowedBy(TimeWindows.tumbling(10.seconds, grace = 0.seconds)).withRetention(5.minutes).aggregate(Named("co"), Store("kept"), text)("s")
        .toStream(Named("c")).mapValues(Named("v"))(identity).selectKey(Named("k"))((windowed, _) => windowed.key)
        .to(Named("w"), topic("out"), text, text)
    val sliding =
      two.windowedBy(SlidingWindows(10.seconds, grace = 0.seconds)).withRetention(5.minutes).aggregate(Named("co"), Store("kept"), text)("s")
        .toStream(Named("c")).selectKey(Named("k"))((windowed, _) => windowed.key).to(Named("w"), topic("out"), text, text)
    val sessions =
      two.windowedBy(SessionWindows(1.second, grace = 0.seconds)).withRetention(5.minutes)
        .aggregate(Named("co"), Store("kept"), text)("s")((_, x, y) => x + y).toStream(Named("c"))
        .selectKey(Named("k"))((windowed, _) => windowed.key).to(Named("w"), topic("out"), text, text.option)
    val records                               = List(("a", "1", 100000L), ("b", "2", 1000L), ("a", "3", 1000L))
    def values(found: Vector[(String, Long)]) = found.map(_._1)
    written(timed, records).map(values).assertEquals(Vector("s+1", "s*2")) *> written(sliding, records).map(values)
      .assertEquals(Vector("s+1", "s*2")) *> written(sessions, records).map(values).assertEquals(Vector("s+1", "s*2"))

  test("two steps cannot share a cogroup's store or the names of its steps, and a store taken by another step is refused"):
    def to(table: KTable.Flowing[IO, String, String, KTable.Updated, Pipeline[IO]], last: String) =
      table.toStream(Named(last)).to(Named("w"), topic("out"), text, text)
    val good = to(two.aggregate(Named("co"), Store("kept"), text)("s"), "c")
    assertEquals(refused(good), Nil)
    assertEquals(
      refused(to(two.aggregate(Named("co"), Store("kept"), text)("s"), "co-cogroup-merge")),
      List(TopologyError.DuplicateName("co-cogroup-merge"))
    )
    assertEquals(
      refused(to(two.aggregate(Named("co"), Store("kept"), text)("s"), "co-cogroup-agg-1")),
      List(TopologyError.DuplicateName("co-cogroup-agg-1"))
    )
    val other =
      KStream(Named("x"), topic("x"), text, text).groupByKey(text, text).count(Named("n"), Store("kept")).toStream(Named("m"))
        .to(Named("y"), topic("z"), text, Serde.long[IO])
    assert(Topology.of(good, other).left.toOption.exists(_.toList.contains(TopologyError.DuplicateStore("kept"))))

  test("a group read twice is one topic read twice, which is refused"):
    val same = group("a")
    assert(
      refused(
        same.cogroup(add).cogroup(same)(mul).aggregate(Named("co"), Store("kept"), text)("s").toStream(Named("c"))
          .to(Named("w"), topic("out"), text, text)
      ).nonEmpty
    )

  test("windows a cogroup cannot use are refused once, however many groups it has"):
    val negative = TimeWindows.tumbling(-1.second, grace = 0.seconds)
    assertEquals(
      refused(
        two.windowedBy(negative).aggregate(Named("co"), Store("kept"), text)("s").toStream(Named("c"))
          .to(Named("w"), topic("out"), Serde.timeWindowed(text, 1.second), text)
      ).size,
      1
    )
    val short = two.windowedBy(TimeWindows.tumbling(10.seconds, grace = 0.seconds)).withRetention(1.second)
    assertEquals(
      refused(
        short.aggregate(Named("co"), Store("kept"), text)("s").toStream(Named("c"))
          .to(Named("w"), topic("out"), Serde.timeWindowed(text, 10.seconds), text)
      ).size,
      1
    )
    val shortSession = two.windowedBy(SessionWindows(10.seconds, grace = 0.seconds)).withRetention(1.second)
    assertEquals(
      refused(
        shortSession.aggregate(Named("co"), Store("kept"), text)("s")((_, x, y) => x + y).toStream(Named("c"))
          .to(Named("w"), topic("out"), Serde.sessionWindowed(text), text.option)
      ).size,
      1
    )

  test("a stream that does not say how it is keyed needs a repartition to cogroup, which is refused if its key did not change"):
    val read: KStream.Unkeyed[IO, String, String] = KStream(Named("a"), topic("a"), text, text)
    val program                                   =
      read.groupByKey(Repartition("r"), text, text).cogroup(add).aggregate(Named("co"), Store("kept"), text)("s").toStream(Named("c"))
        .to(Named("w"), topic("out"), text, text)
    assertEquals(refused(program), List(TopologyError.UnusedRepartition("r")))

  test("a mapped program keeps each group's own time and one store"):
    val program =
      two.windowedBy(TimeWindows.tumbling(10.seconds, grace = 0.seconds)).withRetention(5.minutes).aggregate(Named("co"), Store("kept"), text)("s")
        .toStream(Named("c")).selectKey(Named("k"))((windowed, _) => windowed.key).to(Named("w"), topic("out"), text, text)
    val mapped = Topology.of(program).orFail.mapK(cats.arrow.FunctionK.id[IO])
    assertEquals(mapped.subtopologies.flatMap(_.stores), List(Store("kept")))
    val task    = Task[IO]("app", mapped.subtopologies.head, 0)
    val records = List(("a", "1", 100000L), ("b", "2", 1000L), ("a", "3", 1000L))
    records.foldLeftM((TaskState.empty, Vector.empty[String])):
      case ((state, out), (name, value, at)) => task
          .process(state, TaskInput(Some(Chunk.array("k".getBytes)), Some(Chunk.array(value.getBytes)), at, Headers.empty, name), 0L).map:
            (after, written) => (after, out ++ written.filter(_.topic.value == "out").map(r => new String(r.value.get.toArray)))
    .map(_._2).assertEquals(Vector("s+1", "s*2"))

  test("what follows a cogroup is keyed as it was folded, so a repartition asked for after it is refused"):
    def after(first: KGroupedStream[IO, String, String, Pipeline[IO]]) =
      val changes: KStream.Unkeyed[IO, String, String] = first.cogroup(add).aggregate(Named("co"), Store("kept"), text)("s").toStream(Named("c"))
      changes.groupByKey(Repartition("again"), text, text).count(Named("n"), Store("counts")).toStream(Named("d"))
        .to(Named("w"), topic("out"), text, Serde.long[IO])
    val moved = KStream(Named("a"), topic("a"), text, text).selectKey(Named("new"))((_, v) => v).groupByKey(Repartition("r"), text, text)
    assertEquals(refused(after(moved)), List(TopologyError.UnusedRepartition("again")))

  test("the types say a cogroup's groups share a key, and a program's groups cannot go into a flow's"):
    compileErrors("""two.cogroup(KStream(Named("c"), topic("c"), Serde.long[IO], text).groupByKey(Serde.long[IO], text))((_, v, t) => t)""").nonEmpty
