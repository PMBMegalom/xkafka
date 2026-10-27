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

import cats.effect.IO
import fs2.Chunk
import munit.CatsEffectSuite
import xkafka.streams.{KStream, Pipeline, Serde, Store, Topology}

/** What a task writes to its changelogs and keeps between records, on every platform. */
final class TaskSuite extends CatsEffectSuite:
  private val text  = Serde.utf8[IO]
  private val count = Serde.long[IO]
  private val in    = Topic.from("in").orFail
  private val out   = Topic.from("out").orFail

  private def bytes(text: String): Option[Chunk[Byte]] = Some(Chunk.array(text.getBytes))
  private def long(n: Long): Option[Chunk[Byte]]       = Some(Chunk.array(java.nio.ByteBuffer.allocate(8).putLong(n).array))
  private val trace                                    = Headers(Header("trace", bytes("1")))

  private def task(pipeline: Pipeline[IO], partition: Int): Task[IO] =
    Task("app", Topology.of(pipeline).orFail.pipelines.head.segments.head, partition)

  private val counting = KStream(in, text, text).groupByKey(text, text).count(Store("counts")).toStream.to(out, text, count)

  test("each change to a store goes to its changelog on the task's own partition, without the record's headers"):
    task(counting, 3).process(TaskState.empty, TaskInput(bytes("k"), bytes("v"), 1000L, trace)).map: (_, written) =>
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
      first  <- counted.process(TaskState.empty, TaskInput(bytes("k"), bytes("v"), 3000L, Headers.empty))
      second <- counted.process(first._1, TaskInput(bytes("k"), bytes("v"), 2000L, Headers.empty))
    yield
      assertEquals((first._1.partitionTime, second._1.partitionTime), (3000L, 3000L))
      assertEquals(second._1.store("counts").get(Chunk.array("k".getBytes)), Some(Stored(long(2).get, 3000L)))

  test("a value its serializer writes as none removes the key from the store and writes a tombstone to the changelog"):
    val emptyIsNone =
      Serde[IO, String](Serializer.instance((t, h, v) => if v.isEmpty then IO.pure(None) else text.serializer.serialize(t, h, v)), text.deserializer)
    val latest =
      KStream(in, text, text).groupByKey(text, text).aggregate(Store("latest"), emptyIsNone)("")((_, v, _) => v).toStream.to(out, text, emptyIsNone)
    val running = task(latest, 0)
    for
      first  <- running.process(TaskState.empty, TaskInput(bytes("k"), bytes("v"), 1000L, Headers.empty))
      second <- running.process(first._1, TaskInput(bytes("k"), bytes(""), 2000L, Headers.empty))
    yield
      assertEquals(first._1.store("latest").keySet, Set(Chunk.array("k".getBytes)))
      assertEquals(second._1.store("latest"), Map.empty)
      assertEquals(second._2.head, TaskOutput(Topic.from("app-latest-changelog").orFail, Some(0), bytes("k"), None, 2000L, Headers.empty))

  test("a record whose key its serializer writes as none is not counted, as Java skips a record with a null key"):
    val dashIsNone =
      Serde[IO, String](Serializer.instance((t, h, k) => if k == "-" then IO.pure(None) else text.serializer.serialize(t, h, k)), text.deserializer)
    val counted = task(KStream(in, text, text).groupByKey(dashIsNone, text).count(Store("counts")).toStream.to(out, text, count), 0)
    counted.process(TaskState.empty, TaskInput(bytes("-"), bytes("v"), 1000L, Headers.empty)).map: (state, written) =>
      assertEquals((state.store("counts"), written), (Map.empty, Vector.empty))
