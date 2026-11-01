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
import fs2.Chunk
import munit.CatsEffectSuite

/** Time windows, the checks `Topology.of` makes of them, and the windowed key's bytes, which `WindowGoldenSuite` holds against Java's on the JVM. */
final class WindowsSuite extends CatsEffectSuite:
  private val text  = Serde.utf8[IO]
  private val count = Serde.long[IO]
  private val topic = Topic.from("in").orFail

  test("a record falls in the one tumbling window it is in, its start included and its end not"):
    val windows = TimeWindows.tumbling(10.seconds, grace = 0.seconds)
    assertEquals(windows.windowsFor(0L), List(Window(0L, 10000L)))
    assertEquals(windows.windowsFor(9999L), List(Window(0L, 10000L)))
    assertEquals(windows.windowsFor(10000L), List(Window(10000L, 20000L)))

  test("a record falls in every hopping window that covers it, earliest first, and in none before time 0"):
    val windows = TimeWindows.hoppingWithNoGrace(10.seconds, advance = 3.seconds)
    assertEquals(windows.windowsFor(10000L), List(Window(3000L, 13000L), Window(6000L, 16000L), Window(9000L, 19000L)))
    assertEquals(windows.windowsFor(2000L), List(Window(0L, 10000L)))
    assertEquals(windows.windowsFor(-1L), Nil)

  private def windowed(windows: TimeWindows) =
    KStream(topic, text, text).groupByKey(text, text).windowedBy(windows).count(Store("counts")).toStream
      .to(topic, Serde.timeWindowed(text, 1.second), count)

  test("windows of no size, no advance, an advance past their size, negative grace, or parts of a millisecond are refused"):
    def refused(windows: TimeWindows) = Topology.of(windowed(windows)).left.toOption.map(_.toList)
    assertEquals(
      refused(TimeWindows.tumblingWithNoGrace(0.seconds)),
      Some(List(TopologyError.InvalidWindows("counts", "the size must be positive, was 0 seconds")))
    )
    assertEquals(
      refused(TimeWindows.hoppingWithNoGrace(10.seconds, advance = 0.seconds)),
      Some(List(TopologyError.InvalidWindows("counts", "the advance must be positive, was 0 seconds")))
    )
    assertEquals(
      refused(TimeWindows.hoppingWithNoGrace(10.seconds, advance = 11.seconds)),
      Some(List(TopologyError.InvalidWindows("counts", "the advance must not exceed the size, was 11 seconds for a size of 10 seconds")))
    )
    assertEquals(
      refused(TimeWindows.tumbling(10.seconds, grace = -1.second)),
      Some(List(TopologyError.InvalidWindows("counts", "the grace must not be negative, was -1 seconds")))
    )
    assertEquals(
      refused(TimeWindows.tumbling(10.seconds, grace = 1.microsecond)),
      Some(List(TopologyError.InvalidWindows("counts", "the size, advance and grace must be whole milliseconds")))
    )
    assert(Topology.of(windowed(TimeWindows.hopping(10.seconds, advance = 10.seconds, grace = 0.seconds))).isRight)

  test("a windowed key is written as its key and its window's start, and read back with the end its size gives"):
    val serde   = Serde.timeWindowed(text, 10.seconds)
    val written = Chunk.array("k".getBytes) ++ Chunk.array(Array[Byte](0, 0, 0, 0, 0, 0, 0x27, 0x10))
    for
      bytes <- serde.serializer.serialize(topic, Headers.empty, Windowed("k", Window(10000L, 20000L)))
      read  <- serde.deserializer.deserialize(topic, Headers.empty, Some(written))
      far   <-
        serde.deserializer.deserialize(
          topic,
          Headers.empty,
          Some(Chunk.array("k".getBytes) ++ Chunk.array(java.nio.ByteBuffer.allocate(8).putLong(Long.MaxValue - 5).array))
        )
      short <- serde.deserializer.deserialize(topic, Headers.empty, Some(Chunk.array(Array[Byte](1, 2, 3)))).attempt
    yield
      assertEquals(bytes, Some(written))
      assertEquals(read, Windowed("k", Window(10000L, 20000L)))
      assertEquals(far.window, Window(Long.MaxValue - 5, Long.MaxValue))
      assert(short.isLeft)

  test("a window store kept for less than its windows' size and grace, or for part of a millisecond, is refused"):
    def kept(retention: FiniteDuration) =
      Topology.of(
        KStream(topic, text, text).groupByKey(text, text).windowedBy(TimeWindows.tumbling(10.seconds, grace = 5.seconds)).withRetention(retention)
          .count(Store("counts")).toStream.to(topic, Serde.timeWindowed(text, 10.seconds), count)
      ).left.toOption.map(_.toList)
    assertEquals(
      kept(14999.millis),
      Some(
        List(TopologyError.InvalidWindows("counts", "the retention must be at least the windows' size and grace, 15000 ms, was 14999 milliseconds"))
      )
    )
    assertEquals(kept(15001001.micros), Some(List(TopologyError.InvalidWindows("counts", "the retention must be whole milliseconds"))))
    assertEquals(kept(15.seconds), None)

  test("sliding windows of a negative time difference, negative grace, parts of a millisecond, or kept too briefly are refused, and of none are not"):
    def refused(windows: SlidingWindows, retention: Option[FiniteDuration] = None) =
      val windowed = KStream(topic, text, text).groupByKey(text, text).windowedBy(windows)
      Topology
        .of(retention.fold(windowed)(windowed.withRetention).count(Store("counts")).toStream.to(topic, Serde.timeWindowed(text, 1.second), count))
        .left.toOption.map(_.toList)
    assertEquals(
      refused(SlidingWindows.withNoGrace(-1.second)),
      Some(List(TopologyError.InvalidWindows("counts", "the time difference must not be negative, was -1 seconds")))
    )
    assertEquals(
      refused(SlidingWindows(1.second, grace = -1.second)),
      Some(List(TopologyError.InvalidWindows("counts", "the grace must not be negative, was -1 seconds")))
    )
    assertEquals(
      refused(SlidingWindows(1.second, grace = 1.microsecond)),
      Some(List(TopologyError.InvalidWindows("counts", "the time difference and grace must be whole milliseconds")))
    )
    assertEquals(
      refused(SlidingWindows(10.seconds, grace = 5.seconds), Some(24999.millis)),
      Some(List(TopologyError.InvalidWindows(
        "counts",
        "the retention must be at least twice the windows' time difference and their grace, 25000 ms, was 24999 milliseconds"
      )))
    )
    assertEquals(refused(SlidingWindows.withNoGrace(0.seconds)), None)
