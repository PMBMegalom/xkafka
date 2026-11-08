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

import driver.PolledRecord
import fs2.Chunk
import cats.effect.IO
import cats.syntax.all.*
import munit.CatsEffectSuite

/** A window store's keys, clock and expiry, as Java's in-memory window store keeps them. */
final class WindowStoreSuite extends CatsEffectSuite:
  private def stored(key: String, start: Long) = WindowStore.key(Chunk.array(key.getBytes), start) -> Stored(Chunk.empty, start)

  test("a window's key is the key, the window's start, and a sequence number of 0"):
    val key = WindowStore.key(Chunk.array("k".getBytes), 10000L)
    assertEquals(key.toVector, "k".getBytes.toVector ++ Vector[Byte](0, 0, 0, 0, 0, 0, 0x27, 0x10, 0, 0, 0, 0))
    assertEquals(WindowStore.startOf(key), 10000L)

  private val windowTime: TaskStore.TimeOf  = WindowStore.startOf
  private val sessionTime: TaskStore.TimeOf = SessionStore.endOf

  private def opened(time: TaskStore.TimeOf, entries: (Chunk[Byte], Stored)*): IO[TaskStore[IO]] =
    TaskStore.inMemory[IO](StoreContents.of(entries.toMap), Some(time))

  private def removal(key: Chunk[Byte]) = PolledRecord("c", 0, 0L, Some(1L), 0, 0, Some(key), None, Headers.empty)

  test("a store's clock is its latest window start, or none for an empty store"):
    (opened(windowTime).flatMap(_.clock), opened(windowTime, stored("a", 5000L), stored("b", 20000L), stored("a", 10000L)).flatMap(_.clock))
      .mapN((empty, held) => assertEquals((empty, held), (WindowStore.NoTime, 20000L)))

  test("a removal restored counts towards a window or session store's clock, whatever happens to what it holds after"):
    val window  = WindowStore.key(Chunk.array("a".getBytes), 50000L)
    val session = SessionStore.key(Chunk.array("a".getBytes), 1000L, 60000L)
    for
      windows  <- opened(windowTime, stored("a", 5000L))
      _        <- Restore.written(Map("w" -> windows), Restore.changes(List("w" -> removal(window))))
      clock    <- windows.clock
      _        <- WindowStore.swept(Map("w" -> windows), Map("w" -> 15000L))
      left     <- windows.all
      after    <- windows.clock
      sessions <- opened(sessionTime)
      _        <- Restore.written(Map("s" -> sessions), Restore.changes(List("s" -> removal(session))))
      ended    <- sessions.clock
    yield
      assertEquals(clock, 50000L)
      assertEquals(left, Vector.empty)
      assertEquals(after, 50000L)
      assertEquals(ended, 60000L)

  test("a window expires once the clock is a whole retention past its start, and a store with no retention named loses nothing"):
    val entries = List(stored("a", 4999L), stored("a", 5000L), stored("a", 5001L), stored("b", 20000L))
    for
      windows <- opened(windowTime, entries*)
      counts  <- opened(windowTime, entries*)
      _       <- WindowStore.swept(Map("windows" -> windows, "counts" -> counts), Map("windows" -> 15000L))
      kept    <- windows.all
      all     <- counts.all
    yield
      assertEquals(kept.map(_._2.timestamp).toSet, Set(5001L, 20000L))
      assertEquals(all.size, 4)

  test("a window changelog keeps a window for the store's retention and a day, as Java declares it, and no longer than Java can say"):
    assertEquals(
      WindowStore.changelogConfigs(15000L),
      List("cleanup.policy" -> "compact,delete", "message.timestamp.type" -> "CreateTime", "retention.ms" -> "86415000")
    )
    assertEquals(WindowStore.changelogConfigs(Long.MaxValue - 1).last, "retention.ms" -> Long.MaxValue.toString)

  test("a session's key is the key, its end and its start, a store's clock is its latest session end, and sessions expire by their end"):
    val key = SessionStore.key(Chunk.array("k".getBytes), 1000L, 10000L)
    assertEquals(key.toVector, "k".getBytes.toVector ++ Vector[Byte](0, 0, 0, 0, 0, 0, 0x27, 0x10, 0, 0, 0, 0, 0, 0, 0x03, 0xe8.toByte))
    assertEquals((SessionStore.startOf(key), SessionStore.endOf(key)), (1000L, 10000L))
    val entries =
      StoreContents.of(Map(
        SessionStore.key(Chunk.array("a".getBytes), 0L, 5000L)      -> Stored(Chunk.empty, 1L),
        key                                                         -> Stored(Chunk.empty, 2L),
        SessionStore.key(Chunk.array("b".getBytes), 20000L, 20000L) -> Stored(Chunk.empty, 3L)
      ))
    opened(sessionTime, entries.toVector*).flatMap: sessions =>
      (sessions.clock, WindowStore.swept(Map("s" -> sessions), Map("s" -> 10000L)) *> sessions.all).mapN: (clock, kept) =>
        assertEquals(clock, 20000L)
        assertEquals(kept.map(_._2.timestamp).toSet, Set(3L))
