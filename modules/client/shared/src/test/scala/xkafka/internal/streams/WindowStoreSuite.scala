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
import munit.FunSuite

/** A window store's keys, clock and expiry, as Java's in-memory window store keeps them. */
final class WindowStoreSuite extends FunSuite:
  private def stored(key: String, start: Long) = WindowStore.key(Chunk.array(key.getBytes), start) -> Stored(Chunk.empty, start)

  test("a window's key is the key, the window's start, and a sequence number of 0"):
    val key = WindowStore.key(Chunk.array("k".getBytes), 10000L)
    assertEquals(key.toVector, "k".getBytes.toVector ++ Vector[Byte](0, 0, 0, 0, 0, 0, 0x27, 0x10, 0, 0, 0, 0))
    assertEquals(WindowStore.startOf(key), 10000L)

  test("a store's clock is its latest window start, or none for an empty store"):
    assertEquals(WindowStore.clockOf(StoreContents.empty), WindowStore.NoTime)
    assertEquals(WindowStore.clockOf(StoreContents.of(Map(stored("a", 5000L), stored("b", 20000L), stored("a", 10000L)))), 20000L)

  test("a removal counts towards a window or session store's clock, whatever happens to what it holds after, and towards no other store's"):
    val window  = WindowStore.key(Chunk.array("a".getBytes), 50000L)
    val session = SessionStore.key(Chunk.array("a".getBytes), 1000L, 60000L)
    val older   = Map(stored("a", 5000L))
    val times   = Map("w" -> ((key: Chunk[Byte]) => WindowStore.startOf(key)), "s" -> ((key: Chunk[Byte]) => SessionStore.endOf(key)))
    def removing(store: String, key: Chunk[Byte], held: StoreContents) =
      Restore.applied(Map(store -> held), store, PolledRecord("c", 0, 0L, Some(1L), 0, 0, Some(key), None, Headers.empty), times)(store)
    val windows = removing("w", window, StoreContents.of(older))
    assertEquals(WindowStore.clockOf(windows), 50000L)
    assertEquals(WindowStore.prunedAll(Map("w" -> windows), Map("w" -> 15000L))("w").size, 0)
    assertEquals(WindowStore.clockOf(WindowStore.prunedAll(Map("w" -> windows), Map("w" -> 15000L))("w")), 50000L)
    assertEquals(WindowStore.clockOf(windows.updated(window, Stored(Chunk.empty, 1L)).after(Map(window -> None))), 50000L)
    assertEquals(SessionStore.clockOf(removing("s", session, StoreContents.empty)), 60000L)
    assertEquals(removing("kv", window, StoreContents.of(older)).latestRemoved, WindowStore.NoTime)

  test("a window expires once the clock is a whole retention past its start"):
    val entries = StoreContents.of(Map(stored("a", 4999L), stored("a", 5000L), stored("a", 5001L)))
    assertEquals(WindowStore.pruned(entries, clock = 20000L, retention = 15000L).values.map(_.timestamp).toSet, Set(5001L))
    assertEquals(
      WindowStore.prunedAll(Map("windows" -> entries, "counts" -> entries), Map("windows" -> 15000L)).view.mapValues(_.size).toMap,
      Map("windows" -> 3, "counts" -> 3)
    )

  test("restored stores lose the windows their own clock has expired, and other stores nothing"):
    val entries = StoreContents.of(Map(stored("a", 0L), stored("a", 15000L), stored("b", 30000L)))
    assertEquals(WindowStore.prunedAll(Map("windows" -> entries), Map("windows" -> 15000L))("windows").values.map(_.timestamp).toSet, Set(30000L))

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
    assertEquals(SessionStore.clockOf(entries), 20000L)
    assertEquals(SessionStore.pruned(entries, 20000L, 10000L).values.map(_.timestamp).toSet, Set(3L))
    assertEquals(WindowStore.prunedAll(Map("s" -> entries), Map("s" -> 10000L), Set("s"))("s").values.map(_.timestamp).toSet, Set(3L))
