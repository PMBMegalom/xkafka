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
    assertEquals(WindowStore.clockOf(Map.empty), WindowStore.NoTime)
    assertEquals(WindowStore.clockOf(Map(stored("a", 5000L), stored("b", 20000L), stored("a", 10000L))), 20000L)

  test("a window expires once the clock is a whole retention past its start"):
    val entries = Map(stored("a", 4999L), stored("a", 5000L), stored("a", 5001L))
    assertEquals(WindowStore.pruned(entries, clock = 20000L, retention = 15000L).values.map(_.timestamp).toSet, Set(5001L))
    assertEquals(
      WindowStore.prunedAll(Map("windows" -> entries, "counts" -> entries), Map("windows" -> 15000L)).view.mapValues(_.size).toMap,
      Map("windows" -> 3, "counts" -> 3)
    )

  test("restored stores lose the windows their own clock has expired, and other stores nothing"):
    val entries = Map(stored("a", 0L), stored("a", 15000L), stored("b", 30000L))
    assertEquals(WindowStore.prunedAll(Map("windows" -> entries), Map("windows" -> 15000L))("windows").values.map(_.timestamp).toSet, Set(30000L))

  test("a window changelog keeps a window for the store's retention and a day, as Java declares it, and no longer than Java can say"):
    assertEquals(
      WindowStore.changelogConfigs(15000L),
      List("cleanup.policy" -> "compact,delete", "message.timestamp.type" -> "CreateTime", "retention.ms" -> "86415000")
    )
    assertEquals(WindowStore.changelogConfigs(Long.MaxValue - 1).last, "retention.ms" -> Long.MaxValue.toString)
