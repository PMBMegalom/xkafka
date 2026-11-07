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

/** A store's contents in the order a store on disk keeps them, on every platform. */
final class StoreContentsSuite extends FunSuite:
  private def key(bytes: Int*) = Chunk.from(bytes.map(_.toByte))

  private def held(value: Int) = Stored(key(value), value.toLong)

  private def of(keys: Chunk[Byte]*) = StoreContents.of(keys.zipWithIndex.map((k, n) => k -> held(n)))

  test("keys go in the order of their bytes taken as unsigned, a key before a longer one it starts"):
    val ordered = List(key(), key(0), key(0, 0), key(1), key(1, 0), key(0x7f), key(0x80), key(0xff), key(0xff, 0))
    assertEquals(of(ordered.reverse*).toVector.map(_._1).toList, ordered)

  test("a prefix finds its keys in order, and only those, wherever they are"):
    val contents = of(key(1), key(1, 0xff), key(1, 0), key(1, 0x80), key(2), key(0, 0xff), key(0xff), key(0xff, 1))
    assertEquals(contents.prefixed(key(1)).map(_._1).toList, List(key(1), key(1, 0), key(1, 0x80), key(1, 0xff)))
    assertEquals(contents.prefixed(key(0xff)).map(_._1).toList, List(key(0xff), key(0xff, 1)))
    assertEquals(contents.prefixed(key(3)).map(_._1).toList, Nil)
    assertEquals(contents.prefixed(key()).size, 8)
    assertEquals(contents.prefixed(key(1, 0x80, 0)).map(_._1).toList, Nil)

  test("a key shorter than the prefix that sorts after it ends the scan"):
    assertEquals(of(key(5), key(5, 1, 2), key(6)).prefixed(key(5, 1)).map(_._1).toList, List(key(5, 1, 2)))

  test("what a key holds is replaced and removed, and contents with the same entries are equal however they were made"):
    val contents = of(key(1)).updated(key(2), held(9)).updated(key(1), held(8))
    assertEquals(contents.get(key(1)), Some(held(8)))
    assertEquals((contents - key(1)).get(key(1)), None)
    assertEquals((contents -- List(key(1), key(2))), StoreContents.empty)
    assertEquals(contents, StoreContents.of(List(key(2) -> held(9), key(1) -> held(8))))
