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

import scala.util.Random

import fs2.Chunk
import munit.FunSuite
import org.apache.kafka.streams.state.internals.Murmur3 as JavaMurmur3

/** Our hash against Java's, for every length around its blocks of sixteen bytes. */
final class Murmur3GoldenSuite extends FunSuite:
  test("the hash is Java's for every length up to two blocks and beyond, on bytes of every value"):
    val random = Random(1071L)
    (0 to 70).foreach: length =>
      (1 to 20).foreach: _ =>
        val bytes       = random.nextBytes(length)
        val Array(a, b) = JavaMurmur3.hash128(bytes)
        assertEquals(Murmur3.hash128(Chunk.array(bytes)), (a, b), s"length $length")
