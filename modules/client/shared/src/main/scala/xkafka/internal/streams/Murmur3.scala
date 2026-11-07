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

import java.lang.Long.rotateLeft

import fs2.Chunk

/** The 128-bit hash Java's foreign-key joins compare a table's values by, from Kafka Streams' `Murmur3.hash128`. */
private[xkafka] object Murmur3:
  private val C1   = 0x87c37b91114253d5L
  private val C2   = 0x4cf5ad432745937fL
  private val N1   = 0x52dce729L
  private val N2   = 0x38495ab5L
  private val Seed = 104729L

  private def first(k: Long): Long  = rotateLeft(k * C1, 31) * C2
  private def second(k: Long): Long = rotateLeft(k * C2, 33) * C1

  private def finished(h: Long): Long =
    val a = (h ^ (h >>> 33)) * 0xff51afd7ed558ccdL
    val b = (a ^ (a >>> 33)) * 0xc4ceb9fe1a85ec53L
    b ^ (b >>> 33)

  /** Java's `Murmur3.hash128` of `bytes`, with its default seed. */
  def hash128(bytes: Chunk[Byte]): (Long, Long) =
    val data                      = bytes.toArray
    def word(at: Int, count: Int) = (0 until count).foldLeft(0L)((word, i) => word | ((data(at + i) & 0xffL) << (8 * i)))
    val blocks                    = data.length >> 4
    val (bodyFirst, bodySecond)   = (0 until blocks).foldLeft((Seed, Seed)):
      case ((h1, h2), block) =>
        val at     = block << 4
        val mixed1 = (rotateLeft(h1 ^ first(word(at, 8)), 27) + h2) * 5 + N1
        val mixed2 = (rotateLeft(h2 ^ second(word(at + 8, 8)), 31) + mixed1) * 5 + N2
        (mixed1, mixed2)
    val tail       = blocks << 4
    val left       = data.length - tail
    val tailFirst  = if left > 0 then bodyFirst ^ first(word(tail, left.min(8))) else bodyFirst
    val tailSecond = if left > 8 then bodySecond ^ second(word(tail + 8, left - 8)) else bodySecond
    val h1         = tailFirst ^ data.length
    val h2         = tailSecond ^ data.length
    val a1         = finished(h1 + h2)
    val a2         = finished(h2 + (h1 + h2))
    (a1 + a2, a2 + (a1 + a2))
