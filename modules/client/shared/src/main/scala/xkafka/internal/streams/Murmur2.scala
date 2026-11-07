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

/** The partition the Java producer picks for a key, from Java's `Utils.murmur2`, made positive and taken modulo the partition count, as
  * `BuiltInPartitioner.partitionForKey` does.
  */
private[xkafka] object Murmur2:
  /** Java's `Utils.murmur2` of `bytes`. */
  def hash(bytes: Chunk[Byte]): Int =
    val data          = bytes.toArray
    val m             = 0x5bd1e995
    val words         = data.length / 4
    def byte(at: Int) = data(at) & 0xff
    val mixed         = (0 until words).foldLeft(0x9747b28c ^ data.length): (h, i) =>
      val at = i * 4
      val k  = (byte(at) + (byte(at + 1) << 8) + (byte(at + 2) << 16) + (byte(at + 3) << 24)) * m
      (h * m) ^ ((k ^ (k >>> 24)) * m)
    val tail = data.length & ~3
    val last =
      data.length % 4 match
        case 3 => (mixed ^ (byte(tail + 2) << 16) ^ (byte(tail + 1) << 8) ^ byte(tail)) * m
        case 2 => (mixed ^ (byte(tail + 1) << 8) ^ byte(tail)) * m
        case 1 => (mixed ^ byte(tail)) * m
        case _ => mixed
    val h = (last ^ (last >>> 13)) * m
    h ^ (h >>> 15)

  /** The partition of `partitions` the Java producer picks for a key written as `bytes`. */
  def partition(bytes: Chunk[Byte], partitions: Int): Int = (hash(bytes) & 0x7fffffff) % partitions
