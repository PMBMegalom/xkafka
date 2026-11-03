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

import fs2.Chunk

/** Values in the order a `java.util.HashSet` holding them iterates, having been added in the order given: by bucket, from each value's `hash` spread
  * as `HashMap` spreads it, in a table of 16 buckets that doubles whenever it holds more than three quarters of its size, or a bucket of a table
  * under 64 would hold more than 8, and never shrinks; within a bucket, in the order the values were first added, except in a bucket of more than 8
  * in a larger table, which Java keeps as a tree and orders otherwise.
  */
private[xkafka] final case class JavaHashSet[A](keys: Vector[A], members: Set[A], capacity: Int, hash: A => Int):
  def +(key: A): JavaHashSet[A] =
    if members(key) then this
    else
      val bucket  = JavaHashSet.spread(hash(key)) % capacity
      val crowded = capacity < 64 && keys.count(k => JavaHashSet.spread(hash(k)) % capacity == bucket) >= 8
      val grown   = if crowded then capacity * 2 else capacity
      JavaHashSet(keys :+ key, members + key, if members.size + 1 > grown / 4 * 3 then grown * 2 else grown, hash)

  /** Whether two of its values share a bucket, whose order Java takes from how they were added, which may not be the order they were added here. */
  def crowded: Boolean = keys.groupBy(key => JavaHashSet.spread(hash(key)) & (capacity - 1)).exists(_._2.size > 1)

  /** The values in the order Java iterates them. */
  def ordered: Vector[A] = keys.zipWithIndex.sortBy((key, added) => (JavaHashSet.spread(hash(key)) & (capacity - 1), added)).map(_._1)

  /** No values, in a table of the same size, as `HashSet.clear` leaves it. */
  def cleared: JavaHashSet[A] = JavaHashSet(Vector.empty, Set.empty, capacity, hash)

private[xkafka] object JavaHashSet:
  def empty[A](hash: A => Int): JavaHashSet[A] = JavaHashSet(Vector.empty, Set.empty, 16, hash)

  /** A hash spread as `HashMap.hash` spreads it, as a non-negative number. */
  def spread(h: Int): Int = (h ^ (h >>> 16)) & Int.MaxValue

  /** `Arrays.hashCode` of bytes, as Java's `Bytes` hashes them. */
  def bytes(key: Chunk[Byte]): Int = key.foldLeft(1)((h, b) => 31 * h + b)

  /** The values `values` adds, in the order a `HashSet` Java fills with them iterates. */
  def ordered[A](values: Iterable[A], hash: A => Int): Vector[A] = values.foldLeft(empty(hash))(_ + _).ordered
