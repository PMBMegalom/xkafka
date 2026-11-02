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

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8

import scala.collection.immutable.SortedSet
import scala.util.Try

import cats.syntax.all.*
import fs2.Chunk

/** Where a change held in a suppression's buffer came from, as Java's `ProcessorRecordContext`: the time and headers it had, and the topic, partition
  * and offset of the record that made it.
  */
private[xkafka] final case class HeldContext(timestamp: Long, offset: Long, topic: String, partition: Int, headers: Headers):
  /** As Java writes it: the timestamp and the offset, the topic's length and UTF-8 bytes, the partition, the number of headers, then each header's
    * key's length and bytes and its value's length and bytes, or -1 for no value.
    */
  def encode: Chunk[Byte] =
    val topicBytes = topic.getBytes(UTF_8)
    val entries    = headers.values.map(header => (header.key.getBytes(UTF_8), header.value.map(_.toArray)))
    val size       = 8 + 8 + 4 + topicBytes.length + 4 + 4 + entries.map((k, v) => 8 + k.length + v.fold(0)(_.length)).sum
    val buffer     = ByteBuffer.allocate(size).putLong(timestamp).putLong(offset).putInt(topicBytes.length).put(topicBytes).putInt(partition)
    buffer.putInt(entries.size)
    entries.foreach: (k, v) =>
      buffer.putInt(k.length).put(k)
      v.fold(buffer.putInt(-1))(bytes => buffer.putInt(bytes.length).put(bytes))
    Chunk.array(buffer.array)

  /** Java's `residentMemorySizeEstimate`: the timestamp, the offset, the topic in characters, the partition, and each header's key in characters and
    * value in bytes.
    */
  def size: Long = 8L + 8L + topic.length + 4L + headers.values.map(header => header.key.length.toLong + header.value.fold(0L)(_.size.toLong)).sum

/** A change held in a suppression's buffer, as Java's `BufferValue` with the time it is held until: the key's value before it was first held, the
  * change's old and new values, each as written or none, and where it came from.
  */
private[xkafka] final case class Held(
    time: Long,
    context: HeldContext,
    prior: Option[Chunk[Byte]],
    old: Option[Chunk[Byte]],
    value: Option[Chunk[Byte]]
):
  /** As Java's buffer logs it, in its version 3: the context, the prior value, the old value, or -2 where it is the prior one, the new value, each
    * with its length or -1 for none, then the time.
    */
  def encode: Chunk[Byte] =
    def part(bytes: Option[Chunk[Byte]]) =
      bytes.fold(Chunk.array(ByteBuffer.allocate(4).putInt(-1).array))(b => Chunk.array(ByteBuffer.allocate(4).putInt(b.size).array) ++ b)
    val oldPart =
      old match
        case None                        => Chunk.array(ByteBuffer.allocate(4).putInt(-1).array)
        case Some(bytes) if prior == old => Chunk.array(ByteBuffer.allocate(4).putInt(-2).array)
        case Some(bytes)                 => part(Some(bytes))
    context.encode ++ part(prior) ++ oldPart ++ part(value) ++ Chunk.array(ByteBuffer.allocate(8).putLong(time).array)

  /** Java's `computeRecordSize` for this change under `key`: the time, the key, the values, the old one only where it differs from the prior one,
    * which Java then holds as one, and the context.
    */
  def size(key: Chunk[Byte]): Long =
    val olds = if prior == old then 0L else old.fold(0L)(_.size.toLong)
    8L + key.size + prior.fold(0L)(_.size.toLong) + olds + value.fold(0L)(_.size.toLong) + context.size

private[xkafka] object Held:
  /** The header Java's buffer logs each change with, its format's version. */
  val Version: Headers = Headers(Header("v", Some(Chunk.array(Array[Byte](3)))))

  /** A change as [[Held.encode]] writes it, or none where it is not one. */
  def decode(bytes: Chunk[Byte]): Option[Held] =
    Try:
      val buffer             = ByteBuffer.wrap(bytes.toArray)
      def array(length: Int) =
        val read = new Array[Byte](length); buffer.get(read); read
      def part(): Option[Chunk[Byte]] =
        buffer.getInt match
          case -1     => None
          case length => Some(Chunk.array(array(length)))
      val timestamp = buffer.getLong
      val offset    = buffer.getLong
      val topic     = String(array(buffer.getInt), UTF_8)
      val partition = buffer.getInt
      val headers   =
        Vector.fill(buffer.getInt):
          val key = String(array(buffer.getInt), UTF_8)
          Header(key, part())
      val prior = part()
      val old   =
        buffer.getInt match
          case -1     => None
          case -2     => prior
          case length => Some(Chunk.array(array(length)))
      val value = part()
      Held(buffer.getLong, HeldContext(timestamp, offset, topic, partition, Headers.fromVector(headers)), prior, old, value)
    .toOption

/** A suppression buffer's changes in the order they go on, by the time each is held until and then by key, unsigned, as Java's buffer orders them,
  * and their size as Java counts it.
  */
private[xkafka] final case class Buffered(order: SortedSet[(Long, Chunk[Byte])], bytes: Long):
  /** With `held` under `key`. */
  def including(key: Chunk[Byte], held: Held): Buffered = Buffered(order + ((held.time, key)), bytes + held.size(key))

  /** Without `held`, which `key` holds. */
  def excluding(key: Chunk[Byte], held: Held): Buffered = Buffered(order - ((held.time, key)), bytes - held.size(key))

private[xkafka] object Buffered:
  private given Ordering[Chunk[Byte]] = Task.unsigned

  val empty: Buffered = Buffered(SortedSet.empty, 0L)

  /** The index of a buffer's `contents`, as restored from its changelog, where each holds a change. */
  def of(contents: Map[Chunk[Byte], Stored]): Option[Buffered] =
    contents.toList.foldLeftM(empty)((index, entry) => Held.decode(entry._2.value).map(index.including(entry._1, _)))

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

/** The keys a buffer changed since its last commit, which it logs at the commit in the order Java's `HashSet` of them iterates. */
type Dirty = JavaHashSet[Chunk[Byte]]

private[xkafka] object Dirty:
  val empty: Dirty = JavaHashSet.empty(JavaHashSet.bytes)

  /** A key's bucket hash, as Java's `HashMap` spreads `Arrays.hashCode` of it. */
  def hash(key: Chunk[Byte]): Int = JavaHashSet.spread(JavaHashSet.bytes(key))
