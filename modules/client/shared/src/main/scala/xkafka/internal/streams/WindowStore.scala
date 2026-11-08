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

import scala.annotation.tailrec

import cats.Monad
import cats.syntax.all.*
import fs2.Chunk
import xkafka.streams.Subtopology

/** A window store as Java's in-memory window store keeps one. Its contents are keyed as its changelog is: the serialized key, the window's start as
  * eight big-endian bytes, and a sequence number of four, which stays 0 for a store that keeps one value per key and window. Its clock is the latest
  * window start it has held; a window whose start is a retention or more behind the clock has expired, reads as absent, and is not kept.
  */
private[xkafka] object WindowStore:
  /** What Java adds to a window store's retention for its changelog's `retention.ms`: `windowstore.changelog.additional.retention.ms`, a day. */
  val AdditionalRetentionMillis: Long = 24L * 60 * 60 * 1000

  /** The clock of a store that has held nothing, Java's `NO_TIMESTAMP`. */
  val NoTime: Long = -1L

  /** The configs Java gives a window store's changelog topic. */
  def changelogConfigs(retention: Long): List[(String, String)] =
    // Java keeps the largest timestamp where the sum would pass it.
    val kept = if retention > Long.MaxValue - AdditionalRetentionMillis then Long.MaxValue else retention + AdditionalRetentionMillis
    List("cleanup.policy" -> "compact,delete", "message.timestamp.type" -> "CreateTime", "retention.ms" -> kept.toString)

  /** The key of a key's window starting at `start`. */
  def key(serialized: Chunk[Byte], start: Long): Chunk[Byte] = serialized ++ Chunk.array(ByteBuffer.allocate(12).putLong(start).putInt(0).array)

  /** The start of the window a key names. */
  def startOf(key: Chunk[Byte]): Long = ByteBuffer.wrap(key.drop(key.size - 12).take(8).toArray).getLong

  /** How to read the time that counts towards its clock from a key of each window and session store of `subtopologies`. */
  def timesOf[F[_]](subtopologies: Iterable[Subtopology[F]]): Map[String, Chunk[Byte] => Long] =
    subtopologies.flatMap: subtopology =>
      subtopology.retentions.keys.map(store => store -> (if subtopology.sessionStores(store) then SessionStore.endOf else startOf))
    .toMap

  /** Whether a window starting at `start` has expired by `clock`. */
  def expired(start: Long, clock: Long, retention: Long): Boolean = start <= clock - retention

  /** Removes from each store of `stores` that `retentions` names what has expired by its clock, as Java drops it from a store after restoring a
    * batch.
    */
  def swept[F[_]](stores: Map[String, TaskStore[F]], retentions: Map[String, Long])(using F: Monad[F]): F[Unit] =
    stores.toList.traverse_ : (name, store) =>
      retentions.get(name).traverse_ : retention =>
        store.clock.flatMap(clock => store.timed(Long.MinValue, clock - retention)).flatMap: gone =>
          store.write(StoreContents.unchanged ++ gone.map((key, _) => key -> None)).unlessA(gone.isEmpty)

/** A session store as Java's in-memory session store keeps one. Its contents are keyed as its changelog is: the serialized key, then the session's
  * end and its start, each as eight big-endian bytes. Its clock is the latest session end it has held; a session whose end is a retention or more
  * behind the clock has expired, reads as absent, and is not kept.
  */
private[xkafka] object SessionStore:
  /** The key of a key's session from `start` to `end`. */
  def key(serialized: Chunk[Byte], start: Long, end: Long): Chunk[Byte] =
    serialized ++ Chunk.array(ByteBuffer.allocate(16).putLong(end).putLong(start).array)

  /** The end of the session a key names. */
  def endOf(key: Chunk[Byte]): Long = ByteBuffer.wrap(key.drop(key.size - 16).take(8).toArray).getLong

  /** The start of the session a key names. */
  def startOf(key: Chunk[Byte]): Long = ByteBuffer.wrap(key.drop(key.size - 8).toArray).getLong

/** The window store of one side of a stream-stream join, as Java's keeps one, which keeps every record: keyed as its changelog is, by the serialized
  * key, the record's time as eight big-endian bytes, and a sequence number of four, which tells records of one key and time apart.
  */
private[xkafka] object JoinWindowStore:
  /** The key of the record of `serialized` at `timestamp` with sequence number `sequence`. */
  def key(serialized: Chunk[Byte], timestamp: Long, sequence: Int): Chunk[Byte] =
    serialized ++ Chunk.array(ByteBuffer.allocate(12).putLong(timestamp).putInt(sequence).array)

  /** The sequence number a key carries. */
  def sequenceOf(key: Chunk[Byte]): Int = ByteBuffer.wrap(key.drop(key.size - 4).toArray).getInt

  /** The sequence number after `sequence`, as Java's store counts, from 1 and back to 0 past the largest int. */
  def next(sequence: Int): Int = (sequence + 1) & 0x7fffffff

/** The store of a left or outer stream-stream join's records that matched nothing yet, as Java's list-valued store keeps it: keyed by the record's
  * time as eight big-endian bytes, its side as one byte, 1 for the left and 0 for the right, and the serialized key; each key holds a list of the
  * values of the records with that time and key, written as Java's list serde writes a list of byte arrays, each its side's flag and the value.
  */
private[xkafka] object SharedJoinStore:
  /** The key of a record of `serialized` at `timestamp` from the left side, or the right. */
  def key(timestamp: Long, left: Boolean, serialized: Chunk[Byte]): Chunk[Byte] =
    Chunk.array(ByteBuffer.allocate(9).putLong(timestamp).put((if left then 1 else 0).toByte).array) ++ serialized

  /** The time a key names. */
  def timestampOf(key: Chunk[Byte]): Long = ByteBuffer.wrap(key.take(8).toArray).getLong

  /** Whether a key names a record of the left side. */
  def isLeft(key: Chunk[Byte]): Boolean = key(8) == 1

  /** The serialized key a key names. */
  def keyOf(key: Chunk[Byte]): Chunk[Byte] = key.drop(9)

  /** A value as the list holds it: its side's flag, 1 where it is the left's, and the value. */
  def element(left: Boolean, value: Chunk[Byte]): Chunk[Byte] = Chunk.singleton((if left then 1 else 0).toByte) ++ value

  /** The list of `elements`, as Java's list serde writes one whose elements vary in size: the flag 1, the count, then each one's size and bytes. */
  def list(elements: Vector[Chunk[Byte]]): Chunk[Byte] =
    val sizes = ByteBuffer.allocate(5).put(1.toByte).putInt(elements.size).array
    elements.foldLeft(Chunk.array(sizes))((written, element) => written ++ Chunk.array(ByteBuffer.allocate(4).putInt(element.size).array) ++ element)

  /** The elements of a list as `list` writes it, or none for bytes that are not one. */
  def elements(list: Chunk[Byte]): Option[Vector[Chunk[Byte]]] =
    def int(at: Int) = Option.when(at + 4 <= list.size)(ByteBuffer.wrap(list.drop(at).take(4).toArray).getInt)
    @tailrec
    def read(at: Int, remaining: Int, done: Vector[Chunk[Byte]]): Option[Vector[Chunk[Byte]]] =
      if remaining == 0 then Option.when(at == list.size)(done)
      else
        int(at) match
          case Some(size) if size >= 0 && at + 4 + size <= list.size => read(at + 4 + size, remaining - 1, done :+ list.drop(at + 4).take(size))
          case _                                                     => None
    if list.isEmpty || list(0) != 1 then None else int(1).filter(_ >= 0).flatMap(read(5, _, Vector.empty))
