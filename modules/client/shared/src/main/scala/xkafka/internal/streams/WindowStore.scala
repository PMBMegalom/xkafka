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

import fs2.Chunk

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

  /** The clock of a store holding `entries`: the latest window start among them. */
  def clockOf(entries: Map[Chunk[Byte], Stored]): Long = entries.keysIterator.map(startOf).maxOption.getOrElse(NoTime)

  /** Whether a window starting at `start` has expired by `clock`. */
  def expired(start: Long, clock: Long, retention: Long): Boolean = start <= clock - retention

  /** The windows of `entries` that have not expired by `clock`. */
  def pruned(entries: Map[Chunk[Byte], Stored], clock: Long, retention: Long): Map[Chunk[Byte], Stored] =
    entries.filter((key, _) => !expired(startOf(key), clock, retention))

  /** Each window store of `stores` without the windows that have expired by its clock, as Java leaves a store after restoring it. */
  def prunedAll(stores: Map[String, Map[Chunk[Byte], Stored]], retentions: Map[String, Long]): Map[String, Map[Chunk[Byte], Stored]] =
    stores.map: (name, entries) =>
      name -> retentions.get(name).fold(entries)(retention => pruned(entries, clockOf(entries), retention))
