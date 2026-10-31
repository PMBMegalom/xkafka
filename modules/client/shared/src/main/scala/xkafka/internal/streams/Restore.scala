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

import cats.Monad
import cats.syntax.all.*
import fs2.Chunk
import driver.{KafkaDriver, PolledRecord, StartAt}

/** Refills stores from their changelogs. */
private[xkafka] object Restore:
  /** How long each poll waits for records. */
  val PollMillis = 500

  /** Reads each changelog partition `changelogs` names, with the store it backs, from where `from` says, or its start, to its end, through
    * `consumer`, which reads under read_committed and reports the ends it reaches, onto what `held` already holds. Each store holds the latest value
    * of each key, and a record with no value removes its key. Records without a key, which no store writes, are skipped.
    */
  def apply[F[_]](
      driver: KafkaDriver[F],
      consumer: driver.Client,
      changelogs: Map[TopicPartition, String],
      from: Map[TopicPartition, Long] = Map.empty,
      held: Map[String, Map[Chunk[Byte], Stored]] = Map.empty
  )(using F: Monad[F]): F[Map[String, Map[Chunk[Byte], Stored]]] =
    val stores   = changelogs.map((partition, store) => (partition.topic.value, partition.partition.value) -> store)
    val starting = changelogs.keys.toVector
    def read(left: Set[(String, Int)], restored: Map[String, Map[Chunk[Byte], Stored]]): F[Map[String, Map[Chunk[Byte], Stored]]] =
      if left.isEmpty then F.pure(restored)
      else
        driver.poll(consumer, PollMillis).flatMap: polled =>
          val after =
            polled.records.foldLeft(restored)((now, record) => stores.get((record.topic, record.partition)).fold(now)(applied(now, _, record)))
          read(left -- polled.ends.map(end => (end.topic, end.partition)), after)
    driver.assign(consumer, starting, starting.map(partition => StartAt(partition, from.getOrElse(partition, 0L), -1))) *>
      read(stores.keySet, stores.values.map(store => store -> held.getOrElse(store, Map.empty[Chunk[Byte], Stored])).toMap)

  /** The stores with one changelog record applied to `store`. */
  def applied(restored: Map[String, Map[Chunk[Byte], Stored]], store: String, record: PolledRecord): Map[String, Map[Chunk[Byte], Stored]] =
    record.key.fold(restored): key =>
      val contents = restored.getOrElse(store, Map.empty)
      restored.updated(store, record.value.fold(contents - key)(value => contents.updated(key, Stored(value, record.timestamp.getOrElse(-1L)))))
