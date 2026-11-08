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

import cats.effect.{Async, Resource}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import driver.{KafkaDriver, PolledRecord, StartAt}
import xkafka.streams.GlobalStore

/** Keeps a program's global tables on one member, apart from its tasks, as Java's global thread keeps them. */
private[xkafka] object GlobalStores:
  /** The tables' stores, by name, and what fails if reading them does. */
  final class Held[F[_]](val stores: Map[String, TaskStore[F]], val failure: F[Unit])

  /** The changes `made` so far with one record read after the tables were up to date: read and written by the table's serdes, as Java's global task
    * does.
    */
  def updated[F[_]](global: GlobalStore[F], made: Map[String, StoreChanges], record: PolledRecord)(using
      cats.MonadThrow[F]
  ): F[Map[String, StoreChanges]] =
    global.change(record.key, record.value, record.headers).map:
      case None              => made
      case Some((key, kept)) =>
        val changes = made.getOrElse(global.store.value, StoreContents.unchanged)
        made.updated(global.store.value, changes.updated(key, kept.map(Stored(_, record.timestamp.getOrElse(-1L)))))

  /** Acquired once each table has read to the end of its topic, as Java starts no stream thread before that, the bytes read so far kept as they are,
    * as Java's restore keeps them. Then a fiber follows the topics until released, which waits for its poll, so that none is cut short before the
    * consumer is destroyed.
    */
  def apply[F[_]](
      driver: KafkaDriver[F],
      consumer: driver.Client,
      globals: List[GlobalStore[F]],
      partitions: Topic => F[Int],
      open: TaskStore.Open[F]
  )(using F: Async[F]): Resource[F, Held[F]] =
    val byTopic = globals.map(global => global.topic.value -> global).toMap
    for
      starts <-
        Resource.eval(
          globals.flatTraverse: global =>
            partitions(global.topic).flatMap: count =>
              (0 until count).toList
                .traverse(n => Partition.from(n).leftMap(new KafkaException.InvalidValue(_)).liftTo[F].map(TopicPartition(global.topic, _)))
        )
      _    <- Resource.eval(driver.assign(consumer, starts.toVector, starts.toVector.map(StartAt(_, 0L, -1))))
      ends <- Resource.eval(Restore.endsOf(driver, consumer, starts.toVector))
      held <-
        Resource.make(
          globals.traverse(global => open(global.store.value, None, StoreContents.empty).map(global.store.value -> _)).map(_.toMap)
        )(_.values.toList.traverse_(_.close))
      ready    <- Resource.eval(F.deferred[Unit])
      failed   <- Resource.eval(F.deferred[Throwable])
      stopping <- Resource.eval(F.ref(false))
      following =
        def read(left: Set[(String, Int)]): F[Unit] =
          stopping.get.ifM(
            F.unit,
            driver.poll(consumer, Restore.PollMillis).flatMap: polled =>
              // Each poll's records are written as one change, as they are read before the tables are up to date, through the tables' serdes after.
              polled.records.foldLeftM(Map.empty[String, StoreChanges]) { (made, record) =>
                byTopic.get(record.topic).fold(F.pure(made)): global =>
                  if left((record.topic, record.partition)) then F.pure(Restore.changed(made, global.store.value, record))
                  else updated(global, made, record)
              }.flatMap(Restore.written(held, _)) *> {
                val remaining = Restore.unfinished(left, polled.ends, ends)
                ready.complete(()).whenA(remaining.isEmpty) *> read(remaining)
              }
          )
        read(starts.map(partition => (partition.topic.value, partition.partition.value)).toSet).handleErrorWith(error => failed.complete(error).void)
      _ <- Resource.make(following.start)(fiber => stopping.set(true) *> fiber.join.void)
      _ <- Resource.eval(F.race(ready.get, failed.get.flatMap(F.raiseError[Unit])).void)
    yield new Held(held, failed.get.flatMap(F.raiseError[Unit]))
