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

import scala.collection.immutable.TreeMap

import cats.effect.Concurrent
import cats.syntax.all.*
import fs2.Chunk

/** One store of a task, by serialized key, in the order of [[StoreContents.keys]]. A task reads it through what it has written since its last commit,
  * and writes to it only once that commit has succeeded, all of a commit's changes at once, so that a store never holds what the group has not
  * committed. A window, session or join window store also finds its keys by the time each names, and keeps a clock: the latest time any key written
  * to it named, removals included, as Java's window and session stores keep theirs.
  */
private[xkafka] trait TaskStore[F[_]]:
  def get(key: Chunk[Byte]): F[Option[Stored]]

  /** The keys from `from`, included, to `to`, left out, and what they hold, in order; a bound not given leaves that side open. */
  def range(from: Option[Chunk[Byte]], to: Option[Chunk[Byte]]): F[Vector[(Chunk[Byte], Stored)]]

  /** The keys naming a time from `from` to `to`, both included, and what they hold, in order of time and then of key; none for a store without time.
    */
  def timed(from: Long, to: Long): F[Vector[(Chunk[Byte], Stored)]]

  /** The latest time a key written to it named, or [[WindowStore.NoTime]], as for a store without time. */
  def clock: F[Long]

  /** Applies `changes` as one, a key with nothing to hold removed, and records with them that the store holds each partition `positions` names of
    * what fills it up to the offset given, the one to read from next.
    */
  def write(changes: StoreChanges, positions: Map[Int, Long]): F[Unit]

  /** Where the store last recorded that it holds each partition of what fills it up to. It never runs ahead of what the store holds. */
  def positions: F[Map[Int, Long]]

  /** Applies `changes` as one: a key with nothing to hold is removed. */
  final def write(changes: StoreChanges): F[Unit] = write(changes, Map.empty)

  /** Where a task's store, filled from one partition of its changelog, last recorded that it holds it up to, if anywhere. */
  final def position(using F: cats.Functor[F]): F[Option[Long]] = F.map(positions)(_.values.maxOption)

  /** Releases what the store holds open, after which it is not used again; a store in memory holds nothing open. */
  def close: F[Unit]

  /** The keys starting with `prefix` and what they hold, in order. */
  final def prefixed(prefix: Chunk[Byte]): F[Vector[(Chunk[Byte], Stored)]] = range(Some(prefix), StoreContents.past(prefix))

  /** Every key and what it holds, in order. */
  final def all: F[Vector[(Chunk[Byte], Stored)]] = range(None, None)

private[xkafka] object TaskStore:
  /** How to read the time a key names, for a window, session or join window store. */
  type TimeOf = Chunk[Byte] => Long

  /** Opens the store of a name for a task, reading times with the `TimeOf` given, if any, and holding what its changelog restored. */
  type Open[F[_]] = (String, Option[TimeOf], StoreContents) => F[TaskStore[F]]

  /** Opens each store in memory. */
  def memory[F[_]: Concurrent]: Open[F] = (_, time, held) => inMemory(held, time)

  /** What a store in memory holds: its keys, the same keys in order of time where it has a time, its clock, and its position. */
  private final case class Held(contents: StoreContents, byTime: TreeMap[(Long, Chunk[Byte]), Unit], clock: Long, positions: Map[Int, Long])

  private val timeThenKey: Ordering[(Long, Chunk[Byte])] = Ordering.Tuple2(using Ordering.Long, StoreContents.keys)

  /** A store in memory, holding `held` to start with, at `positions`, and finding keys by the time `time` reads, if given. */
  def inMemory[F[_]](held: StoreContents, time: Option[TimeOf] = None, positions: Map[Int, Long] = Map.empty)(using
      F: Concurrent[F]
  ): F[TaskStore[F]] =
    val byTime =
      time.fold(TreeMap.empty(using timeThenKey))(of => TreeMap.from(held.keysIterator.map(key => (of(key), key) -> ()))(using timeThenKey))
    val clock = time.fold(WindowStore.NoTime)(of => held.keysIterator.map(of).maxOption.getOrElse(WindowStore.NoTime))
    F.ref(Held(held, byTime, clock, positions)).map: state =>
      new TaskStore[F]:
        def get(key: Chunk[Byte]): F[Option[Stored]]                                                    = state.get.map(_.contents.get(key))
        def range(from: Option[Chunk[Byte]], to: Option[Chunk[Byte]]): F[Vector[(Chunk[Byte], Stored)]] = state.get.map(_.contents.range(from, to))
        def timed(from: Long, to: Long): F[Vector[(Chunk[Byte], Stored)]]                               =
          state.get.map: now =>
            if from > to then Vector.empty
            else now.byTime.rangeFrom((from, Chunk.empty)).keysIterator.takeWhile(_._1 <= to).map((_, key) => key -> now.contents(key)).toVector
        def clock: F[Long]                                                   = state.get.map(_.clock)
        def positions: F[Map[Int, Long]]                                     = state.get.map(_.positions)
        def close: F[Unit]                                                   = F.unit
        def write(changes: StoreChanges, positions: Map[Int, Long]): F[Unit] =
          state.update: now =>
            val moved = now.positions ++ positions
            time.fold(now.copy(contents = now.contents.after(changes), positions = moved)): of =>
              val indexed =
                changes.foldLeft(now.byTime):
                  case (index, (key, Some(_))) => index.updated((of(key), key), ())
                  case (index, (key, None))    => index - ((of(key), key))
              Held(now.contents.after(changes), indexed, changes.keysIterator.map(of).foldLeft(now.clock)(math.max), moved)
