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

import scala.concurrent.duration.FiniteDuration

import cats.effect.{IO, Ref}
import cats.syntax.all.*

/** A state directory kept in memory for tests on every platform: what each store holds and its position outlast closing it, as a directory on disk
  * keeps them, and the stores opened, the tasks given up and the directories removed are recorded.
  */
final class MemoryStateDirectory private (
    val stores: Ref[IO, Map[(String, String), (StoreContents, Option[Long])]],
    val gaveUp: Ref[IO, Map[String, Map[String, Long]]],
    val opened: Ref[IO, List[(String, String)]],
    val removed: Ref[IO, Set[String]]
) extends StateDirectory[IO]:
  def open(owner: String, store: String, time: Option[TaskStore.TimeOf]): IO[TaskStore[IO]] =
    opened.update(_ :+ (owner, store)) *> stores.get.map(_.get((owner, store))).flatMap: found =>
      // A store holding something without saying how far is emptied, as on disk.
      val (contents, position) = found.filter(_._2.nonEmpty).getOrElse((StoreContents.empty, None))
      TaskStore.inMemory[IO](contents, time, position).map: store0 =>
        new TaskStore[IO]:
          export store0.{clock, get, position, range, timed}
          def write(changes: StoreChanges, position: Option[Long]): IO[Unit] = store0.write(changes, position)
          def close: IO[Unit]                                                =
            (store0.all, store0.position).flatMapN((all, at) => stores.update(_.updated((owner, store), (StoreContents.of(all), at))))

  def processId(fresh: String): IO[String] = IO.pure(fresh)

  def released(owner: String, positions: Map[String, Long]): IO[Unit] = gaveUp.update(_.updated(owner, positions))

  def held: IO[Map[String, Map[String, Long]]] = gaveUp.get

  def cleaned(held: Set[String], delay: FiniteDuration): IO[Unit] = IO.unit

object MemoryStateDirectory:
  /** One holding `stores` and having given up `gaveUp` already, as an earlier run left it. */
  def apply(
      stores: Map[(String, String), (StoreContents, Option[Long])] = Map.empty,
      gaveUp: Map[String, Map[String, Long]] = Map.empty
  ): IO[MemoryStateDirectory] =
    (IO.ref(stores), IO.ref(gaveUp), IO.ref(List.empty[(String, String)]), IO.ref(Set.empty[String])).mapN(new MemoryStateDirectory(_, _, _, _))
