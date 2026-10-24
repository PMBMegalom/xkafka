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

import scala.concurrent.duration.FiniteDuration

import cats.effect.Temporal
import cats.effect.syntax.all.*
import cats.syntax.all.*
import fs2.concurrent.SignallingRef

/** What a consumer has handed out and committed of each partition it holds, which partitions it no longer hands records out of, and how many times
  * the group has assigned each partition to it.
  *
  * Both offsets are next offsets, the one after the last record handed out and the greatest one committed, so a partition is settled once the second
  * has reached the first.
  */
private[xkafka] final case class Handover(
    handedOut: Map[LeaseKey, Long],
    committed: Map[LeaseKey, Long],
    withdrawn: Set[LeaseKey],
    assignments: Map[LeaseKey, Long]
):
  def settled(partition: LeaseKey): Boolean = handedOut.get(partition).forall(next => committed.get(partition).exists(_ >= next))

  def assignment(partition: LeaseKey): Long = assignments.getOrElse(partition, 0L)

  def handOut(partition: LeaseKey, readUnder: Long, next: Long): (Handover, Boolean) =
    if withdrawn.contains(partition) || readUnder != assignment(partition) then (this, false)
    else (copy(handedOut = handedOut.updated(partition, next.max(handedOut.getOrElse(partition, next)))), true)

  def commit(offsets: Iterable[(LeaseKey, Long)]): Handover =
    copy(committed = offsets.foldLeft(committed)((held, entry) => held.updated(entry._1, entry._2.max(held.getOrElse(entry._1, entry._2)))))

  def withdraw(partitions: List[LeaseKey]): Handover = copy(withdrawn = withdrawn ++ partitions)

  def forget(partitions: List[LeaseKey]): Handover = copy(handedOut = handedOut -- partitions, committed = committed -- partitions)

  def assign(partitions: List[LeaseKey]): Handover =
    forget(partitions).copy(withdrawn = withdrawn -- partitions, assignments = assignments ++ partitions.map(key => key -> (assignment(key) + 1L)))

private[xkafka] object Handover:
  val empty: Handover = Handover(Map.empty, Map.empty, Set.empty, Map.empty)

/** Holds back a revocation until the records a consumer handed out from its partitions are committed, for at most a limit.
  *
  * A backend reports every record its record stream hands out and every offset committed, by the consumer or by a transaction. A revocation stops
  * records of its partitions being handed out, then waits. Those partitions hand out nothing more until the group assigns them again, since a record
  * read before the revocation can no longer be committed once it is applied. Each record carries the assignment it was read under, so one read before
  * the revocation is not handed out after the group assigns its partition again, when the partition starts over from its committed offset.
  */
private[xkafka] sealed trait RevocationHold[F[_]]:
  /** The assignment each partition is read under right now, to label the records of a poll that has returned. */
  def readUnder: F[LeaseKey => Long]

  /** Whether a record of `partition` read under assignment `readUnder`, whose next offset is `next`, may be handed out, recording it where it may. */
  def handOut(partition: LeaseKey, readUnder: Long, next: Long): F[Boolean]

  /** Records offsets that are now committed. */
  def committed(offsets: Iterable[(LeaseKey, Long)]): F[Unit]

  /** Stops `partitions` handing out records, waits for each to settle or for the limit to pass, and reports the ones that did not settle. */
  def revoke(partitions: List[LeaseKey]): F[Set[LeaseKey]]

  /** Stops `partitions` handing out records without waiting, for partitions the group has already given away. */
  def lose(partitions: List[LeaseKey]): F[Unit]

  /** Forgets what was handed out and committed of partitions whose revocation has been applied. */
  def revoked(partitions: List[LeaseKey]): F[Unit]

  /** Starts `partitions` afresh, handing out records again. */
  def assign(partitions: List[LeaseKey]): F[Unit]

private[xkafka] object RevocationHold:
  def apply[F[_]](limit: Option[FiniteDuration])(using F: Temporal[F]): F[RevocationHold[F]] =
    limit.fold(F.pure(unheld[F]))(value => SignallingRef[F, Handover](Handover.empty).map(held(_, value)))

  private def held[F[_]](state: SignallingRef[F, Handover], limit: FiniteDuration)(using F: Temporal[F]): RevocationHold[F] =
    new RevocationHold[F]:
      override def readUnder: F[LeaseKey => Long] = state.get.map(held => held.assignment)

      override def handOut(partition: LeaseKey, readUnder: Long, next: Long): F[Boolean] = state.modify(_.handOut(partition, readUnder, next))

      override def committed(offsets: Iterable[(LeaseKey, Long)]): F[Unit] = state.update(_.commit(offsets))

      override def revoke(partitions: List[LeaseKey]): F[Set[LeaseKey]] =
        val settled = state.discrete.exists(held => partitions.forall(held.settled)).compile.drain
        state.update(_.withdraw(partitions)) *> settled.timeout(limit).attempt *> state.get.map(held => partitions.filterNot(held.settled).toSet)

      override def lose(partitions: List[LeaseKey]): F[Unit] = state.update(_.withdraw(partitions).forget(partitions))

      override def revoked(partitions: List[LeaseKey]): F[Unit] = state.update(_.forget(partitions))

      override def assign(partitions: List[LeaseKey]): F[Unit] = state.update(_.assign(partitions))

  private def unheld[F[_]](using F: Temporal[F]): RevocationHold[F] =
    new RevocationHold[F]:
      override def readUnder: F[LeaseKey => Long] = F.pure(_ => 0L)

      override def handOut(partition: LeaseKey, readUnder: Long, next: Long): F[Boolean] = F.pure(true)

      override def committed(offsets: Iterable[(LeaseKey, Long)]): F[Unit] = F.unit

      override def revoke(partitions: List[LeaseKey]): F[Set[LeaseKey]] = F.pure(Set.empty)

      override def lose(partitions: List[LeaseKey]): F[Unit] = F.unit

      override def revoked(partitions: List[LeaseKey]): F[Unit] = F.unit

      override def assign(partitions: List[LeaseKey]): F[Unit] = F.unit
