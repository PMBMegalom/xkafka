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

import cats.effect.Concurrent
import cats.effect.syntax.all.*
import cats.syntax.all.*
import fs2.concurrent.SignallingRef

/** One assignment of one partition to one consumer, from the rebalance that assigned it to the one that revoked it. */
private[xkafka] final case class Lease[P](partition: P, number: Long)

/** How every backend names a partition in its leases: the topic and partition number as the client reports them. */
private[xkafka] type LeaseKey = (String, Int)

/** The partitions a consumer holds, and how many transactions are recording an offset under each of their leases right now.
  *
  * `revocations` counts every revocation of a partition, so a read can tell whether one happened while it ran.
  */
private[xkafka] final case class Leases[P](next: Long, current: Map[P, Lease[P]], revocations: Map[P, Long], inFlight: Map[Lease[P], Int]):
  def assigned(partitions: List[P]): Leases[P] =
    copy(
      next = next + partitions.size,
      current = current ++ partitions.zipWithIndex.map((partition, index) => partition -> Lease(partition, next + index))
    )

  def revoked(partitions: List[P]): (Leases[P], Set[Lease[P]]) =
    val counted = partitions.foldLeft(revocations)((counts, partition) => counts.updated(partition, counts.getOrElse(partition, 0L) + 1L))
    (copy(current = current -- partitions, revocations = counted), partitions.flatMap(current.get).toSet)

  /** The lease an item read from `partition` was read under: the partition's lease once the read finished, unless the partition was revoked while it
    * ran, which leaves no telling which assignment the item came from.
    */
  def readUnder(partition: P, before: Leases[P]): Option[Lease[P]] =
    current.get(partition).filter(_ => revocations.get(partition) == before.revocations.get(partition))

  def acquire(lease: Lease[P]): (Leases[P], Boolean) =
    if current.get(lease.partition).contains(lease) then (copy(inFlight = inFlight.updated(lease, inFlight.getOrElse(lease, 0) + 1)), true)
    else (this, false)

  def release(lease: Lease[P]): Leases[P] = copy(inFlight = inFlight.updatedWith(lease)(_.map(_ - 1).filter(_ > 0)))

  /** Acquires every lease or none of them. */
  def acquireAll(leases: List[Lease[P]]): (Leases[P], Boolean) =
    if leases.forall(lease => current.get(lease.partition).contains(lease)) then
      (leases.foldLeft(this)((held, lease) => held.acquire(lease)._1), true)
    else (this, false)

  def idle(leases: Set[Lease[P]]): Boolean = leases.forall(lease => !inFlight.contains(lease))

private[xkafka] object Leases:
  def empty[P]: Leases[P] = Leases(0L, Map.empty, Map.empty, Map.empty)

/** Ties each offset a consumer hands out to the assignment it was read under, so a transaction records it only while this consumer still holds it.
  *
  * A backend feeds it every assignment and revocation, and applies a revocation only once `revoke` completes, which is when no transaction is still
  * recording an offset under one of the revoked leases. Until then the group cannot hand the partition to another member. With fencing off, this
  * records nothing and every offset names the consumer's current membership, which is how offsets behaved before fencing existed.
  *
  * `P` is however the backend names a partition, so it can key leases without converting what the client reports.
  */
private[xkafka] sealed trait AssignmentLeases[F[_], P]:
  /** Records an assignment. The eager protocol replaces the whole assignment, so `replacing` also ends every lease still held. */
  def assign(partitions: List[P], replacing: Boolean): F[Unit]

  /** Ends the partitions' leases, and completes once no transaction is still recording an offset under one of them. */
  def revoke(partitions: List[P]): F[Unit]

  /** Runs `read` and labels each item with the lease it was read under, where one can be named. */
  def reading[A](read: F[List[A]])(partition: A => P): F[List[(A, Option[Lease[P]])]]

  /** The lease each partition holds right now, for a backend whose reads cannot overlap a rebalance. */
  def held: F[P => Option[Lease[P]]]

  /** What a transaction records an offset read under `lease` against. `owner` tells one consumer's leases from another's, and `handle` names the
    * consumer's current membership, which is what the backend sends.
    */
  def membership(owner: AnyRef, lease: Option[Lease[P]], handle: F[GroupHandle], release: GroupHandle => F[Unit]): GroupMembership[F]

  /** Runs one attempt at a plain commit of offsets read under `leases`, holding them for as long as it runs, so a revocation waits for it. */
  def holding[A](leases: List[Option[Lease[P]]])(attempt: F[A]): F[A]

private[xkafka] object AssignmentLeases:
  def apply[F[_], P](enabled: Boolean)(using F: Concurrent[F]): F[AssignmentLeases[F, P]] =
    if enabled then SignallingRef[F, Leases[P]](Leases.empty).map(fenced) else F.pure(unfenced)

  /** Classified the way the broker classifies an offset recorded under a generation the group has left. A new one each time, since whoever catches it
    * may add to it.
    */
  def revokedAssignment: KafkaException.BackendFailure =
    new KafkaException.BackendFailure(
      "the offset was read under an assignment this consumer no longer holds",
      Some(ErrorCode.IllegalGeneration),
      Some(false),
      Some(false)
    )

  private def fenced[F[_], P](leases: SignallingRef[F, Leases[P]])(using F: Concurrent[F]): AssignmentLeases[F, P] =
    new AssignmentLeases[F, P]:
      override def assign(partitions: List[P], replacing: Boolean): F[Unit] =
        leases.update(held => (if replacing then held.revoked(held.current.keys.toList)._1 else held).assigned(partitions))

      override def revoke(partitions: List[P]): F[Unit] =
        leases.modify(_.revoked(partitions)).flatMap(ended => leases.discrete.exists(_.idle(ended)).compile.drain)

      override def reading[A](read: F[List[A]])(partition: A => P): F[List[(A, Option[Lease[P]])]] =
        for
          before <- leases.get
          items  <- read
          after  <- leases.get
        yield items.map(item => item -> after.readUnder(partition(item), before))

      override def held: F[P => Option[Lease[P]]] = leases.get.map(_.current.get)

      override def membership(owner: AnyRef, lease: Option[Lease[P]], handle: F[GroupHandle], release: GroupHandle => F[Unit]): GroupMembership[F] =
        val acquire =
          lease.fold(F.raiseError[GroupHandle](revokedAssignment)): held =>
            leases.modify(_.acquire(held)).ifM(handle.onError(_ => leases.update(_.release(held))), F.raiseError(revokedAssignment))
        new GroupMembership.Backend(
          owner -> lease,
          acquire,
          value => release(value).guarantee(lease.traverse_(held => leases.update(_.release(held))))
        )

      override def holding[A](held: List[Option[Lease[P]]])(attempt: F[A]): F[A] =
        held.sequence match
          case None        => F.raiseError(revokedAssignment)
          case Some(named) => F.bracket(leases.modify(_.acquireAll(named)))(acquired =>
              if acquired then attempt else F.raiseError(revokedAssignment)
            )(acquired => leases.update(state => named.foldLeft(state)(_.release(_))).whenA(acquired))

  private def unfenced[F[_], P](using F: Concurrent[F]): AssignmentLeases[F, P] =
    new AssignmentLeases[F, P]:
      override def assign(partitions: List[P], replacing: Boolean): F[Unit] = F.unit

      override def revoke(partitions: List[P]): F[Unit] = F.unit

      override def reading[A](read: F[List[A]])(partition: A => P): F[List[(A, Option[Lease[P]])]] = read.map(_.map(_ -> None))

      override def held: F[P => Option[Lease[P]]] = F.pure(_ => None)

      override def membership(owner: AnyRef, lease: Option[Lease[P]], handle: F[GroupHandle], release: GroupHandle => F[Unit]): GroupMembership[F] =
        new GroupMembership.Backend(owner, handle, release)

      override def holding[A](leases: List[Option[Lease[P]]])(attempt: F[A]): F[A] = attempt
