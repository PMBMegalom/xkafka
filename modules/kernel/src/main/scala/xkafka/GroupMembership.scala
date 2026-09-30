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

import cats.arrow.FunctionK
import cats.effect.{MonadCancelThrow, Resource}
import cats.syntax.all.*

/** What a backend needs in order to record a consumer's offsets against its group.
  *
  * Each backend defines its own, and a transaction of that same backend is the only thing that reads one. The kernel names no backend type, so this
  * says only that a value came from one.
  */
private[xkafka] trait GroupHandle

/** How an offset names the consumer group a transaction records it against.
  *
  * The backends disagree on what that name is. The Java client and librdkafka each take a metadata value of their own, and the JavaScript client
  * takes the consumer object itself, so there is no portable shape to expose. An offset therefore carries its own backend's `GroupHandle`.
  *
  * A backend that can name the membership an offset was read under, and not only the consumer's current one, gives each offset that membership. A
  * transaction then records the offset under it, and the broker rejects it where the group has moved on since.
  *
  * An offset no transaction can reach carries that as a value, so transforming a consumer's effect keeps the answer without an instance for the new
  * effect.
  */
private[xkafka] sealed trait GroupMembership[F[_]]:
  /** What the backend that built these offsets needs in order to record them, absent where they did not come from a consumer. */
  def handle: Option[GroupMembership.Backend[F]]

  def mapK[G[_]](fk: FunctionK[F, G]): GroupMembership[G]

private[xkafka] object GroupMembership:
  /** Pairs the handle of each membership in `batch` with the offsets read under it.
    *
    * A transaction reads these to record offsets against the groups they came from, and the wording of both failures lives here so that every backend
    * reports the same thing.
    */
  def resolve[F[_]](batch: CommittableOffsetBatch[F])(using F: MonadCancelThrow[F]): Resource[F, List[(GroupHandle, Map[TopicPartition, Offset])]] =
    batch.memberships.toList.traverse { (membership, offsets) =>
      membership.handle match
        case Some(handle) => Resource.makeFull[F, GroupHandle](poll => poll(handle.acquire))(handle.release).map(_ -> offsets)
        case None         => Resource
            .eval(F.raiseError(new KafkaException.Unsupported("a transaction can only record offsets that came from a consumer of the same backend")))
    }

  /** For a handle built by one backend and read by another. */
  def unrecognised(handle: GroupHandle): KafkaException.Unsupported =
    new KafkaException.Unsupported(s"a transaction on this backend cannot record offsets from ${handle.getClass.getName}")

  /** For offsets that did not come from a consumer, so no transaction can say which group they belong to. */
  final case class Absent[F[_]]() extends GroupMembership[F]:
    override def handle: Option[Backend[F]] = None

    override def mapK[G[_]](fk: FunctionK[F, G]): GroupMembership[G] = Absent()

  /** For offsets whose consumer a transaction of the same backend can name.
    *
    * Two values with the same `key` name the same membership, so a transaction acquires one handle for all the offsets they share, whatever effect
    * each was transformed into.
    */
  final class Backend[F[_]](val key: Any, val acquire: F[GroupHandle], val release: GroupHandle => F[Unit]) extends GroupMembership[F]:
    override def handle: Option[Backend[F]] = Some(this)

    override def mapK[G[_]](fk: FunctionK[F, G]): GroupMembership[G] = new Backend(key, fk(acquire), handle => fk(release(handle)))

    override def equals(other: Any): Boolean =
      other match
        case backend: Backend[?] => backend.key == key
        case _                   => false

    override def hashCode: Int = key.##

  object Backend:
    /** A membership equal only to itself and to its transformations. */
    def apply[F[_]](acquire: F[GroupHandle], release: GroupHandle => F[Unit]): Backend[F] = new Backend(new AnyRef, acquire, release)
