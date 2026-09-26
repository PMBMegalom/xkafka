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

import cats.MonadThrow
import cats.arrow.FunctionK
import cats.syntax.all.*

/** What a backend needs in order to record a consumer's offsets against its group.
  *
  * Each backend defines its own, and a transaction of that same backend is the only thing that reads one. The kernel names no backend type, so this
  * says only that a value came from one.
  */
private[xkafka] trait GroupHandle

/** How a committer names the consumer group a transaction records offsets against.
  *
  * The backends disagree on what that name is. The Java client and librdkafka each take a metadata value of their own, and the JavaScript client
  * takes the consumer object itself, so there is no portable shape to expose. A committer therefore carries its own backend's `GroupHandle`.
  *
  * A committer no transaction can reach carries that as a value, so transforming a consumer's effect keeps the answer without an instance for the new
  * effect.
  */
private[xkafka] sealed trait GroupMembership[F[_]]:
  /** What the backend that built this committer needs in order to record its offsets, absent where the committer did not come from a consumer. */
  def handle: Option[F[GroupHandle]]

  def mapK[G[_]](fk: FunctionK[F, G]): GroupMembership[G]

private[xkafka] object GroupMembership:
  /** Pairs the handle of each committer in `batch` with the offsets that committer holds.
    *
    * A transaction reads these to record offsets against the groups they came from, and the wording of both failures lives here so that every backend
    * reports the same thing.
    */
  def resolve[F[_]](batch: CommittableOffsetBatch[F])(using F: MonadThrow[F]): F[List[(GroupHandle, Map[TopicPartition, Offset])]] =
    batch.offsets.toList.traverse { (committer, offsets) =>
      committer.membership.handle match
        case Some(handle) => handle.map(_ -> offsets)
        case None         => F
            .raiseError(new KafkaException.Unsupported("a transaction can only record offsets that came from a consumer of the same backend"))
    }

  /** For a handle built by one backend and read by another. */
  def unrecognised(handle: GroupHandle): KafkaException.Unsupported =
    new KafkaException.Unsupported(s"a transaction on this backend cannot record offsets from ${handle.getClass.getName}")

  /** For a committer that did not come from a consumer, so no transaction can say which group its offsets belong to. */
  final case class Absent[F[_]]() extends GroupMembership[F]:
    override def handle: Option[F[GroupHandle]] = None

    override def mapK[G[_]](fk: FunctionK[F, G]): GroupMembership[G] = Absent()

  /** For a committer whose consumer a transaction of the same backend can name. */
  final case class Backend[F[_]](value: F[GroupHandle]) extends GroupMembership[F]:
    override def handle: Option[F[GroupHandle]] = Some(value)

    override def mapK[G[_]](fk: FunctionK[F, G]): GroupMembership[G] = Backend(fk(value))
