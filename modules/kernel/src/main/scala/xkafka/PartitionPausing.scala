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

import cats.Applicative
import cats.arrow.FunctionK

/** Whether a backend stops and restarts delivery for individual partitions.
  *
  * Kafka holds the position a paused partition had reached, so resuming continues from the record after the last one handed over, and nothing is lost
  * by pausing.
  *
  * A backend with nothing to hold back carries that as a value, so transforming a consumer's effect keeps the answer without an instance for the new
  * effect.
  */
private[xkafka] sealed trait PartitionPausing[F[_]]:
  /** The capability itself, where a backend with nothing to hold back does nothing. */
  def orNoop(using Applicative[F]): PartitionPausing.Backend[F]

  def mapK[G[_]](fk: FunctionK[F, G]): PartitionPausing[G]

private[xkafka] object PartitionPausing:
  /** For a backend that feeds each partition on its own. */
  final case class Absent[F[_]]() extends PartitionPausing[F]:
    override def orNoop(using Applicative[F]): Backend[F] = noop

    override def mapK[G[_]](fk: FunctionK[F, G]): PartitionPausing[G] = Absent()

  /** For a backend that delivers every partition through one source. */
  trait Backend[F[_]] extends PartitionPausing[F]:
    self =>

    def pause(topicPartitions: Set[TopicPartition]): F[Unit]

    def resume(topicPartitions: Set[TopicPartition]): F[Unit]

    override final def orNoop(using Applicative[F]): Backend[F] = self

    override final def mapK[G[_]](fk: FunctionK[F, G]): PartitionPausing[G] =
      new Backend[G]:
        override def pause(topicPartitions: Set[TopicPartition]): G[Unit] = fk(self.pause(topicPartitions))

        override def resume(topicPartitions: Set[TopicPartition]): G[Unit] = fk(self.resume(topicPartitions))

  private def noop[F[_]](using F: Applicative[F]): Backend[F] =
    new Backend[F]:
      override def pause(topicPartitions: Set[TopicPartition]): F[Unit] = F.unit

      override def resume(topicPartitions: Set[TopicPartition]): F[Unit] = F.unit
