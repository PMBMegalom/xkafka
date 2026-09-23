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

/** Stops and restarts delivery for individual partitions.
  *
  * Kafka holds the position a paused partition had reached, so resuming continues from the record after the last one handed over, and nothing is lost
  * by pausing.
  */
private[xkafka] trait PartitionPausing[F[_]]:
  def pause(topicPartitions: Set[TopicPartition]): F[Unit]

  def resume(topicPartitions: Set[TopicPartition]): F[Unit]

private[xkafka] object PartitionPausing:
  /** For a backend that feeds each partition on its own, where there is nothing to hold back. */
  def noop[F[_]](using F: Applicative[F]): PartitionPausing[F] =
    new PartitionPausing[F]:
      override def pause(topicPartitions: Set[TopicPartition]): F[Unit] = F.unit

      override def resume(topicPartitions: Set[TopicPartition]): F[Unit] = F.unit
