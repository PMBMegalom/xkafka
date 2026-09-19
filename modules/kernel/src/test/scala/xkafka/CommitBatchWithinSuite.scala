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

import scala.concurrent.duration.*

import cats.effect.{IO, Ref}
import fs2.Stream
import munit.CatsEffectSuite

final class CommitBatchWithinSuite extends CatsEffectSuite:
  private val topic           = Topic.from("topic").toOption.get
  private val firstPartition  = Partition.from(0).toOption.get
  private val secondPartition = Partition.from(1).toOption.get
  private val first           = Offset.from(1L).toOption.get
  private val second          = Offset.from(2L).toOption.get

  test("commitBatchWithin commits when the batch reaches its size limit"):
    for
      committed <- Ref[IO].of(Vector.empty[Map[TopicPartition, Offset]])
      committer = recordingCommitter(committed)
      offsets   =
        List(offsetAt(firstPartition, first, committer), offsetAt(firstPartition, second, committer), offsetAt(secondPartition, first, committer))
      _      <- Stream.emits(offsets).covary[IO].through(commitBatchWithin(2, 1.hour)).compile.drain
      result <- committed.get
    yield assertEquals(result, Vector(Map(TopicPartition(topic, firstPartition) -> second), Map(TopicPartition(topic, secondPartition) -> first)))

  test("commitBatchWithin commits when its time window elapses"):
    for
      committed <- Ref[IO].of(Vector.empty[Map[TopicPartition, Offset]])
      committer = recordingCommitter(committed)
      offset    = offsetAt(firstPartition, first, committer)
      _ <- (Stream.emit(offset).covary[IO] ++ Stream.never[IO]).through(commitBatchWithin(10, 50.millis)).take(1).compile.drain.timeout(5.seconds)
      result <- committed.get
    yield assertEquals(result, Vector(Map(TopicPartition(topic, firstPartition) -> first)))

  private def recordingCommitter(committed: Ref[IO, Vector[Map[TopicPartition, Offset]]]): OffsetCommitter[IO] =
    new OffsetCommitter[IO]:
      override def commit(offsets: Map[TopicPartition, Offset]): IO[Unit] = committed.update(_ :+ offsets)

  private def offsetAt(partition: Partition, nextOffsetValue: Offset, offsetCommitter: OffsetCommitter[IO]): CommittableOffset[IO] =
    new CommittableOffset[IO]:
      override val topicPartition: TopicPartition = TopicPartition(topic, partition)
      override val nextOffset: Offset             = nextOffsetValue
      override val committer: OffsetCommitter[IO] = offsetCommitter
