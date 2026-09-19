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

import cats.effect.{Deferred, IO, Ref}
import cats.effect.std.Queue
import fs2.Stream
import munit.CatsEffectSuite

final class PartitionRecordsSuite extends CatsEffectSuite:
  private val topic           = Topic.from("events").toOption.get
  private val firstPartition  = TopicPartition(topic, Partition.from(0).toOption.get)
  private val secondPartition = TopicPartition(topic, Partition.from(1).toOption.get)

  test("partitionedRecords preserves records across assignment changes and closes revoked streams"):
    for
      assignment    <- Ref[IO].of(Set(firstPartition))
      input         <- Queue.unbounded[IO, CommittableConsumerRecord[IO, String, String]]
      firstOpened   <- Deferred[IO, Unit]
      firstConsumed <- Deferred[IO, Unit]
      consumer = testConsumer(assignment, input)
      observed <-
        consumer.partitionedRecords(10.millis, 2).evalMap: partition =>
          val records =
            if partition.topicPartition == firstPartition then
              Stream.exec(firstOpened.complete(()).void) ++ partition.records.evalTap(_ => firstConsumed.complete(()).void)
            else partition.records.take(1)
          records.compile.toList.map(partition.topicPartition -> _)
        .take(2).compile.toList.start
      _ <- firstOpened.get
      first  = record(firstPartition, 0L, "first")
      second = record(secondPartition, 0L, "second")
      _      <- input.offer(first)
      _      <- firstConsumed.get
      _      <- assignment.set(Set(secondPartition))
      _      <- input.offer(second)
      result <- observed.joinWithNever.timeout(5.seconds)
    yield
      assertEquals(result.map(_._1), List(firstPartition, secondPartition))
      assertEquals(result.map(_._2), List(List(first), List(second)))

  test("partitionedRecords rejects a non-positive queue bound"):
    for
      assignment <- Ref[IO].of(Set.empty[TopicPartition])
      input      <- Queue.unbounded[IO, CommittableConsumerRecord[IO, String, String]]
      result     <- testConsumer(assignment, input).partitionedRecords(10.millis, 0).compile.drain.attempt
    yield result match
      case Left(error: IllegalArgumentException) => assertEquals(error.getMessage, "maxQueuedRecords must be positive")
      case Left(error)                           => fail(s"unexpected error: $error")
      case Right(())                             => fail("expected an invalid queue bound to fail")

  private def testConsumer(
      currentAssignment: Ref[IO, Set[TopicPartition]],
      input: Queue[IO, CommittableConsumerRecord[IO, String, String]]
  ): KafkaConsumer[IO, String, String] =
    new KafkaConsumer[IO, String, String]:
      override val records: Stream[IO, CommittableConsumerRecord[IO, String, String]]                       = Stream.fromQueueUnterminated(input)
      override def assignment: IO[Set[TopicPartition]]                                                      = currentAssignment.get
      override def committed(topicPartitions: Set[TopicPartition]): IO[Map[TopicPartition, Option[Offset]]] = IO.pure(Map.empty)
      override def seek(topicPartition: TopicPartition, offset: Offset): IO[Unit]                           = IO.unit

  private def record(topicPartition: TopicPartition, offsetValue: Long, value: String): CommittableConsumerRecord[IO, String, String] =
    val partition       = topicPartition
    val offset          = Offset.from(offsetValue).toOption.get
    val followingOffset = offset.next.toOption.get
    val committable     =
      new CommittableOffset[IO]:
        override val topicPartition: TopicPartition = partition
        override val nextOffset: Offset             = followingOffset
        override val committer: OffsetCommitter[IO] =
          new OffsetCommitter[IO]:
            override def commit(offsets: Map[TopicPartition, Offset]): IO[Unit] = IO.unit
    CommittableConsumerRecord(ConsumerRecord(topicPartition, offset, None, "key", value, Headers.empty), committable)
