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
import fs2.Stream
import munit.CatsEffectSuite

final class AssignmentChangesSuite extends CatsEffectSuite:
  private val topic           = Topic.from("events").toOption.get
  private val firstPartition  = TopicPartition(topic, Partition.from(0).toOption.get)
  private val secondPartition = TopicPartition(topic, Partition.from(1).toOption.get)

  test("assignmentChanges emits the current assignment immediately and suppresses unchanged polls"):
    for
      current   <- Ref[IO].of(Set(firstPartition))
      firstRead <- Deferred[IO, Unit]
      consumer = recordingConsumer(current, firstRead)
      update   <- (firstRead.get >> IO.sleep(50.millis) >> current.set(Set(secondPartition))).start
      observed <- consumer.assignmentChanges(10.millis).take(2).compile.toList.timeout(5.seconds)
      _        <- update.joinWithNever
    yield assertEquals(observed, List(Set(firstPartition), Set(secondPartition)))

  private def recordingConsumer(current: Ref[IO, Set[TopicPartition]], firstRead: Deferred[IO, Unit]): KafkaConsumer[IO, Unit, Unit] =
    new KafkaConsumer[IO, Unit, Unit]:
      override val records: Stream[IO, CommittableConsumerRecord[IO, Unit, Unit]] = Stream.empty
      override def assignment: IO[Set[TopicPartition]]                            = current.get.flatTap(_ => firstRead.complete(()).void)
      override def committed(topicPartitions: Set[TopicPartition]): IO[Map[TopicPartition, Option[Offset]]] = IO.pure(Map.empty)
      override def beginningOffsets(topicPartitions: Set[TopicPartition]): IO[Map[TopicPartition, Offset]]  = IO.pure(Map.empty)
      override def endOffsets(topicPartitions: Set[TopicPartition]): IO[Map[TopicPartition, Offset]]        = IO.pure(Map.empty)
      override def seek(topicPartition: TopicPartition, offset: Offset): IO[Unit]                           = IO.unit
