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
import cats.effect.{IO, Ref}
import munit.CatsEffectSuite

final class OffsetMetadataSuite extends CatsEffectSuite:
  private val topic     = Topic.from("events").toOption.get
  private val partition = TopicPartition(topic, Partition.from(0).toOption.get)
  private val epoch     = LeaderEpoch.from(4).toOption.get

  test("a leader epoch cannot be negative"):
    assertEquals(LeaderEpoch.from(-1), Left(ValidationError.NegativeLeaderEpoch(-1)))
    assertEquals(LeaderEpoch.from(0).map(_.value), Right(0))

  test("an offset commits with its leader epoch and no metadata until it is given some"):
    for
      sent <- Ref[IO].of(List.empty[Map[TopicPartition, OffsetAndMetadata]])
      offset = read(7L, recording(sent))
      _     <- offset.commit
      _     <- offset.withMetadata("noted").commit
      calls <- sent.get
    yield assertEquals(
      calls,
      List(Map(partition -> OffsetAndMetadata(at(7L), Some(epoch), "")), Map(partition -> OffsetAndMetadata(at(7L), Some(epoch), "noted")))
    )

  test("metadata survives a change of effect"):
    for
      sent  <- Ref[IO].of(List.empty[Map[TopicPartition, OffsetAndMetadata]])
      _     <- read(7L, recording(sent)).withMetadata("noted").mapK(FunctionK.id[IO]).commit
      calls <- sent.get
    yield assertEquals(calls, List(Map(partition -> OffsetAndMetadata(at(7L), Some(epoch), "noted"))))

  test("a batch commits the leader epoch and metadata of the offset it keeps"):
    for
      sent <- Ref[IO].of(List.empty[Map[TopicPartition, OffsetAndMetadata]])
      owner = recording(sent)
      batch = CommittableOffsetBatch.fromFoldable(List(read(9L, owner).withMetadata("kept"), read(3L, owner).withMetadata("dropped")))
      _     <- batch.commit
      calls <- sent.get
    yield
      assertEquals(calls, List(Map(partition -> OffsetAndMetadata(at(9L), Some(epoch), "kept"))))
      assertEquals(batch.offsetsAndMetadata, Map(owner -> Map(partition -> OffsetAndMetadata(at(9L), Some(epoch), "kept"))))

  test("committing bare offsets sends no leader epoch and no metadata"):
    for
      sent  <- Ref[IO].of(List.empty[Map[TopicPartition, OffsetAndMetadata]])
      _     <- recording(sent).commit(Map(partition -> at(2L)))
      calls <- sent.get
    yield assertEquals(calls, List(Map(partition -> OffsetAndMetadata(at(2L)))))

  private def at(value: Long): Offset = Offset.from(value).toOption.get

  private def recording(sent: Ref[IO, List[Map[TopicPartition, OffsetAndMetadata]]]): OffsetCommitter[IO] =
    new OffsetCommitter[IO]:
      override def commitWithMetadata(offsets: Map[TopicPartition, OffsetAndMetadata]): IO[Unit] = sent.update(_ :+ offsets)

  private def read(next: Long, owner: OffsetCommitter[IO]): CommittableOffset[IO] =
    new CommittableOffset[IO]:
      override def topicPartition: TopicPartition = partition

      override def nextOffset: Offset = at(next)

      override def leaderEpoch: Option[LeaderEpoch] = Some(epoch)

      override def committer: OffsetCommitter[IO] = owner
