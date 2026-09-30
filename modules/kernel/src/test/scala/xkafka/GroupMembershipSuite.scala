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

/** A batch keeps the membership each offset was read under, so a transaction records an offset under that one and not the consumer's current one. */
final class GroupMembershipSuite extends CatsEffectSuite:
  private val topic  = Topic.from("events").toOption.get
  private val first  = TopicPartition(topic, Partition.from(0).toOption.get)
  private val second = TopicPartition(topic, Partition.from(1).toOption.get)

  test("a transaction records each offset under the membership it was read under"):
    val batch = CommittableOffsetBatch.fromFoldable(List(offset(first, 5L, membership("earlier")), offset(second, 3L, membership("later"))))

    GroupMembership.resolve(batch).use(IO.pure).map: resolved =>
      val expected =
        Map[GroupHandle, Map[TopicPartition, Offset]](KeyedHandle("earlier") -> Map(first -> at(5L)), KeyedHandle("later") -> Map(second -> at(3L)))
      assertEquals(resolved.toMap, expected)

  test("offsets read under one membership share one handle, however their effect was transformed"):
    for
      acquired <- Ref[IO].of(0)
      shared = new GroupMembership.Backend[IO]("shared", acquired.update(_ + 1).as(KeyedHandle("shared")), _ => IO.unit)
      batch  = CommittableOffsetBatch.fromFoldable(List(offset(first, 5L, shared), offset(second, 3L, shared).mapK(FunctionK.id[IO])))
      resolved <- GroupMembership.resolve(batch).use(IO.pure)
      count    <- acquired.get
    yield
      assertEquals(count, 1)
      assertEquals(resolved.map(_._2), List(Map(first -> at(5L), second -> at(3L))))

  test("a batch keeps the membership of the higher offset, and of the later one where both are equal"):
    val higherEarlier = CommittableOffsetBatch.fromFoldable(List(offset(first, 5L, membership("earlier")), offset(first, 3L, membership("later"))))
    val equal         = CommittableOffsetBatch.fromFoldable(List(offset(first, 5L, membership("earlier")), offset(first, 5L, membership("later"))))

    assertEquals(higherEarlier.memberships, Map(membership("earlier") -> Map(first -> at(5L))))
    assertEquals(equal.memberships, Map(membership("later") -> Map(first -> at(5L))))

  test("a plain commit sends one merged commit per committer, whatever the memberships"):
    for
      commits <- Ref[IO].of(List.empty[Map[TopicPartition, Offset]])
      owner = recording(commits)
      _ <-
        CommittableOffsetBatch.fromFoldable(List(offset(first, 5L, membership("earlier"), owner), offset(first, 3L, membership("later"), owner)))
          .commit
      sent <- commits.get
    yield assertEquals(sent, List(Map(first -> at(5L))))

  test("a batch commits each partition with the lease of the offset it keeps"):
    for
      received <- Ref[IO].of(List.empty[Map[TopicPartition, (Offset, Option[Lease])]])
      owner =
        new OffsetCommitter[IO]:
          override def commit(offsets: Map[TopicPartition, Offset]): IO[Unit] = IO.unit

          override private[xkafka] def commitLeased(offsets: Map[TopicPartition, (Offset, Option[Lease])]): IO[Unit] = received.update(_ :+ offsets)
      earlier = Lease(LeaseKey("events", 0), 1L)
      later   = Lease(LeaseKey("events", 0), 2L)
      other   = Lease(LeaseKey("events", 1), 3L)
      _ <-
        CommittableOffsetBatch
          .fromFoldable(List(leased(first, 3L, owner, Some(earlier)), leased(first, 5L, owner, Some(later)), leased(second, 2L, owner, Some(other))))
          .commit
      sent <- received.get
    yield assertEquals(sent, List(Map(first -> (at(5L), Some(later)), second -> (at(2L), Some(other)))))

  private def leased(partition: TopicPartition, value: Long, owner: OffsetCommitter[IO], read: Option[Lease]): CommittableOffset[IO] =
    new CommittableOffset[IO]:
      override def topicPartition: TopicPartition = partition

      override def nextOffset: Offset = at(value)

      override def committer: OffsetCommitter[IO] = owner

      override private[xkafka] def lease: Option[Lease] = read

  private final case class KeyedHandle(key: String) extends GroupHandle

  private def membership(key: String): GroupMembership[IO] = new GroupMembership.Backend[IO](key, IO.pure(KeyedHandle(key)), _ => IO.unit)

  private def recording(commits: Ref[IO, List[Map[TopicPartition, Offset]]]): OffsetCommitter[IO] =
    new OffsetCommitter[IO]:
      override def commit(offsets: Map[TopicPartition, Offset]): IO[Unit] = commits.update(_ :+ offsets)

  private val unused: OffsetCommitter[IO] =
    new OffsetCommitter[IO]:
      override def commit(offsets: Map[TopicPartition, Offset]): IO[Unit] = IO.unit

  private def offset(partition: TopicPartition, value: Long, read: GroupMembership[IO], owner: OffsetCommitter[IO] = unused): CommittableOffset[IO] =
    new CommittableOffset[IO]:
      override def topicPartition: TopicPartition = partition

      override def nextOffset: Offset = at(value)

      override def committer: OffsetCommitter[IO] = owner

      override private[xkafka] def membership: GroupMembership[IO] = read

  private def at(value: Long): Offset = Offset.from(value).toOption.get
