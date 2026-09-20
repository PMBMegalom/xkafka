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

import cats.Id
import munit.ScalaCheckSuite
import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Prop.forAll

/** `CommittableOffsetBatch` merges offsets as a join-semilattice keyed by topic-partition, per committer. */
final class OffsetBatchPropertySuite extends ScalaCheckSuite, ModelGenerators:
  private val committer: OffsetCommitter[Id] =
    new OffsetCommitter[Id]:
      override def commit(offsets: Map[TopicPartition, Offset]): Id[Unit] = ()

  private val otherCommitter: OffsetCommitter[Id] =
    new OffsetCommitter[Id]:
      override def commit(offsets: Map[TopicPartition, Offset]): Id[Unit] = ()

  private def offsetAt(topicPartition: TopicPartition, next: Offset, owner: OffsetCommitter[Id]): CommittableOffset[Id] =
    val partition = topicPartition
    new CommittableOffset[Id]:
      override val topicPartition: TopicPartition = partition
      override val nextOffset: Offset             = next
      override val committer: OffsetCommitter[Id] = owner

  private given Arbitrary[CommittableOffset[Id]] =
    Arbitrary:
      for
        topicPartition <- collidingTopicPartitions
        offset         <- smallOffsets
        owner          <- Gen.oneOf(committer, otherCommitter)
      yield offsetAt(topicPartition, offset, owner)

  private def batchOf(offsets: List[CommittableOffset[Id]]): CommittableOffsetBatch[Id] = CommittableOffsetBatch.fromFoldable(offsets)

  private def flattened(batch: CommittableOffsetBatch[Id]): Map[(OffsetCommitter[Id], TopicPartition), Offset] =
    batch.offsets.flatMap((owner, offsets) => offsets.map((topicPartition, offset) => (owner, topicPartition) -> offset))

  property("a batch keeps the greatest offset for each committer and topic-partition"):
    forAll: (offsets: List[CommittableOffset[Id]]) =>
      val expected =
        offsets.groupBy(offset => (offset.committer, offset.topicPartition)).map((key, group) => key -> group.map(_.nextOffset).maxBy(_.value))
      assertEquals(flattened(batchOf(offsets)), expected)

  property("merging batches is the same as batching the concatenation"):
    forAll: (left: List[CommittableOffset[Id]], right: List[CommittableOffset[Id]]) =>
      assertEquals(flattened(batchOf(left).updated(batchOf(right))), flattened(batchOf(left ++ right)))

  property("merging batches is commutative"):
    forAll: (left: List[CommittableOffset[Id]], right: List[CommittableOffset[Id]]) =>
      assertEquals(flattened(batchOf(left).updated(batchOf(right))), flattened(batchOf(right).updated(batchOf(left))))

  property("merging batches is associative"):
    forAll: (a: List[CommittableOffset[Id]], b: List[CommittableOffset[Id]], c: List[CommittableOffset[Id]]) =>
      val left  = batchOf(a).updated(batchOf(b)).updated(batchOf(c))
      val right = batchOf(a).updated(batchOf(b).updated(batchOf(c)))
      assertEquals(flattened(left), flattened(right))

  property("merging a batch with itself changes nothing"):
    forAll: (offsets: List[CommittableOffset[Id]]) =>
      val batch = batchOf(offsets)
      assertEquals(flattened(batch.updated(batch)), flattened(batch))

  property("the empty batch is the merge identity"):
    forAll: (offsets: List[CommittableOffset[Id]]) =>
      val batch = batchOf(offsets)
      assertEquals(flattened(batch.updated(CommittableOffsetBatch.empty[Id])), flattened(batch))
      assertEquals(flattened(CommittableOffsetBatch.empty[Id].updated(batch)), flattened(batch))

  property("size counts distinct topic-partitions across every committer"):
    forAll: (offsets: List[CommittableOffset[Id]]) =>
      assertEquals(batchOf(offsets).size, offsets.map(offset => (offset.committer, offset.topicPartition)).distinct.size)
