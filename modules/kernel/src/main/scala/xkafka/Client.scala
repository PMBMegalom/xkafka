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

import cats.{Applicative, Foldable}
import cats.arrow.FunctionK
import cats.data.NonEmptyList
import cats.tagless.FunctorK
import fs2.Stream

final case class ClientSettings(bootstrapServers: NonEmptyList[String], clientId: Option[String] = None, properties: Map[String, String] = Map.empty)

final case class ProducerSettings[F[_], K, V](
    client: ClientSettings,
    keySerializer: Serializer[F, K],
    valueSerializer: Serializer[F, V],
    properties: Map[String, String] = Map.empty
):
  def mapK[G[_]](fk: FunctionK[F, G]): ProducerSettings[G, K, V] =
    ProducerSettings(client, keySerializer.mapK(fk), valueSerializer.mapK(fk), properties)

object ProducerSettings:
  given [K, V]: FunctorK[[F[_]] =>> ProducerSettings[F, K, V]] with
    override def mapK[F[_], G[_]](settings: ProducerSettings[F, K, V])(fk: FunctionK[F, G]): ProducerSettings[G, K, V] = settings.mapK(fk)

enum AutoOffsetReset:
  case Earliest, Latest

final case class ConsumerSettings[F[_], K, V](
    client: ClientSettings,
    groupId: ConsumerGroup,
    keyDeserializer: Deserializer[F, K],
    valueDeserializer: Deserializer[F, V],
    autoOffsetReset: AutoOffsetReset = AutoOffsetReset.Latest,
    properties: Map[String, String] = Map.empty
):
  def mapK[G[_]](fk: FunctionK[F, G]): ConsumerSettings[G, K, V] =
    ConsumerSettings(client, groupId, keyDeserializer.mapK(fk), valueDeserializer.mapK(fk), autoOffsetReset, properties)

object ConsumerSettings:
  given [K, V]: FunctorK[[F[_]] =>> ConsumerSettings[F, K, V]] with
    override def mapK[F[_], G[_]](settings: ConsumerSettings[F, K, V])(fk: FunctionK[F, G]): ConsumerSettings[G, K, V] = settings.mapK(fk)

enum Subscription:
  case Topics(topics: NonEmptyList[Topic])
  case Pattern(pattern: TopicPattern)

trait OffsetCommitter[F[_]]:
  self =>

  def commit(offsets: Map[TopicPartition, Offset]): F[Unit]

  final def mapK[G[_]](fk: FunctionK[F, G]): OffsetCommitter[G] = OffsetCommitter.transformed(self, fk)

object OffsetCommitter:
  given FunctorK[OffsetCommitter] with
    override def mapK[F[_], G[_]](committer: OffsetCommitter[F])(fk: FunctionK[F, G]): OffsetCommitter[G] = committer.mapK(fk)

  private def transformed[F[_], G[_]](committer: OffsetCommitter[F], fk: FunctionK[F, G]): OffsetCommitter[G] =
    TransformedOffsetCommitter(committer, fk)

  private final case class TransformedOffsetCommitter[F[_], G[_]](underlying: OffsetCommitter[F], fk: FunctionK[F, G]) extends OffsetCommitter[G]:
    override def commit(offsets: Map[TopicPartition, Offset]): G[Unit] = fk(underlying.commit(offsets))

trait CommittableOffset[F[_]]:
  self =>

  def topicPartition: TopicPartition

  /** The next offset to consume after this commit succeeds. */
  def nextOffset: Offset

  def committer: OffsetCommitter[F]

  final def commit: F[Unit] = committer.commit(Map(topicPartition -> nextOffset))

  final def mapK[G[_]](fk: FunctionK[F, G]): CommittableOffset[G] =
    new CommittableOffset[G]:
      override def topicPartition: TopicPartition = self.topicPartition

      override def nextOffset: Offset = self.nextOffset

      override def committer: OffsetCommitter[G] = self.committer.mapK(fk)

object CommittableOffset:
  given FunctorK[CommittableOffset] with
    override def mapK[F[_], G[_]](offset: CommittableOffset[F])(fk: FunctionK[F, G]): CommittableOffset[G] = offset.mapK(fk)

opaque type CommittableOffsetBatch[F[_]] = Map[OffsetCommitter[F], Map[TopicPartition, Offset]]

object CommittableOffsetBatch:
  def empty[F[_]]: CommittableOffsetBatch[F] = Map.empty

  def fromFoldable[F[_], G[_]: Foldable](offsets: G[CommittableOffset[F]]): CommittableOffsetBatch[F] =
    Foldable[G].foldLeft(offsets, empty[F])((batch, offset) => batch.updated(offset))

  extension [F[_]](batch: CommittableOffsetBatch[F])
    def updated(offset: CommittableOffset[F]): CommittableOffsetBatch[F] =
      batch.updated(offset.committer, include(batch.getOrElse(offset.committer, Map.empty), offset.topicPartition, offset.nextOffset))

    def updated(other: CommittableOffsetBatch[F]): CommittableOffsetBatch[F] =
      other.foldLeft(batch):
        case (result, (committer, offsets)) => result.updated(
            committer,
            offsets.foldLeft(result.getOrElse(committer, Map.empty)):
              case (committerOffsets, (topicPartition, offset)) => include(committerOffsets, topicPartition, offset)
          )

    def offsets: Map[OffsetCommitter[F], Map[TopicPartition, Offset]] = batch

    def size: Int = batch.valuesIterator.map(_.size).sum

    def commit(using F: Applicative[F]): F[Unit] =
      batch.foldLeft(F.unit):
        case (result, (committer, offsets)) => F.productR(result)(committer.commit(offsets))

    def mapK[G[_]](fk: FunctionK[F, G]): CommittableOffsetBatch[G] = batch.map((committer, offsets) => committer.mapK(fk) -> offsets)

  given FunctorK[CommittableOffsetBatch] with
    override def mapK[F[_], G[_]](batch: CommittableOffsetBatch[F])(fk: FunctionK[F, G]): CommittableOffsetBatch[G] = batch.mapK(fk)

  private def include(offsets: Map[TopicPartition, Offset], topicPartition: TopicPartition, offset: Offset): Map[TopicPartition, Offset] =
    offsets.updatedWith(topicPartition):
      case current @ Some(value) if value.value >= offset.value => current
      case Some(_) | None                                       => Some(offset)

final case class CommittableConsumerRecord[F[_], K, V](record: ConsumerRecord[K, V], offset: CommittableOffset[F]):
  def mapK[G[_]](fk: FunctionK[F, G]): CommittableConsumerRecord[G, K, V] = CommittableConsumerRecord(record, offset.mapK(fk))

object CommittableConsumerRecord:
  given [K, V]: FunctorK[[F[_]] =>> CommittableConsumerRecord[F, K, V]] with
    override def mapK[F[_], G[_]](record: CommittableConsumerRecord[F, K, V])(fk: FunctionK[F, G]): CommittableConsumerRecord[G, K, V] =
      record.mapK(fk)

trait KafkaProducer[F[_], K, V]:
  self =>

  def produce(records: NonEmptyList[ProducerRecord[K, V]]): F[ProducerResult[K, V]]

  final def mapK[G[_]](fk: FunctionK[F, G]): KafkaProducer[G, K, V] =
    new KafkaProducer[G, K, V]:
      override def produce(records: NonEmptyList[ProducerRecord[K, V]]): G[ProducerResult[K, V]] = fk(self.produce(records))

object KafkaProducer:
  given [K, V]: FunctorK[[F[_]] =>> KafkaProducer[F, K, V]] with
    override def mapK[F[_], G[_]](producer: KafkaProducer[F, K, V])(fk: FunctionK[F, G]): KafkaProducer[G, K, V] = producer.mapK(fk)

trait KafkaConsumer[F[_], K, V]:
  self =>

  def records: Stream[F, CommittableConsumerRecord[F, K, V]]

  final def mapK[G[_]](fk: FunctionK[F, G]): KafkaConsumer[G, K, V] =
    new KafkaConsumer[G, K, V]:
      override val records: Stream[G, CommittableConsumerRecord[G, K, V]] = self.records.map(_.mapK(fk)).translate(fk)

object KafkaConsumer:
  given [K, V]: FunctorK[[F[_]] =>> KafkaConsumer[F, K, V]] with
    override def mapK[F[_], G[_]](consumer: KafkaConsumer[F, K, V])(fk: FunctionK[F, G]): KafkaConsumer[G, K, V] = consumer.mapK(fk)
