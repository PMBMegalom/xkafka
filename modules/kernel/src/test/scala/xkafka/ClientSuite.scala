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
import cats.data.NonEmptyList
import cats.effect.SyncIO
import cats.syntax.all.*
import cats.tagless.FunctorK
import fs2.{Chunk, Stream}
import munit.FunSuite

final class ClientSuite extends FunSuite:
  private type ErrorOr[A] = Either[String, A]

  private val topic          = Topic.from("events").toOption.get
  private val partition      = Partition.from(0).toOption.get
  private val offset         = Offset.from(0L).toOption.get
  private val nextOffset     = Offset.from(1L).toOption.get
  private val group          = ConsumerGroup.from("workers").toOption.get
  private val clientSettings = ClientSettings(NonEmptyList.one("localhost:9092"))
  private val producerRecord = ProducerRecord(topic, "key", "value")
  private val consumerRecord = ConsumerRecord(TopicPartition(topic, partition), offset, None, "key", "value", Headers.empty)

  private val optionToErrorOr: FunctionK[Option, ErrorOr] =
    new FunctionK[Option, ErrorOr]:
      override def apply[A](value: Option[A]): ErrorOr[A] = value.toRight("empty")

  private val optionToSyncIO: FunctionK[Option, SyncIO] =
    new FunctionK[Option, SyncIO]:
      override def apply[A](value: Option[A]): SyncIO[A] = value.fold(SyncIO.raiseError(new NoSuchElementException("empty")))(SyncIO.pure)

  test("producer and consumer settings support natural transformations"):
    val serializer     = Serializer.instance[Option, String]((_, _, value) => Some(Some(Chunk.array(value.getBytes("UTF-8")))))
    val deserializer   = Deserializer.instance[Option, String]((_, _, bytes) => bytes.map(chunk => new String(chunk.toArray, "UTF-8")))
    val producer       = ProducerSettings(clientSettings, serializer, serializer)
    val consumer       = ConsumerSettings(clientSettings, group, deserializer, deserializer)
    val mappedProducer = FunctorK[[F[_]] =>> ProducerSettings[F, String, String]].mapK(producer)(optionToErrorOr)
    val mappedConsumer = FunctorK[[F[_]] =>> ConsumerSettings[F, String, String]].mapK(consumer)(optionToErrorOr)
    val bytes          = Some(Chunk.array("value".getBytes("UTF-8")))

    assertEquals(mappedProducer.valueSerializer.serialize(topic, Headers.empty, "value"), Right(bytes))
    assertEquals(mappedConsumer.valueDeserializer.deserialize(topic, Headers.empty, bytes), Right("value"))

  test("committable offsets and records support natural transformations"):
    val source       = committableOffset
    val record       = CommittableConsumerRecord(consumerRecord, source)
    val mapped       = FunctorK[CommittableOffset].mapK(source)(optionToErrorOr)
    val mappedRecord = FunctorK[[F[_]] =>> CommittableConsumerRecord[F, String, String]].mapK(record)(optionToErrorOr)

    assertEquals(mapped.topicPartition, source.topicPartition)
    assertEquals(mapped.nextOffset, source.nextOffset)
    assertEquals(mapped.commit, Right(()))
    assertEquals(mappedRecord.record, consumerRecord)
    assertEquals(mappedRecord.offset.commit, Right(()))

  test("partition records support natural transformations"):
    val sourceRecord = CommittableConsumerRecord(consumerRecord, committableOffset)
    val source       = PartitionRecords(consumerRecord.topicPartition, Stream.emit(sourceRecord).covary[Option])
    val mapped       = FunctorK[[F[_]] =>> PartitionRecords[F, String, String]].mapK(source)(optionToSyncIO)

    assertEquals(mapped.topicPartition, consumerRecord.topicPartition)
    assertEquals(mapped.records.compile.toList.unsafeRunSync().map(_.record), List(consumerRecord))

  test("offset batches retain the greatest next offset per topic-partition"):
    val otherPartition = Partition.from(1).toOption.get
    val laterOffset    = Offset.from(2L).toOption.get
    val batch          =
      CommittableOffsetBatch.fromFoldable(NonEmptyList.of(committableOffset, offsetAt(partition, laterOffset), offsetAt(otherPartition, nextOffset)))

    assertEquals(batch.size, 2)
    assertEquals(
      batch.offsets(optionCommitter),
      Map(TopicPartition(topic, partition) -> laterOffset, TopicPartition(topic, otherPartition) -> nextOffset)
    )
    assertEquals(batch.commit, Some(()))

  test("offset batches support natural transformations"):
    val batch  = CommittableOffsetBatch.fromFoldable(NonEmptyList.one(committableOffset))
    val mapped = FunctorK[CommittableOffsetBatch].mapK(batch)(optionToErrorOr)

    assertEquals(mapped.commit, Right(()))

  test("individually transformed offsets retain their shared committer"):
    val laterOffset = Offset.from(2L).toOption.get
    val offsets     = NonEmptyList.of(committableOffset, offsetAt(partition, laterOffset)).map(_.mapK(optionToErrorOr))
    val batch       = CommittableOffsetBatch.fromFoldable(offsets)

    assertEquals(batch.offsets.size, 1)
    assertEquals(batch.size, 1)
    assertEquals(batch.offsets.valuesIterator.flatMap(_.valuesIterator).toList, List(laterOffset))
    assertEquals(batch.commit, Right(()))

  test("KafkaProducer supports natural transformations"):
    val source =
      new KafkaProducer[Option, String, String]:
        override def produce(records: NonEmptyList[ProducerRecord[String, String]]): Option[ProducerResult[String, String]] =
          Some(ProducerResult(records, Nil))
    val mapped = FunctorK[[F[_]] =>> KafkaProducer[F, String, String]].mapK(source)(optionToErrorOr)

    assertEquals(mapped.produce(NonEmptyList.one(producerRecord)), Right(ProducerResult(NonEmptyList.one(producerRecord), Nil)))

  test("KafkaConsumer transforms both its stream and committable offsets"):
    val sourceRecord = CommittableConsumerRecord(consumerRecord, committableOffset)
    val source       =
      new KafkaConsumer[Option, String, String]:
        override val records: Stream[Option, CommittableConsumerRecord[Option, String, String]] = Stream.emit(sourceRecord).covary[Option]
        override def assignment: Option[Set[TopicPartition]]                                    = Some(Set(consumerRecord.topicPartition))
        override def committed(topicPartitions: Set[TopicPartition]): Option[Map[TopicPartition, Option[Offset]]] =
          Some(topicPartitions.map(_ -> Some(nextOffset)).toMap)
        override def seek(topicPartition: TopicPartition, offset: Offset): Option[Unit] = Some(())
    val mapped = FunctorK[[F[_]] =>> KafkaConsumer[F, String, String]].mapK(source)(optionToSyncIO)

    val result =
      (
        mapped.records.compile.lastOrError,
        mapped.assignment,
        mapped.committed(Set(consumerRecord.topicPartition)),
        mapped.seek(consumerRecord.topicPartition, offset)
      ).flatMapN((record, assignment, committed, _) => record.offset.commit.as((record.record, assignment, committed))).unsafeRunSync()

    assertEquals(result, (consumerRecord, Set(consumerRecord.topicPartition), Map(consumerRecord.topicPartition -> Some(nextOffset))))

  private def committableOffset: CommittableOffset[Option] = offsetAt(partition, nextOffset)

  private val optionCommitter: OffsetCommitter[Option] =
    new OffsetCommitter[Option]:
      override def commit(offsets: Map[TopicPartition, Offset]): Option[Unit] = Some(())

  private def offsetAt(partition: Partition, nextOffsetValue: Offset): CommittableOffset[Option] =
    new CommittableOffset[Option]:
      override val topicPartition: TopicPartition     = TopicPartition(topic, partition)
      override val nextOffset: Offset                 = nextOffsetValue
      override val committer: OffsetCommitter[Option] = optionCommitter
