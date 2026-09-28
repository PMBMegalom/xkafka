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
import cats.data.{EitherT, NonEmptyList, NonEmptySet}
import cats.effect.{IO, Resource}
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import munit.CatsEffectSuite

final class KafkaClientMapKSuite extends CatsEffectSuite:
  private type ErrorIO[A] = EitherT[IO, String, A]

  private val ioToErrorIO: FunctionK[IO, ErrorIO] =
    new FunctionK[IO, ErrorIO]:
      override def apply[A](value: IO[A]): ErrorIO[A] = EitherT.liftF(value)

  private val errorIOToIO: FunctionK[ErrorIO, IO] =
    new FunctionK[ErrorIO, IO]:
      override def apply[A](value: ErrorIO[A]): IO[A] = value.value.flatMap(_.leftMap(new RuntimeException(_)).liftTo[IO])

  test("a transaction transformed between effects records offsets against the group they came from"):
    val topic           = Topic.from("events").toOption.get
    val clientSettings  = ClientSettings.from(NonEmptyList.one("localhost:9092")).toOption.get
    val transactionalId = TransactionalId.from("writer").toOption.get
    val serializer      = Serializer.const[ErrorIO, String](Some(Chunk.array(Array[Byte](1))))
    val settings        = TransactionalProducerSettings.from(clientSettings, transactionalId, serializer, serializer).toOption.get
    val record          = ProducerRecord(topic, "key", "value")

    val committer =
      new OffsetCommitter[ErrorIO]:
        override def commit(offsets: Map[TopicPartition, Offset]): ErrorIO[Unit] = EitherT.pure(())

        override private[xkafka] val membership: GroupMembership[ErrorIO] =
          GroupMembership.Backend(EitherT.pure(TestGroupHandle("workers")), _ => EitherT.pure(()))

    IO.ref(List.empty[GroupHandle]).flatMap: seen =>
      val client = sourceRecording(value => seen.update(_ :+ value)).imapK(ioToErrorIO)(errorIOToIO)
      val batch  = CommittableOffsetBatch.empty[ErrorIO].updated(committableOffset(topic, committer))

      client.transactionalProducer(settings)
        .use(producer => producer.transactionally(transaction => transaction.produce(NonEmptyList.one(record)) *> transaction.commitOffsets(batch)))
        .value.flatMap(result => seen.get.map(result -> _))
    .map: (result, recorded) =>
      assertEquals(result, Right(()))
      assertEquals(recorded, List(TestGroupHandle("workers")))

  /** Stands in for a backend's own handle, which only that backend can name. */
  private final case class TestGroupHandle(group: String) extends GroupHandle

  private def committableOffset(topic: Topic, from: OffsetCommitter[ErrorIO]): CommittableOffset[ErrorIO] =
    new CommittableOffset[ErrorIO]:
      override val topicPartition: TopicPartition      = TopicPartition(topic, Partition.from(0).toOption.get)
      override val nextOffset: Offset                  = Offset.from(1L).toOption.get
      override val committer: OffsetCommitter[ErrorIO] = from

  test("KafkaClient transforms settings, resources, and returned algebras between effects"):
    val topic            = Topic.from("events").toOption.get
    val group            = ConsumerGroup.from("workers").toOption.get
    val client           = source.imapK(ioToErrorIO)(errorIOToIO)
    val serializer       = Serializer.const[ErrorIO, String](Some(Chunk.array(Array[Byte](1))))
    val deserializer     = Deserializer.instance[ErrorIO, String]((_, _, _) => EitherT.rightT("value"))
    val clientSettings   = ClientSettings.from(NonEmptyList.one("localhost:9092")).toOption.get
    val producerSettings = ProducerSettings.from(clientSettings, serializer, serializer).toOption.get
    val consumerSettings = ConsumerSettings.from(clientSettings, group, deserializer, deserializer).toOption.get
    val record           = ProducerRecord(topic, "key", "value")

    for
      produced <- client.producer(producerSettings).use(_.produceAndAwait(NonEmptyList.one(record))).value
      consumed <- client.consumer(consumerSettings, Selection.Topics(NonEmptySet.one(topic))).use(_.records.compile.drain).value
    yield
      assertEquals(produced, Right(ProducerResult(NonEmptyList.one(record -> None))))
      assertEquals(consumed, Right(()))

  private val source: KafkaClient[IO] = sourceRecording(_ => IO.unit)

  private def sourceRecording(record: GroupHandle => IO[Unit]): KafkaClient[IO] =
    new KafkaClient[IO]:
      override def transactionalProducer[K, V](
          settings: TransactionalProducerSettings[IO, K, V]
      ): Resource[IO, KafkaTransactionalProducer[IO, K, V]] =
        Resource.pure:
          new KafkaTransactionalProducer[IO, K, V]:
            override def transactionally[A](use: Transaction[IO, K, V] => IO[A]): IO[A] =
              use:
                new Transaction[IO, K, V]:
                  override def produce(records: NonEmptyList[ProducerRecord[K, V]]): IO[ProducerResult[K, V]] =
                    IO.pure(ProducerResult(records.map(_ -> None)))

                  override def commitOffsets(batch: CommittableOffsetBatch[IO]): IO[Unit] =
                    batch.offsets.toList.traverse_ { (committer, _) =>
                      committer.membership.handle.fold(IO.raiseError[Unit](new IllegalStateException("no membership")))(_.acquire.flatMap(record))
                    }

      override def producer[K, V](settings: ProducerSettings[IO, K, V]): Resource[IO, KafkaProducer[IO, K, V]] =
        Resource.pure:
          new KafkaProducer[IO, K, V]:
            override def produce(records: NonEmptyList[ProducerRecord[K, V]]): IO[IO[ProducerResult[K, V]]] =
              val first = records.head
              (
                settings.keySerializer.serialize(first.topic, first.headers, first.key),
                settings.valueSerializer.serialize(first.topic, first.headers, first.value)
              ).tupled.as(IO.pure(ProducerResult(records.map(_ -> None))))

            override def partitionsFor(value: Topic): IO[Set[Partition]] = IO.pure(Set.empty)

      override def admin(settings: ClientSettings): Resource[IO, KafkaAdminClient[IO]] =
        Resource.pure:
          new KafkaAdminClient[IO]:
            override def createTopics(topics: NonEmptySet[NewTopic]): IO[Unit]                      = IO.unit
            override def deleteTopics(topics: NonEmptySet[Topic]): IO[Unit]                         = IO.unit
            override def createPartitions(topic: Topic, count: Int): IO[Unit]                       = IO.unit
            override def describeTopics(topics: NonEmptySet[Topic]): IO[Map[Topic, Set[Partition]]] = IO.pure(Map.empty)

      override def consumer[K, V](settings: ConsumerSettings[IO, K, V], selection: Selection): Resource[IO, KafkaConsumer[IO, K, V]] =
        Resource.pure:
          new KafkaConsumer[IO, K, V]:
            override val records: Stream[IO, CommittableConsumerRecord[IO, K, V]]                                 = Stream.empty
            override def assignment: IO[Set[TopicPartition]]                                                      = IO.pure(Set.empty)
            override val assignmentChanges: Stream[IO, Set[TopicPartition]]                                       = Stream.empty
            override def stopConsuming: IO[Unit]                                                                  = IO.unit
            override def committed(topicPartitions: Set[TopicPartition]): IO[Map[TopicPartition, Option[Offset]]] =
              IO.pure(topicPartitions.map(_ -> None).toMap)
            override def beginningOffsets(topicPartitions: Set[TopicPartition]): IO[Map[TopicPartition, Offset]] = IO.pure(Map.empty)
            override def endOffsets(topicPartitions: Set[TopicPartition]): IO[Map[TopicPartition, Offset]]       = IO.pure(Map.empty)
            override def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Timestamp]): IO[Map[TopicPartition, Option[Offset]]] =
              IO.pure(Map.empty)
            override def partitionsFor(topic: Topic): IO[Set[Partition]]                 = IO.pure(Set.empty)
            override def listTopics: IO[Map[Topic, Set[Partition]]]                      = IO.pure(Map.empty)
            override def seek(topicPartition: TopicPartition, offset: Offset): IO[Unit]  = IO.unit
            override def seekToBeginning(topicPartitions: Set[TopicPartition]): IO[Unit] = IO.unit
            override def seekToEnd(topicPartitions: Set[TopicPartition]): IO[Unit]       = IO.unit
            override def position(topicPartition: TopicPartition): IO[Option[Offset]]    = IO.pure(None)
