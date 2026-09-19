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
import cats.data.{EitherT, NonEmptyList}
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

  test("KafkaClient transforms settings, resources, and returned algebras between effects"):
    val topic            = Topic.from("events").toOption.get
    val group            = ConsumerGroup.from("workers").toOption.get
    val client           = source.imapK(ioToErrorIO)(errorIOToIO)
    val serializer       = Serializer.const[ErrorIO, String](Some(Chunk.array(Array[Byte](1))))
    val deserializer     = Deserializer.instance[ErrorIO, String]((_, _, _) => EitherT.rightT("value"))
    val producerSettings = ProducerSettings(ClientSettings(NonEmptyList.one("localhost:9092")), serializer, serializer)
    val consumerSettings = ConsumerSettings(ClientSettings(NonEmptyList.one("localhost:9092")), group, deserializer, deserializer)
    val record           = ProducerRecord(topic, "key", "value")

    for
      produced <- client.producer(producerSettings).use(_.produce(NonEmptyList.one(record))).value
      consumed <- client.consumer(consumerSettings, Subscription.Topics(NonEmptyList.one(topic))).use(_.records.compile.drain).value
    yield
      assertEquals(produced, Right(ProducerResult(NonEmptyList.one(record), Nil)))
      assertEquals(consumed, Right(()))

  private val source: KafkaClient[IO] =
    new KafkaClient[IO]:
      override def producer[K, V](settings: ProducerSettings[IO, K, V]): Resource[IO, KafkaProducer[IO, K, V]] =
        Resource.pure:
          new KafkaProducer[IO, K, V]:
            override def produce(records: NonEmptyList[ProducerRecord[K, V]]): IO[ProducerResult[K, V]] =
              val first = records.head
              (
                settings.keySerializer.serialize(first.topic, first.headers, first.key),
                settings.valueSerializer.serialize(first.topic, first.headers, first.value)
              ).tupled.as(ProducerResult(records, Nil))

      override def consumer[K, V](settings: ConsumerSettings[IO, K, V], subscription: Subscription): Resource[IO, KafkaConsumer[IO, K, V]] =
        Resource.pure:
          new KafkaConsumer[IO, K, V]:
            override val records: Stream[IO, CommittableConsumerRecord[IO, K, V]]                                 = Stream.empty
            override def assignment: IO[Set[TopicPartition]]                                                      = IO.pure(Set.empty)
            override def committed(topicPartitions: Set[TopicPartition]): IO[Map[TopicPartition, Option[Offset]]] =
              IO.pure(topicPartitions.map(_ -> None).toMap)
            override def seek(topicPartition: TopicPartition, offset: Offset): IO[Unit] = IO.unit
