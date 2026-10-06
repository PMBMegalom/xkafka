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
package downstream

import scala.concurrent.duration.*

import cats.data.{NonEmptyList, NonEmptyMap, ValidatedNel, NonEmptySet}
import cats.effect.IO
import cats.effect.IOApp
import cats.syntax.all.*

object DownstreamSmoke extends IOApp.Simple:
  private val bootstrapServer = sys.env.getOrElse(
    "XKAFKA_INTEGRATION_BOOTSTRAP_SERVERS",
    "127.0.0.1:19092"
  )

  private val utf8Serializer   = Serializer.utf8[IO]
  private val utf8Deserializer = Deserializer.utf8[IO]

  override val run: IO[Unit] =
    val suffix           = System.currentTimeMillis().toString
    val topic            = Topic.from(s"xkafka-downstream-$suffix").fold(error => throw new IllegalArgumentException(error.toString), identity)
    val group            = ConsumerGroup.from(s"xkafka-downstream-$suffix").fold(error => throw new IllegalArgumentException(error.toString), identity)
    val expected         = ProducerRecord(topic, "downstream-key", "downstream-value")
    val output           = Topic.from(s"xkafka-downstream-out-$suffix").fold(error => throw new IllegalArgumentException(error.toString), identity)
    val transactionalId  = TransactionalId.from(s"xkafka-downstream-$suffix").fold(error => throw new IllegalArgumentException(error.toString), identity)

    for
      client           <- validated(ClientSettings.from(NonEmptyList.one(bootstrapServer)))
      producerSettings <- validated(ProducerSettings.from(client, utf8Serializer, utf8Serializer))
      consumerSettings <- validated(ConsumerSettings.from(client, group, utf8Deserializer, utf8Deserializer, AutoOffsetReset.Earliest))
      produced <- KafkaClient[IO].producer(producerSettings).use(_.produceAndAwait(NonEmptyList.one(expected))).timeout(45.seconds)
      transactionalSettings <- validated(TransactionalProducerSettings.from(client, transactionalId, utf8Serializer, utf8Serializer))
      consumed <- KafkaClient[IO]
        .consumer(consumerSettings, Selection.Topics(NonEmptySet.one(topic)))
        .use: consumer =>
          KafkaClient[IO].transactionalProducer(transactionalSettings).use: producer =>
            for
              taken <- consumer.records.take(1).compile.lastOrError
              // The offsets a transaction records carry the committer they came from, so the group they belong to
              // needs no naming here.
              _ <- producer.transactionally: transaction =>
                transaction.produce(NonEmptyList.one(ProducerRecord(output, taken.record.key, taken.record.value))) *>
                  transaction.commitOffsets(CommittableOffsetBatch.empty[IO].updated(taken.offset))
            yield taken
        .timeout(90.seconds)
      _ <- IO.raiseUnless(produced.records.size == 1)(new AssertionError("producer did not acknowledge the record"))
      _ <- IO.raiseUnless(consumed.record.key == expected.key)(new AssertionError(s"unexpected key: ${consumed.record.key}"))
      _ <- IO.raiseUnless(consumed.record.value == expected.value)(new AssertionError(s"unexpected value: ${consumed.record.value}"))
      transacted <- KafkaClient[IO]
        .consumer(consumerSettings.withIsolationLevel(IsolationLevel.ReadCommitted), Selection.Topics(NonEmptySet.one(output)))
        .use(_.records.take(1).compile.lastOrError)
        .timeout(60.seconds)
      _ <- IO.raiseUnless(transacted.record.value == expected.value)(new AssertionError(s"unexpected transacted value: ${transacted.record.value}"))
      // The record was read, so the whole partition can go.
      cut = NonEmptyMap.one(consumed.offset.topicPartition, consumed.offset.nextOffset)
      deleted <- KafkaClient[IO].admin(client).use(_.deleteRecords(cut)).timeout(60.seconds)
      _ <- IO.raiseUnless(deleted == cut.toSortedMap.view.mapValues(Right(_)).toMap)(new AssertionError(s"unexpected deletion: $deleted"))
      // The JavaScript backend cannot describe configuration, and says so.
      described <- KafkaClient[IO].admin(client).use(_.describeTopicConfigurations(NonEmptySet.one(topic))).attempt.timeout(60.seconds)
      _ <- described match
        case Right(answers) if answers.get(topic).flatMap(_.toOption).flatMap(_.get("cleanup.policy")).isDefined => IO.unit
        case Left(_: KafkaException.Unsupported)                                                                 => IO.unit
        case other => IO.raiseError(new AssertionError(s"unexpected configuration: $other"))
      _ <- IO.println("xkafka downstream smoke test passed")
    yield ()

  private def validated[A](result: ValidatedNel[SettingsError, A]): IO[A] =
    IO.fromEither(result.toEither.leftMap(errors => new IllegalArgumentException(errors.toList.map(_.message).mkString("; "))))
