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

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.IO
import munit.CatsEffectSuite
import internal.librdkafka.MockCluster

/** What librdkafka reports for one partition, or for one record, reaches the application as that partition's or record's own failure, against
  * librdkafka's in-process mock cluster.
  */
final class LibrdkafkaFailuresSuite extends CatsEffectSuite:
  override val munitIOTimeout: Duration = 1.minute

  private def valid[A](result: Either[ValidationError, A]): A = result.fold(error => fail(error.message), identity)

  private val text  = Deserializer.utf8[IO]
  private val topic = valid(Topic.from("events"))

  private def code(outcome: Either[Throwable, ?]): Option[ErrorCode] =
    outcome.left.toOption.collect { case failure: KafkaException.BackendFailure => failure.code }.flatten

  test("a record too large to send fails as too large, though a login failed before it"):
    LibrdkafkaPlatform.mockCluster[IO](Map(MockCluster.Brokers)).use: cluster =>
      for
        _    <- cluster.createTopic(topic.value, 1)
        sasl <- SaslSettings.from(SaslMechanism.Plain, "user", "password").liftTo[IO]
        // The mock cluster takes no SASL login, so the client's logins fail.
        client    <- ClientSettings.from(NonEmptyList.one(cluster.bootstrapServers), security = SecuritySettings.SaslPlaintext(sasl)).liftTo[IO]
        producing <- ProducerSettings.from(client, Serializer.utf8[IO], Serializer.utf8[IO]).liftTo[IO]
        outcome   <-
          KafkaClient[IO].producer(producing).use: producer =>
            IO.sleep(3.seconds) *> producer.produceAndAwait(NonEmptyList.one(ProducerRecord(topic, "key", "v" * (2 * 1024 * 1024)))).attempt
      yield assertEquals(code(outcome), Some(ErrorCode.MessageTooLarge), outcome)

  test("a failure librdkafka holds fatal is reported by its cause, on the call that met it and on the calls after"):
    LibrdkafkaPlatform.mockCluster[IO](Map(MockCluster.Brokers)).use: cluster =>
      for
        _         <- cluster.createTopic(topic.value, 1)
        client    <- ClientSettings.from(NonEmptyList.one(cluster.bootstrapServers)).liftTo[IO]
        producing <- ProducerSettings.from(client, Serializer.utf8[IO], Serializer.utf8[IO]).liftTo[IO]
        _         <- KafkaClient[IO].producer(producing).use(_.produceAndAwait(NonEmptyList.of("a", "b").map(ProducerRecord(topic, "key", _))))
        group     <- ConsumerGroup.from("fatal").liftTo[IO]
        consuming <- ConsumerSettings.from(client, group, text, text, AutoOffsetReset.Earliest, groupInstanceId = Some("member")).liftTo[IO]
        outcomes  <-
          KafkaClient[IO].consumer(consuming, Selection.Topics(NonEmptySet.of(topic))).use: consumer =>
            for
              first <- consumer.records.head.compile.lastOrError.timeout(30.seconds)
              // The group's coordinator answers that another member holds this one's instance id, which fences it for good.
              _      <- cluster.failRequests(MockCluster.OffsetCommit, List(MockCluster.FencedInstanceId))
              commit <- first.offset.commit.attempt
              after  <- first.offset.commit.attempt
            yield (commit, after)
      yield
        assertEquals(code(outcomes._1), Some(ErrorCode.Fenced), outcomes._1)
        assertEquals(code(outcomes._2), Some(ErrorCode.Fenced), outcomes._2)

  test("records librdkafka gives up once a failure is fatal report that failure, as fatal, not that they were purged"):
    LibrdkafkaPlatform.mockCluster[IO](Map(MockCluster.Brokers)).use: cluster =>
      for
        _      <- cluster.createTopic(topic.value, 1)
        client <- ClientSettings.from(NonEmptyList.one(cluster.bootstrapServers)).liftTo[IO]
        // librdkafka keeps the gapless guarantee by holding a permanent failure of an idempotent producer fatal.
        producing <- ProducerSettings.from(client, Serializer.utf8[IO], Serializer.utf8[IO], Map("enable.gapless.guarantee" -> "true")).liftTo[IO]
        records = NonEmptyList.fromListUnsafe(List.tabulate(2000)(n => ProducerRecord(topic, s"key-$n", "v" * 1000)))
        attempts <-
          KafkaClient[IO].producer(producing).use: producer =>
            producer.produceAndAwait(NonEmptyList.one(ProducerRecord(topic, "key", "v"))) *>
              cluster.failRequests(MockCluster.Produce, List(MockCluster.InvalidProducerEpoch)) *> producer.produceAttemptEachAndAwait(records)
      yield
        val failures = attempts.toList.flatMap(_.outcome.left.toOption).collect { case failure: KafkaException.BackendFailure => failure }
        assert(failures.size > 1, attempts.toList.map(_.outcome).distinct)
        assertEquals(failures.filter(_.code.contains(ErrorCode.Purged)), Nil)
        assertEquals(failures.map(_.fatal).distinct, List(Some(true)), failures.filter(_.fatal.isEmpty).map(_.getMessage).distinct)

  test("a committed offset the broker refuses for one partition fails with the error it gave that partition"):
    LibrdkafkaPlatform.mockCluster[IO](Map(MockCluster.Brokers)).use: cluster =>
      for
        _         <- cluster.createTopic(topic.value, 1)
        client    <- ClientSettings.from(NonEmptyList.one(cluster.bootstrapServers)).liftTo[IO]
        group     <- ConsumerGroup.from("failures").liftTo[IO]
        consuming <- ConsumerSettings.from(client, group, text, text).liftTo[IO]
        missing = TopicPartition(valid(Topic.from("missing")), valid(Partition.from(0)))
        // The mock cluster answers a topic it does not have with an error for that partition alone.
        outcome <- KafkaClient[IO].consumer(consuming, Selection.Topics(NonEmptySet.of(topic))).use(_.committed(Set(missing)).attempt)
      yield assertEquals(code(outcome), Some(ErrorCode.UnknownTopicOrPartition), outcome)
