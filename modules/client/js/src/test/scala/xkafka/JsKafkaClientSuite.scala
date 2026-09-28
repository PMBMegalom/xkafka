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
import scala.scalajs.js
import scala.scalajs.js.JSConverters.*
import scala.scalajs.js.typedarray.Uint8Array

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{Deferred, IO, Ref}
import cats.effect.std.Dispatcher
import cats.syntax.all.*
import fs2.Chunk
import internal.confluent
import munit.CatsEffectSuite

final class JsKafkaClientSuite extends CatsEffectSuite:
  test("wraps librdkafka failures with their code and classification"):
    val failure =
      js.Dynamic.literal(message = "connection failed", code = -195, isRetriable = true, isFatal = false, isTxnRequiresAbort = true)
        .asInstanceOf[confluent.RdError]
    val producer =
      js.Dynamic.literal(
        connect =
          ((_: js.Any, done: js.Function2[confluent.RdError | Null, js.Any, Unit]) => done(failure, ())): js.Function2[
            js.Any,
            js.Function2[confluent.RdError | Null, js.Any, Unit],
            Unit
          ],
        disconnect =
          ((_: Int, done: js.Function2[confluent.RdError | Null, js.Any, Unit]) => done(null, ())): js.Function2[
            Int,
            js.Function2[confluent.RdError | Null, js.Any, Unit],
            Unit
          ],
        setPollInterval = ((_: Int) => ()): js.Function1[Int, Unit],
        on =
          ((_: String, _: js.Function2[confluent.RdError | Null, confluent.RdDeliveryReport, Unit]) => ()): js.Function2[
            String,
            js.Function2[confluent.RdError | Null, confluent.RdDeliveryReport, Unit],
            Unit
          ]
      ).asInstanceOf[confluent.RdProducer]
    val serializer = Serializer.const[IO, String](None)
    val settings   = ProducerSettings.from(clientSettings, serializer, serializer).toOption.get

    interceptIO[KafkaException.BackendFailure](
      KafkaClientPlatform.fromDriver[IO](driver(producerValue = producer)).producer(settings).use(_ => IO.unit)
    ).map: error =>
      assertEquals(error.detail, "connection failed")
      assertEquals(error.code, Some(ErrorCode.NetworkException))
      assertEquals(error.retriable, Some(true))
      assertEquals(error.fatal, Some(false))
      assertEquals(error.transactionAbortRequired, Some(true))

  test("librdkafka configuration is built directly, with the managed keys derived from typed settings"):
    val producer = dynamic(confluent.Values.rdProducerConfig(js.Array("broker-1:9092", "broker-2:9092"), "client", Map("linger.ms" -> "5")))
    val consumer =
      dynamic(
        confluent.Values.rdConsumerConfig(js.Array("broker-1:9092"), "client", "group", AutoOffsetReset.Earliest, Map("fetch.wait.max.ms" -> "10"))
      )

    assertEquals(producer.selectDynamic("bootstrap.servers").asInstanceOf[String], "broker-1:9092,broker-2:9092")
    assertEquals(producer.selectDynamic("client.id").asInstanceOf[String], "client")
    assertEquals(producer.selectDynamic("linger.ms").asInstanceOf[String], "5")
    // Delivery reports are what make per-record metadata possible.
    assertEquals(producer.selectDynamic("dr_cb").asInstanceOf[Boolean], true)
    assertEquals(consumer.selectDynamic("group.id").asInstanceOf[String], "group")
    assertEquals(consumer.selectDynamic("fetch.wait.max.ms").asInstanceOf[String], "10")
    assertEquals(consumer.selectDynamic("enable.auto.commit").asInstanceOf[Boolean], false)
    assertEquals(consumer.selectDynamic("auto.offset.reset").asInstanceOf[String], "earliest")

  test("producer enqueues each record with its own opaque and reports metadata per record"):
    Dispatcher.sequential[IO].use: dispatcher =>
      for
        connected    <- Ref.of[IO, Int](0)
        disconnected <- Ref.of[IO, Int](0)
        produced     <- IO(js.Array[js.Dynamic]())
        reporter     <- Deferred[IO, js.Function2[confluent.RdError | Null, confluent.RdDeliveryReport, Unit]]
        producer =
          js.Dynamic.literal(
            connect =
              (
                  (_: js.Any, done: js.Function2[confluent.RdError | Null, js.Any, Unit]) =>
                    dispatcher.unsafeRunAndForget(connected.update(_ + 1) >> IO(done(null, ())))
              ): js.Function2[js.Any, js.Function2[confluent.RdError | Null, js.Any, Unit], Unit],
            disconnect =
              (
                  (_: Int, done: js.Function2[confluent.RdError | Null, js.Any, Unit]) =>
                    dispatcher.unsafeRunAndForget(disconnected.update(_ + 1) >> IO(done(null, ())))
              ): js.Function2[Int, js.Function2[confluent.RdError | Null, js.Any, Unit], Unit],
            setPollInterval = ((_: Int) => ()): js.Function1[Int, Unit],
            on =
              (
                  (_: String, listener: js.Function2[confluent.RdError | Null, confluent.RdDeliveryReport, Unit]) =>
                    dispatcher.unsafeRunAndForget(reporter.complete(listener).void)
              ): js.Function2[String, js.Function2[confluent.RdError | Null, confluent.RdDeliveryReport, Unit], Unit],
            produce =
              (
                  (topic: String, partition: js.Any, value: js.Any, key: js.Any, timestamp: js.Any, opaque: js.Any, headers: js.Any) =>
                    produced.push(js.Dynamic.literal(
                      topic = topic,
                      partition = partition,
                      value = value,
                      key = key,
                      timestamp = timestamp,
                      opaque = opaque,
                      headers = headers
                    )): Unit
              ): js.Function7[String, js.Any, js.Any, js.Any, js.Any, js.Any, js.Any, Unit]
          ).asInstanceOf[confluent.RdProducer]
        record =
          ProducerRecord(
            topic = topic("events"),
            key = "key",
            value = "value",
            partition = Some(partition(2)),
            timestamp = Some(Timestamp.fromEpochMillis(1234L)),
            headers =
              Headers(
                Header("trace", Some(Chunk.array(Array[Byte](1)))),
                Header("other", Some(Chunk.array(Array[Byte](9)))),
                Header("trace", Some(Chunk.array(Array[Byte](2))))
              )
          )
        settings = ProducerSettings.from(clientSettings, utf8Serializer, utf8Serializer, Map("linger.ms" -> "5")).toOption.get
        result <-
          KafkaClientPlatform.fromDriver[IO](driver(producerValue = producer, expectedProducerProperties = Map("linger.ms" -> "5", DefaultAcks)))
            .producer(settings).use: underlying =>
              for
                acknowledgement <- underlying.produce(NonEmptyList.one(record))
                listener        <- reporter.get
                opaque = produced(0).opaque
                _ <-
                  IO(listener(
                    null,
                    js.Dynamic.literal(topic = "events", partition = 2, offset = 41d, timestamp = 1234d, opaque = opaque)
                      .asInstanceOf[confluent.RdDeliveryReport]
                  ))
                value <- acknowledgement
              yield value
        connectedCount    <- connected.get
        disconnectedCount <- disconnected.get
        _                 <-
          IO:
            assertEquals(connectedCount, 1)
            assertEquals(disconnectedCount, 1)
            assertEquals(produced.length, 1)
            assertEquals(produced(0).topic.asInstanceOf[String], "events")
            assertEquals(produced(0).partition.asInstanceOf[Int], 2)
            assertEquals(produced(0).timestamp.asInstanceOf[Double], 1234d)
            assertEquals(byteVector(produced(0).key.asInstanceOf[Uint8Array]), "key".getBytes("UTF-8").toVector)

            // librdkafka takes headers as an ordered array, so duplicate names keep their produced order.
            val headers = produced(0).headers.asInstanceOf[js.Array[js.Dictionary[Uint8Array]]]
            assertEquals(headers.length, 3)
            assertEquals(headers.toList.flatMap(_.keys.toList), List("trace", "other", "trace"))
            assertEquals(headers.toList.flatMap(_.values.toList).map(byteVector), List(Vector(1.toByte), Vector(9.toByte), Vector(2.toByte)))

            assertEquals(result.records.map((value, _) => value), NonEmptyList.one(record))
            assertEquals(result.records.toList.map((_, metadata) => metadata.flatMap(_.offset.map(_.value))), List(Some(41L)))
      yield ()

  test("producer rejects a header without a value rather than changing it to empty bytes"):
    val produced = js.Array[js.Any]()
    val producer =
      js.Dynamic.literal(
        connect =
          ((_: js.Any, done: js.Function2[confluent.RdError | Null, js.Any, Unit]) => done(null, ())): js.Function2[
            js.Any,
            js.Function2[confluent.RdError | Null, js.Any, Unit],
            Unit
          ],
        disconnect =
          ((_: Int, done: js.Function2[confluent.RdError | Null, js.Any, Unit]) => done(null, ())): js.Function2[
            Int,
            js.Function2[confluent.RdError | Null, js.Any, Unit],
            Unit
          ],
        setPollInterval = ((_: Int) => ()): js.Function1[Int, Unit],
        on =
          ((_: String, _: js.Function2[confluent.RdError | Null, confluent.RdDeliveryReport, Unit]) => ()): js.Function2[
            String,
            js.Function2[confluent.RdError | Null, confluent.RdDeliveryReport, Unit],
            Unit
          ],
        produce =
          ((_: String, _: js.Any, _: js.Any, _: js.Any, _: js.Any, _: js.Any, headers: js.Any) => produced.push(headers): Unit): js.Function7[
            String,
            js.Any,
            js.Any,
            js.Any,
            js.Any,
            js.Any,
            js.Any,
            Unit
          ]
      ).asInstanceOf[confluent.RdProducer]
    val record   = ProducerRecord(topic("events"), "key", "value", headers = Headers(Header("missing", None)))
    val settings = ProducerSettings.from(clientSettings, utf8Serializer, utf8Serializer).toOption.get

    KafkaClientPlatform.fromDriver[IO](driver(producerValue = producer)).producer(settings).use(_.produce(NonEmptyList.one(record))).attempt.map:
      case Left(error: KafkaException.Unsupported) =>
        assertEquals(error.detail, "the JavaScript backend cannot produce header 'missing' without a value")
        assertEquals(produced.length, 0)
      case Left(error) => fail(s"unexpected error: $error")
      case Right(_)    => fail("expected a missing header value to be rejected")

  test("a partial enqueue failure rolls back the batch and leaves the producer usable"):
    Dispatcher.sequential[IO].use: dispatcher =>
      for
        attempts <- IO(js.Array[js.Dynamic]())
        reporter <- Deferred[IO, js.Function2[confluent.RdError | Null, confluent.RdDeliveryReport, Unit]]
        producer =
          js.Dynamic.literal(
            connect =
              ((_: js.Any, done: js.Function2[confluent.RdError | Null, js.Any, Unit]) => done(null, ())): js.Function2[
                js.Any,
                js.Function2[confluent.RdError | Null, js.Any, Unit],
                Unit
              ],
            disconnect =
              ((_: Int, done: js.Function2[confluent.RdError | Null, js.Any, Unit]) => done(null, ())): js.Function2[
                Int,
                js.Function2[confluent.RdError | Null, js.Any, Unit],
                Unit
              ],
            setPollInterval = ((_: Int) => ()): js.Function1[Int, Unit],
            on =
              (
                  (_: String, listener: js.Function2[confluent.RdError | Null, confluent.RdDeliveryReport, Unit]) =>
                    dispatcher.unsafeRunAndForget(reporter.complete(listener).void)
              ): js.Function2[String, js.Function2[confluent.RdError | Null, confluent.RdDeliveryReport, Unit], Unit],
            produce =
              (
                  (topic: String, partition: js.Any, _: js.Any, _: js.Any, _: js.Any, opaque: js.Any, _: js.Any) =>
                    attempts.push(js.Dynamic.literal(topic = topic, partition = partition, opaque = opaque)): Unit
                    if attempts.length == 2 then throw new RuntimeException("local queue is full")
              ): js.Function7[String, js.Any, js.Any, js.Any, js.Any, js.Any, js.Any, Unit]
          ).asInstanceOf[confluent.RdProducer]
        record   = ProducerRecord(topic("events"), "key", "value", partition = Some(partition(0)))
        settings = ProducerSettings.from(clientSettings, utf8Serializer, utf8Serializer).toOption.get
        outcomes <-
          KafkaClientPlatform.fromDriver[IO](driver(producerValue = producer)).producer(settings).use: value =>
            for
              failed          <- value.produce(NonEmptyList.of(record, record)).attempt
              acknowledgement <- value.produce(NonEmptyList.one(record))
              report          <- reporter.get
              _               <-
                IO:
                  List(attempts(0), attempts(2)).zipWithIndex.foreach: (attempt, index) =>
                    report(
                      null,
                      js.Dynamic.literal(topic = attempt.topic, partition = attempt.partition, offset = index.toDouble, opaque = attempt.opaque)
                        .asInstanceOf[confluent.RdDeliveryReport]
                    )
              succeeded <- acknowledgement
            yield (failed, succeeded)
      yield
        assertEquals(outcomes._1.leftMap(_.getMessage), Left("local queue is full"))
        assertEquals(outcomes._2.records.toList.map((_, metadata) => metadata.flatMap(_.offset.map(_.value))), List(Some(1L)))

  test("consumer decodes a pulled batch, keeps header order, and commits the exact next offset"):
    for
      committed       <- IO(js.Array[js.Dynamic]())
      delivered       <- IO(js.Array[confluent.RdMessage]())
      pollTimeouts    <- IO(js.Array[Int]())
      commitListeners <- IO(js.Array[js.Function2[confluent.RdError | Null, js.Array[confluent.RdTopicPartition], Unit]]())
      message =
        js.Dynamic.literal(
          topic = "events",
          partition = 2,
          offset = 41d,
          key = uint8("key"),
          value = uint8("value"),
          timestamp = 1234d,
          headers = js.Array(rdHeader("trace", Array[Byte](1)), rdHeader("other", Array[Byte](9)), rdHeader("trace", Array[Byte](2)))
        ).asInstanceOf[confluent.RdMessage]
      _ <- IO(delivered.push(message): Unit)
      consumer =
        js.Dynamic.literal(
          connect =
            ((_: js.Any, done: js.Function2[confluent.RdError | Null, js.Any, Unit]) => done(null, ())): js.Function2[
              js.Any,
              js.Function2[confluent.RdError | Null, js.Any, Unit],
              Unit
            ],
          disconnect =
            ((done: js.Function2[confluent.RdError | Null, js.Any, Unit]) => done(null, ())): js.Function1[
              js.Function2[confluent.RdError | Null, js.Any, Unit],
              Unit
            ],
          setDefaultConsumeTimeout = ((value: Int) => pollTimeouts.push(value): Unit): js.Function1[Int, Unit],
          on =
            (
                (event: String, listener: js.Function2[confluent.RdError | Null, js.Array[confluent.RdTopicPartition], Unit]) =>
                  if event == "offset.commit" then commitListeners.push(listener): Unit else ()
            ): js.Function2[String, js.Function2[confluent.RdError | Null, js.Array[confluent.RdTopicPartition], Unit], Unit],
          removeListener = ignoreConsumerListener,
          subscribe = ((_: js.Array[confluent.SubscriptionTopic]) => ()): js.Function1[js.Array[confluent.SubscriptionTopic], Unit],
          consume =
            (
                (_: Int, done: js.Function2[confluent.RdError | Null, js.Array[confluent.RdMessage], Unit]) =>
                  done(null, delivered.splice(0, delivered.length).toJSArray)
            ): js.Function2[Int, js.Function2[confluent.RdError | Null, js.Array[confluent.RdMessage], Unit], Unit],
          // The client takes no callback for a commit and reports its outcome on an event, so the stub does the same.
          commit =
            ((offsets: js.Array[confluent.RdTopicPartitionOffset]) =>
              offsets.foreach(value => committed.push(dynamic(value)): Unit)
              commitListeners.foreach(_(null, offsets.asInstanceOf[js.Array[confluent.RdTopicPartition]]))
            ): js.Function1[js.Array[confluent.RdTopicPartitionOffset], Unit]
        ).asInstanceOf[confluent.RdConsumer]
      settings =
        ConsumerSettings
          .from(clientSettings, group, utf8Deserializer, utf8Deserializer, AutoOffsetReset.Earliest, properties = Map("fetch.wait.max.ms" -> "10"))
          .toOption.get
      record <-
        KafkaClientPlatform
          .fromDriver[IO](driver(consumerValue = consumer, expectedConsumerProperties = Map("fetch.wait.max.ms" -> "10", DefaultIsolationLevel)))
          // Committing inside the resource, because a commit now waits for the client to report it.
          .consumer(settings, Selection.Topics(NonEmptySet.one(topic("events")))).use(_.records.take(1).compile.lastOrError.flatTap(_.offset.commit))
    yield
      assertEquals(record.record.topicPartition, TopicPartition(topic("events"), partition(2)))
      assertEquals(record.record.key, "key")
      assertEquals(record.record.value, "value")
      assertEquals(record.record.offset.value, 41L)
      assertEquals(record.record.timestamp.map(_.epochMillis), Some(1234L))

      // librdkafka returns one object per header, so duplicate names keep their order.
      assertEquals(record.record.headers.values.map(_.key), Vector("trace", "other", "trace"))
      assertEquals(record.record.headers.values.map(_.value.map(_.toList)), Vector(Some(List[Byte](1)), Some(List[Byte](9)), Some(List[Byte](2))))

      assertEquals(record.offset.nextOffset.value, 42L)
      assertEquals(committed.length, 1)
      assertEquals(committed(0).topic.asInstanceOf[String], "events")
      assertEquals(committed(0).partition.asInstanceOf[Int], 2)
      assertEquals(committed(0).offset.asInstanceOf[Double], 42d)

      // The consumer poll timeout has to reach the client, which is the only place it takes effect.
      assertEquals(pollTimeouts.toList, List(ConsumerSettings.DefaultPollTimeout.toMillis.toInt))

  test("a late commit report cannot answer the next commit"):
    Dispatcher.sequential[IO].use: dispatcher =>
      for
        delivered       <- IO(js.Array(commitMessage(0d), commitMessage(1d)))
        listener        <- Deferred[IO, CommitListener]
        submitted       <- IO(js.Array[js.Array[confluent.RdTopicPartitionOffset]]())
        secondSubmitted <- Deferred[IO, Unit]
        consumer =
          commitConsumer(
            delivered,
            value => dispatcher.unsafeRunAndForget(listener.complete(value).void),
            offsets =>
              submitted.push(offsets): Unit
              if submitted.length == 2 then dispatcher.unsafeRunAndForget(secondSubmitted.complete(()).void)
          )
        settings = consumerSettings.withCommitTimeout(100.millis).toOption.get.withCommitRecovery(CommitRecovery.none)
        outcomes <-
          KafkaClientPlatform.fromDriver[IO](driver(consumerValue = consumer)).consumer(settings, Selection.Topics(NonEmptySet.one(topic("events"))))
            .use: value =>
              for
                records   <- value.records.take(2).compile.toList
                first     <- records.head.offset.commit.attempt
                second    <- records(1).offset.commit.start
                _         <- secondSubmitted.get.timeout(1.second)
                report    <- listener.get
                _         <- IO(report(null, submitted(0).asInstanceOf[js.Array[confluent.RdTopicPartition]]))
                afterLate <- second.join.map(Some(_)).timeoutTo(25.millis, IO.pure(None))
                refused =
                  dynamic(js.Dynamic.literal(message = "commit refused", code = 13, isFatal = false, isRetriable = true))
                    .asInstanceOf[confluent.RdError]
                _             <- IO(report(refused, submitted(1).asInstanceOf[js.Array[confluent.RdTopicPartition]]))
                secondOutcome <- second.joinWithNever.attempt
              yield (first, afterLate, secondOutcome)
      yield
        outcomes._1 match
          case Left(failure: KafkaException.BackendFailure) => assertEquals(failure.code, Some(ErrorCode.RequestTimedOut))
          case other                                        => fail(s"the first commit should time out, got $other")
        assertEquals(outcomes._2, None)
        outcomes._3 match
          case Left(failure: KafkaException.BackendFailure) =>
            assertEquals(failure.detail, "commit refused")
            assertEquals(failure.code, Some(ErrorCode.NetworkException))
          case other => fail(s"the reported failure should reach the second commit, got $other")

  test("a synchronous commit failure does not leave a waiter for the next report"):
    for
      delivered <- IO(js.Array(commitMessage(0d), commitMessage(1d)))
      listeners <- IO(js.Array[CommitListener]())
      submitted <- IO(js.Array[js.Array[confluent.RdTopicPartitionOffset]]())
      consumer =
        commitConsumer(
          delivered,
          listener => listeners.push(listener): Unit,
          offsets =>
            submitted.push(offsets): Unit
            if submitted.length == 1 then throw new RuntimeException("commit exploded")
            else listeners.foreach(_(null, offsets.asInstanceOf[js.Array[confluent.RdTopicPartition]]))
        )
      settings = consumerSettings.withCommitTimeout(1.second).toOption.get.withCommitRecovery(CommitRecovery.none)
      outcomes <-
        KafkaClientPlatform.fromDriver[IO](driver(consumerValue = consumer)).consumer(settings, Selection.Topics(NonEmptySet.one(topic("events"))))
          .use: value =>
            for
              records <- value.records.take(2).compile.toList
              first   <- records.head.offset.commit.attempt
              second  <- records(1).offset.commit.attempt
            yield (first, second)
    yield
      assertEquals(outcomes._1.leftMap(_.getMessage), Left("commit exploded"))
      assertEquals(outcomes._2, Right(()))

  test("a commit report without offsets fails inside the consumer error channel"):
    for
      delivered <- IO(js.Array(commitMessage(0d)))
      listeners <- IO(js.Array[CommitListener]())
      consumer =
        commitConsumer(
          delivered,
          listener => listeners.push(listener): Unit,
          _ => listeners.foreach(_(null, null.asInstanceOf[js.Array[confluent.RdTopicPartition]]))
        )
      settings = consumerSettings.withCommitTimeout(1.second).toOption.get.withCommitRecovery(CommitRecovery.none)
      outcome <-
        KafkaClientPlatform.fromDriver[IO](driver(consumerValue = consumer)).consumer(settings, Selection.Topics(NonEmptySet.one(topic("events"))))
          .use(_.records.head.compile.lastOrError.flatMap(_.offset.commit)).attempt
    yield outcome match
      case Left(failure: KafkaException.InvalidBackendResponse) => assertEquals(failure.detail, "commit report is missing its offsets")
      case other                                                => fail(s"a malformed report should fail as an invalid backend response, got $other")

  test("cancelling a commit removes its waiter before another commit starts"):
    Dispatcher.sequential[IO].use: dispatcher =>
      for
        delivered       <- IO(js.Array(commitMessage(0d), commitMessage(1d)))
        listener        <- Deferred[IO, CommitListener]
        submitted       <- IO(js.Array[js.Array[confluent.RdTopicPartitionOffset]]())
        firstSubmitted  <- Deferred[IO, Unit]
        secondSubmitted <- Deferred[IO, Unit]
        consumer =
          commitConsumer(
            delivered,
            value => dispatcher.unsafeRunAndForget(listener.complete(value).void),
            offsets =>
              submitted.push(offsets): Unit
              val observed = if submitted.length == 1 then firstSubmitted else secondSubmitted
              dispatcher.unsafeRunAndForget(observed.complete(()).void)
          )
        settings = consumerSettings.withCommitTimeout(1.second).toOption.get.withCommitRecovery(CommitRecovery.none)
        pendingAfterLate <-
          KafkaClientPlatform.fromDriver[IO](driver(consumerValue = consumer)).consumer(settings, Selection.Topics(NonEmptySet.one(topic("events"))))
            .use: value =>
              for
                records <- value.records.take(2).compile.toList
                first   <- records.head.offset.commit.start
                _       <- firstSubmitted.get.timeout(1.second)
                _       <- first.cancel
                second  <- records(1).offset.commit.start
                _       <- secondSubmitted.get.timeout(1.second)
                report  <- listener.get
                _       <- IO(report(null, submitted(0).asInstanceOf[js.Array[confluent.RdTopicPartition]]))
                pending <- second.join.map(Some(_)).timeoutTo(25.millis, IO.pure(None))
                _       <- IO(report(null, submitted(1).asInstanceOf[js.Array[confluent.RdTopicPartition]]))
                _       <- second.joinWithNever
              yield pending
      yield assertEquals(pendingAfterLate, None)

  test("a rebalance the client cannot read fails the consumer rather than stopping its assignment tracking"):
    for
      handlers <- IO(js.Array[js.Function2[confluent.RdError | Null, js.Array[confluent.RdTopicPartition], Unit]]())
      consumer =
        js.Dynamic.literal(
          connect =
            ((_: js.Any, done: js.Function2[confluent.RdError | Null, js.Any, Unit]) => done(null, ())): js.Function2[
              js.Any,
              js.Function2[confluent.RdError | Null, js.Any, Unit],
              Unit
            ],
          disconnect =
            ((done: js.Function2[confluent.RdError | Null, js.Any, Unit]) => done(null, ())): js.Function1[
              js.Function2[confluent.RdError | Null, js.Any, Unit],
              Unit
            ],
          setDefaultConsumeTimeout = ((_: Int) => ()): js.Function1[Int, Unit],
          on =
            (
                (_: String, handler: js.Function2[confluent.RdError | Null, js.Array[confluent.RdTopicPartition], Unit]) =>
                  handlers.push(handler): Unit
            ): js.Function2[String, js.Function2[confluent.RdError | Null, js.Array[confluent.RdTopicPartition], Unit], Unit],
          removeListener = ignoreConsumerListener,
          subscribe = ((_: js.Array[confluent.SubscriptionTopic]) => ()): js.Function1[js.Array[confluent.SubscriptionTopic], Unit],
          consume =
            ((_: Int, done: js.Function2[confluent.RdError | Null, js.Array[confluent.RdMessage], Unit]) => done(null, js.Array())): js.Function2[
              Int,
              js.Function2[confluent.RdError | Null, js.Array[confluent.RdMessage], Unit],
              Unit
            ],
          // A partition this client rejects, so reading the assignment the rebalance reports raises.
          assignments =
            (() => js.Array(js.Dynamic.literal(topic = "events", partition = -1).asInstanceOf[confluent.RdTopicPartition])): js.Function0[
              js.Array[confluent.RdTopicPartition]
            ]
        ).asInstanceOf[confluent.RdConsumer]
      settings = ConsumerSettings.from(clientSettings, group, utf8Deserializer, utf8Deserializer, AutoOffsetReset.Earliest).toOption.get
      outcome <-
        KafkaClientPlatform.fromDriver[IO](driver(consumerValue = consumer)).consumer(settings, Selection.Topics(NonEmptySet.one(topic("events"))))
          .use: value =>
            IO(handlers.foreach(_(null, js.Array()))) *> value.records.compile.drain.timeout(5.seconds).attempt
    yield assert(
      outcome.left.exists(_.isInstanceOf[KafkaException.InvalidBackendResponse]),
      s"the unreadable assignment should reach whoever reads the consumer, got $outcome"
    )

  test("consumer release removes callback work before disconnecting"):
    for
      order           <- IO(js.Array[String]())
      assignmentCalls <- IO(js.Array[Unit]())
      listeners       <- IO(js.Dictionary("rebalance" -> js.Array[CommitListener](), "offset.commit" -> js.Array[CommitListener]()))
      state           <- IO(js.Dynamic.literal(disconnected = false))
      consumer =
        js.Dynamic.literal(
          connect =
            ((_: js.Any, done: js.Function2[confluent.RdError | Null, js.Any, Unit]) => done(null, ())): js.Function2[
              js.Any,
              js.Function2[confluent.RdError | Null, js.Any, Unit],
              Unit
            ],
          disconnect =
            ((done: js.Function2[confluent.RdError | Null, js.Any, Unit]) =>
              order.push("disconnect"): Unit
              state.updateDynamic("disconnected")(true)
              listeners("rebalance").foreach(_(null, js.Array()))
              done(null, ())
            ): js.Function1[js.Function2[confluent.RdError | Null, js.Any, Unit], Unit],
          setDefaultConsumeTimeout = ((_: Int) => ()): js.Function1[Int, Unit],
          on = ((event: String, listener: CommitListener) => listeners(event).push(listener): Unit): js.Function2[String, CommitListener, Unit],
          removeListener =
            (
                (event: String, listener: CommitListener) =>
                  order.push(s"remove:$event"): Unit
                  val index = listeners(event).indexOf(listener)
                  if index >= 0 then listeners(event).splice(index, 1): Unit
            ): js.Function2[String, CommitListener, Unit],
          subscribe = ((_: js.Array[confluent.SubscriptionTopic]) => ()): js.Function1[js.Array[confluent.SubscriptionTopic], Unit],
          consume =
            ((_: Int, _: js.Function2[confluent.RdError | Null, js.Array[confluent.RdMessage], Unit]) => ()): js.Function2[
              Int,
              js.Function2[confluent.RdError | Null, js.Array[confluent.RdMessage], Unit],
              Unit
            ],
          assignments =
            (() =>
              assignmentCalls.push(()): Unit
              if state.selectDynamic("disconnected").asInstanceOf[Boolean] then throw new RuntimeException("Local: Erroneous state")
              js.Array[confluent.RdTopicPartition]()
            ): js.Function0[js.Array[confluent.RdTopicPartition]]
        ).asInstanceOf[confluent.RdConsumer]
      _ <-
        KafkaClientPlatform.fromDriver[IO](driver(consumerValue = consumer))
          .consumer(consumerSettings, Selection.Topics(NonEmptySet.one(topic("events")))).use_
    yield
      assertEquals(order.toList, List("remove:offset.commit", "remove:rebalance", "disconnect"))
      assertEquals(assignmentCalls.length, 0)

  test("the isolation level reaches the backend as the property it spells"):
    val settings = ConsumerSettings.from(clientSettings, group, utf8Deserializer, utf8Deserializer).toOption.get

    for
      captured <- IO(js.Array[Map[String, String]]())
      client = KafkaClientPlatform.fromDriver[IO](collectingDriver(captured))
      _ <- client.consumer(settings.withIsolationLevel(IsolationLevel.ReadCommitted), Selection.Topics(NonEmptySet.one(topic("events")))).use_.attempt
    yield assertEquals(captured.toList, List(Map("isolation.level" -> "read_committed")))

  test("the acks setting reaches the backend as the property it spells"):
    val serializer = Serializer.const[IO, String](None)
    val settings   = ProducerSettings.from(clientSettings, serializer, serializer).toOption.get.withAcks(Acks.Leader)

    for
      captured <- IO(js.Array[Map[String, String]]())
      _        <- KafkaClientPlatform.fromDriver[IO](collectingDriver(captured)).producer(settings).use_.attempt
    yield assertEquals(captured.toList, List(Map("acks" -> "1")))

  test("the transactional settings reach the backend as the properties Kafka reads"):
    val serializer      = Serializer.const[IO, String](None)
    val transactionalId = TransactionalId.from("writer").toOption.get
    val settings        =
      TransactionalProducerSettings.from(clientSettings, transactionalId, serializer, serializer, transactionTimeout = 30.seconds).toOption.get
    val producer =
      js.Dynamic.literal(
        connect =
          ((_: js.Any, done: js.Function2[confluent.RdError | Null, js.Any, Unit]) => done(null, ())): js.Function2[
            js.Any,
            js.Function2[confluent.RdError | Null, js.Any, Unit],
            Unit
          ],
        disconnect =
          ((_: Int, done: js.Function2[confluent.RdError | Null, js.Any, Unit]) => done(null, ())): js.Function2[
            Int,
            js.Function2[confluent.RdError | Null, js.Any, Unit],
            Unit
          ],
        setPollInterval = ((_: Int) => ()): js.Function1[Int, Unit],
        on =
          ((_: String, _: js.Function2[confluent.RdError | Null, confluent.RdDeliveryReport, Unit]) => ()): js.Function2[
            String,
            js.Function2[confluent.RdError | Null, confluent.RdDeliveryReport, Unit],
            Unit
          ],
        initTransactions =
          ((_: Int, done: js.Function1[confluent.RdError | Null, Unit]) => done(null)): js.Function2[
            Int,
            js.Function1[confluent.RdError | Null, Unit],
            Unit
          ]
      ).asInstanceOf[confluent.RdProducer]
    val expected = Map("transactional.id" -> "writer", "transaction.timeout.ms" -> "30000", DefaultAcks)

    KafkaClientPlatform.fromDriver[IO](driver(producerValue = producer, expectedProducerProperties = expected)).transactionalProducer(settings).use_

  /** Every consumer carries one, so a driver that is not given an expectation is given this. */
  private val DefaultIsolationLevel = "isolation.level" -> "read_uncommitted"

  /** Likewise every producer, which asks every replica for an answer unless told otherwise. */
  private val DefaultAcks = "acks" -> "all"

  private type CommitListener = js.Function2[confluent.RdError | Null, js.Array[confluent.RdTopicPartition], Unit]

  private val clientSettings = ClientSettings.from(NonEmptyList.one("localhost:9092"), Some("tests")).toOption.get

  private val utf8Serializer: Serializer[IO, String] = Serializer.instance((_, _, value) => IO.pure(Some(Chunk.array(value.getBytes("UTF-8")))))

  private val utf8Deserializer: Deserializer[IO, String] =
    Deserializer.instance((_, _, value) => IO.pure(value.fold("")(bytes => new String(bytes.toArray, "UTF-8"))))

  private def commitConsumer(
      delivered: js.Array[confluent.RdMessage],
      register: CommitListener => Unit,
      commitOffsets: js.Array[confluent.RdTopicPartitionOffset] => Unit
  ): confluent.RdConsumer =
    js.Dynamic.literal(
      connect =
        ((_: js.Any, done: js.Function2[confluent.RdError | Null, js.Any, Unit]) => done(null, ())): js.Function2[
          js.Any,
          js.Function2[confluent.RdError | Null, js.Any, Unit],
          Unit
        ],
      disconnect =
        ((done: js.Function2[confluent.RdError | Null, js.Any, Unit]) => done(null, ())): js.Function1[
          js.Function2[confluent.RdError | Null, js.Any, Unit],
          Unit
        ],
      setDefaultConsumeTimeout = ((_: Int) => ()): js.Function1[Int, Unit],
      on =
        (
            (event: String, listener: CommitListener) => if event == "offset.commit" then register(listener)
        ): js.Function2[String, CommitListener, Unit],
      removeListener = ignoreConsumerListener,
      subscribe = ((_: js.Array[confluent.SubscriptionTopic]) => ()): js.Function1[js.Array[confluent.SubscriptionTopic], Unit],
      consume =
        (
            (_: Int, done: js.Function2[confluent.RdError | Null, js.Array[confluent.RdMessage], Unit]) =>
              done(null, delivered.splice(0, delivered.length).toJSArray)
        ): js.Function2[Int, js.Function2[confluent.RdError | Null, js.Array[confluent.RdMessage], Unit], Unit],
      commit =
        ((offsets: js.Array[confluent.RdTopicPartitionOffset]) => commitOffsets(offsets)): js.Function1[js.Array[
          confluent.RdTopicPartitionOffset
        ], Unit]
    ).asInstanceOf[confluent.RdConsumer]

  private def commitMessage(offset: Double): confluent.RdMessage =
    js.Dynamic.literal(topic = "events", partition = 0, offset = offset, key = uint8("key"), value = uint8("value"), headers = js.Array())
      .asInstanceOf[confluent.RdMessage]

  private def ignoreConsumerListener: js.Function2[String, CommitListener, Unit] = ((_: String, _: CommitListener) => ())

  /** Captures what a client would have been built with. Acquiring the client then fails, because the stub hands back nothing, so the properties are
    * asserted on afterwards instead of from inside an effect whose failure has to be swallowed.
    */
  /** Captures what a client would have been built with. Acquiring it then fails, because the stub hands back nothing, so the properties are asserted
    * on afterwards rather than from inside an effect whose failure the test has to swallow.
    */
  private def collectingDriver(captured: js.Array[Map[String, String]]): ConfluentKafkaDriver =
    new ConfluentKafkaDriver:
      override def producer(settings: ClientSettings, properties: Map[String, String]): confluent.RdProducer =
        captured.push(properties): Unit
        null

      override def consumer(
          settings: ClientSettings,
          groupId: ConsumerGroup,
          autoOffsetReset: AutoOffsetReset,
          properties: Map[String, String]
      ): confluent.RdConsumer =
        captured.push(properties): Unit
        null

  private def driver(
      producerValue: confluent.RdProducer = null,
      consumerValue: confluent.RdConsumer = null,
      expectedProducerProperties: Map[String, String] = Map(DefaultAcks),
      expectedConsumerProperties: Map[String, String] = Map(DefaultIsolationLevel)
  ): ConfluentKafkaDriver =
    new ConfluentKafkaDriver:
      override def producer(settings: ClientSettings, properties: Map[String, String]): confluent.RdProducer =
        assertEquals(properties, expectedProducerProperties)
        producerValue

      override def consumer(
          settings: ClientSettings,
          groupId: ConsumerGroup,
          autoOffsetReset: AutoOffsetReset,
          properties: Map[String, String]
      ): confluent.RdConsumer =
        assertEquals(properties, expectedConsumerProperties)
        consumerValue

  private def rdHeader(name: String, value: Array[Byte]): confluent.RdHeader =
    val bytes = new Uint8Array(value.length)
    value.zipWithIndex.foreach((byte, index) => bytes(index) = byte.toShort)
    val result = js.Dictionary.empty[Uint8Array | String | Null]
    result(name) = bytes
    result

  private def topic(value: String): Topic = Topic.from(value).fold(error => fail(error.toString), identity)

  private def partition(value: Int): Partition = Partition.from(value).fold(error => fail(error.toString), identity)

  private val group = ConsumerGroup.from("workers").fold(error => fail(error.toString), identity)

  private val consumerSettings = ConsumerSettings.from(clientSettings, group, utf8Deserializer, utf8Deserializer).toOption.get

  private def uint8(value: String): Uint8Array =
    val bytes  = value.getBytes("UTF-8")
    val result = new Uint8Array(bytes.length)
    bytes.iterator.zipWithIndex.foreach:
      case (byte, index) => result(index) = byte.toShort
    result

  private def byteVector(value: Uint8Array): Vector[Byte] = Vector.tabulate(value.length)(index => value(index).toByte)

  private def dynamic(value: Any): js.Dynamic = value.asInstanceOf[js.Dynamic]
