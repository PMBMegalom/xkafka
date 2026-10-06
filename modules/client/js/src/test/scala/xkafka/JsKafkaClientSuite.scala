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
import scala.scalajs.js.timers
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
        confluent.Values
          .rdConsumerConfig(js.Array("broker-1:9092"), "client", "group", AutoOffsetReset.Earliest, Map("fetch.wait.max.ms" -> "10"), ignoreRebalance)
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
    // A function, so the client leaves every assign and unassign to it.
    assertEquals(js.typeOf(consumer.selectDynamic("rebalance_cb")), "function")

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
          KafkaClientPlatform
            .fromDriver[IO](driver(producerValue = producer, expectedProducerProperties = Map("linger.ms" -> "5", DefaultAcks, DefaultIdempotence)))
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
          .toOption.get.withoutAssignmentFencing
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
        settings = unleasedSettings.withCommitTimeout(100.millis).toOption.get.withCommitRecovery(CommitRecovery.none)
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
      settings = unleasedSettings.withCommitTimeout(1.second).toOption.get.withCommitRecovery(CommitRecovery.none)
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

  test("a commit report that arrives after the commit timeout fails as a timed out request"):
    for
      delivered <- IO(js.Array(commitMessage(0d)))
      listeners <- IO(js.Array[CommitListener]())
      consumer =
        commitConsumer(
          delivered,
          listener => listeners.push(listener): Unit,
          offsets =>
            // Holding the event loop past the deadline means the report always arrives before the timer can run.
            val until = js.Date.now() + 50
            while js.Date.now() < until do ()
            listeners.foreach(_(null, offsets.asInstanceOf[js.Array[confluent.RdTopicPartition]]))
        )
      settings = unleasedSettings.withCommitTimeout(5.millis).toOption.get.withCommitRecovery(CommitRecovery.none)
      outcome <-
        KafkaClientPlatform.fromDriver[IO](driver(consumerValue = consumer)).consumer(settings, Selection.Topics(NonEmptySet.one(topic("events"))))
          .use(_.records.take(1).evalMap(_.offset.commit).compile.drain.attempt)
    yield outcome match
      case Left(failure: KafkaException.BackendFailure) => assertEquals(failure.code, Some(ErrorCode.RequestTimedOut), failure.getMessage)
      case other                                        => fail(s"a report after the deadline should time the commit out, got $other")

  test("a commit report the client cannot read answers no commit and leaves the consumer running"):
    for
      delivered <- IO(js.Array(commitMessage(0d), commitMessage(1d)))
      listeners <- IO(js.Array[CommitListener]())
      consumer =
        commitConsumer(
          delivered,
          listener => listeners.push(listener): Unit,
          _ => listeners.foreach(_(null, null.asInstanceOf[js.Array[confluent.RdTopicPartition]]))
        )
      settings = unleasedSettings.withCommitTimeout(200.millis).toOption.get.withCommitRecovery(CommitRecovery.none)
      outcome <-
        KafkaClientPlatform.fromDriver[IO](driver(consumerValue = consumer)).consumer(settings, Selection.Topics(NonEmptySet.one(topic("events"))))
          // Committing inside the stream means the second record arrives only if the first unreadable report left the
          // consumer running.
          .use(_.records.evalMap(_.offset.commit.attempt).take(2).compile.toList.timeout(5.seconds))
    yield
      assertEquals(outcome.length, 2)
      outcome.foreach:
        case Left(failure: KafkaException.BackendFailure) => assertEquals(failure.code, Some(ErrorCode.RequestTimedOut))
        case other                                        => fail(s"an unreadable report should leave the commit to time out, got $other")

  test("a commit report whose offset the client omitted still answers its commit"):
    for
      delivered <- IO(js.Array(commitMessage(0d)))
      listeners <- IO(js.Array[CommitListener]())
      consumer =
        commitConsumer(
          delivered,
          listener => listeners.push(listener): Unit,
          // The client omits `offset` whenever librdkafka reports a negative one, so the event names only its partition.
          _ => listeners.foreach(_(null, js.Array(partitionOnlyReport)))
        )
      settings = unleasedSettings.withCommitTimeout(1.second).toOption.get.withCommitRecovery(CommitRecovery.none)
      outcome <-
        KafkaClientPlatform.fromDriver[IO](driver(consumerValue = consumer)).consumer(settings, Selection.Topics(NonEmptySet.one(topic("events"))))
          .use(_.records.head.compile.lastOrError.flatMap(_.offset.commit)).attempt
    yield assertEquals(outcome, Right(()))

  test("a commit report whose present offset is malformed answers no commit"):
    for
      delivered <- IO(js.Array(commitMessage(0d)))
      listeners <- IO(js.Array[CommitListener]())
      consumer = commitConsumer(delivered, listener => listeners.push(listener): Unit, _ => listeners.foreach(_(null, js.Array(commitReport(1.5)))))
      settings = unleasedSettings.withCommitTimeout(100.millis).toOption.get.withCommitRecovery(CommitRecovery.none)
      outcome <-
        KafkaClientPlatform.fromDriver[IO](driver(consumerValue = consumer)).consumer(settings, Selection.Topics(NonEmptySet.one(topic("events"))))
          .use(_.records.head.compile.lastOrError.flatMap(_.offset.commit)).attempt
    yield outcome match
      case Left(failure: KafkaException.BackendFailure) => assertEquals(failure.code, Some(ErrorCode.RequestTimedOut))
      case other                                        => fail(s"a malformed report should leave the commit to time out, got $other")

  test("a failure reported without its offset reaches the commit instead of its timeout"):
    for
      delivered <- IO(js.Array(commitMessage(0d)))
      listeners <- IO(js.Array[CommitListener]())
      consumer =
        commitConsumer(
          delivered,
          listener => listeners.push(listener): Unit,
          _ =>
            val refused =
              dynamic(js.Dynamic.literal(message = "coordinator not available", code = 15, isFatal = false, isRetriable = true))
                .asInstanceOf[confluent.RdError]
            listeners.foreach(_(refused, js.Array(partitionOnlyReport)))
        )
      settings = unleasedSettings.withCommitTimeout(1.second).toOption.get.withCommitRecovery(CommitRecovery.none)
      outcome <-
        KafkaClientPlatform.fromDriver[IO](driver(consumerValue = consumer)).consumer(settings, Selection.Topics(NonEmptySet.one(topic("events"))))
          .use(_.records.head.compile.lastOrError.flatMap(_.offset.commit)).attempt
    yield outcome match
      case Left(failure: KafkaException.BackendFailure) =>
        assertEquals(failure.detail, "coordinator not available")
        assertNotEquals(failure.code, Some(ErrorCode.RequestTimedOut))
      case other => fail(s"the reported failure should reach the commit, got $other")

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
        settings = unleasedSettings.withCommitTimeout(1.second).toOption.get.withCommitRecovery(CommitRecovery.none)
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
      rebalances <- IO(js.Array[confluent.RdRebalance]())
      consumer = rebalancingConsumer(js.Array(), js.Array())
      // A partition this client rejects, so reading the assignment the rebalance leaves raises.
      _ <-
        IO(dynamic(consumer).updateDynamic("assignments")(
          (() => js.Array(js.Dynamic.literal(topic = "events", partition = -1).asInstanceOf[confluent.RdTopicPartition])): js.Function0[
            js.Array[confluent.RdTopicPartition]
          ]
        ))
      outcome <-
        KafkaClientPlatform.fromDriver[IO](driver(consumerValue = consumer, rebalances = rebalances))
          .consumer(consumerSettings, Selection.Topics(NonEmptySet.one(topic("events")))).use: value =>
            IO(rebalance(rebalances(0), consumer, AssignPartitions, 0)) *> value.records.compile.drain.timeout(5.seconds).attempt
    yield assert(
      outcome.left.exists(_.isInstanceOf[KafkaException.InvalidBackendResponse]),
      s"the unreadable assignment should reach whoever reads the consumer, got $outcome"
    )

  test("consumer release unassigns the revocation its disconnect brings, without reading the assignment afterwards"):
    for
      calls      <- IO(js.Array[String]())
      rebalances <- IO(js.Array[confluent.RdRebalance]())
      consumer = rebalancingConsumer(js.Array(), calls)
      // librdkafka revokes the assignment while it closes, and finishes closing only once that revocation is unassigned.
      _ <-
        IO(dynamic(consumer).updateDynamic("disconnect")(((done: js.Function2[confluent.RdError | Null, js.Any, Unit]) =>
          calls.push("disconnect"): Unit
          dynamic(consumer).updateDynamic("closing")(done)
          rebalance(rebalances(0), consumer, RevokePartitions, 0)
        ): js.Function1[js.Function2[confluent.RdError | Null, js.Any, Unit], Unit]))
      _ <-
        IO(dynamic(consumer).updateDynamic("unassign")((() =>
          calls.push("unassign"): Unit
          val closing = dynamic(consumer).selectDynamic("closing")
          if !js.isUndefined(closing) then closing.asInstanceOf[js.Function2[confluent.RdError | Null, js.Any, Unit]](null, ())
        ): js.Function0[Unit]))
      _ <-
        KafkaClientPlatform.fromDriver[IO](driver(consumerValue = consumer, rebalances = rebalances))
          .consumer(consumerSettings, Selection.Topics(NonEmptySet.one(topic("events")))).use: _ =>
            IO(rebalance(rebalances(0), consumer, AssignPartitions, 0)) *> until(calls.contains("assignments"))
          .timeout(5.seconds)
    yield
      val disconnected = calls.indexOf("disconnect")
      assert(disconnected >= 0, calls.toList.toString)
      assertEquals(calls.toList.drop(disconnected), List("disconnect", "unassign"))

  test("an offset read under a revoked assignment cannot be recorded in a transaction, and one read after reassignment can"):
    for
      calls      <- IO(js.Array[String]())
      delivered  <- IO(js.Array[confluent.RdMessage]())
      rebalances <- IO(js.Array[confluent.RdRebalance]())
      sent       <- IO(js.Array[js.Function1[confluent.RdError | Null, Unit]]())
      rd     = rebalancingConsumer(delivered, calls)
      client = KafkaClientPlatform.fromDriver[IO](transactionalDriver(transactionalProducer(sent, hold = false), rd, rebalances))
      outcome <-
        (client.transactionalProducer(transactionalSettings), client.consumer(consumerSettings, Selection.Topics(NonEmptySet.one(topic("events")))))
          .tupled.use: (producer, consumer) =>
            def read(offset: Double) = IO(delivered.push(commitMessage(offset))) *> consumer.records.take(1).compile.lastOrError
            def recorded(value: CommittableConsumerRecord[IO, String, String]) =
              producer.transactionally(_.commitOffsets(CommittableOffsetBatch.empty[IO].updated(value.offset))).attempt
            for
              _            <- IO(rebalance(rebalances(0), rd, AssignPartitions, 0)) *> until(calls.count(_ == "assign") == 1)
              earlier      <- read(0d)
              _            <- IO(rebalance(rebalances(0), rd, RevokePartitions, 0)) *> until(calls.contains("unassign"))
              revoked      <- recorded(earlier)
              _            <- IO(rebalance(rebalances(0), rd, AssignPartitions, 0)) *> until(calls.count(_ == "assign") == 2)
              later        <- read(1d)
              stillRevoked <- recorded(earlier)
              current      <- recorded(later)
            yield (revoked, stillRevoked, current)
          .timeout(10.seconds)
    yield
      val (revoked, stillRevoked, current) = outcome
      assert(illegalGeneration(revoked), s"an offset from a revoked assignment should be rejected, got $revoked")
      assert(illegalGeneration(stillRevoked), s"reassigning the partition should not revive an earlier offset, got $stillRevoked")
      assertEquals(current, Right(()))
      // Only the offset read under the current assignment ever reached the client.
      assertEquals(sent.length, 1)

  test("with assignment fencing off, an offset read under a revoked assignment is still recorded, as it was before fencing"):
    for
      calls      <- IO(js.Array[String]())
      delivered  <- IO(js.Array[confluent.RdMessage]())
      rebalances <- IO(js.Array[confluent.RdRebalance]())
      sent       <- IO(js.Array[js.Function1[confluent.RdError | Null, Unit]]())
      rd       = rebalancingConsumer(delivered, calls)
      client   = KafkaClientPlatform.fromDriver[IO](transactionalDriver(transactionalProducer(sent, hold = false), rd, rebalances))
      unfenced = consumerSettings.withoutAssignmentFencing
      outcome <-
        (client.transactionalProducer(transactionalSettings), client.consumer(unfenced, Selection.Topics(NonEmptySet.one(topic("events"))))).tupled
          .use: (producer, consumer) =>
            for
              _       <- IO(rebalance(rebalances(0), rd, AssignPartitions, 0)) *> until(calls.contains("assign"))
              earlier <- IO(delivered.push(commitMessage(0d))) *> consumer.records.take(1).compile.lastOrError
              _       <- IO(rebalance(rebalances(0), rd, RevokePartitions, 0)) *> until(calls.contains("unassign"))
              stale   <- producer.transactionally(_.commitOffsets(CommittableOffsetBatch.empty[IO].updated(earlier.offset))).attempt
            yield stale
          .timeout(10.seconds)
    yield
      assertEquals(outcome, Right(()))
      assertEquals(sent.length, 1)

  test("a plain commit of an offset read under a revoked assignment is refused before it reaches the client"):
    for
      calls      <- IO(js.Array[String]())
      delivered  <- IO(js.Array[confluent.RdMessage]())
      rebalances <- IO(js.Array[confluent.RdRebalance]())
      committed  <- IO(js.Array[js.Array[confluent.RdTopicPartitionOffset]]())
      rd = committing(rebalancingConsumer(delivered, calls), committed, js.Array(), hold = false)
      outcome <-
        KafkaClientPlatform.fromDriver[IO](driver(consumerValue = rd, rebalances = rebalances))
          .consumer(consumerSettings, Selection.Topics(NonEmptySet.one(topic("events")))).use: consumer =>
            def read(offset: Double) = IO(delivered.push(commitMessage(offset))) *> consumer.records.take(1).compile.lastOrError
            for
              _       <- IO(rebalance(rebalances(0), rd, AssignPartitions, 0)) *> until(calls.count(_ == "assign") == 1)
              earlier <- read(0d)
              _       <- IO(rebalance(rebalances(0), rd, RevokePartitions, 0)) *> until(calls.contains("unassign"))
              revoked <- earlier.offset.commit.attempt
              _       <- IO(rebalance(rebalances(0), rd, AssignPartitions, 0)) *> until(calls.count(_ == "assign") == 2)
              later   <- read(1d)
              current <- later.offset.commit.attempt
            yield (revoked, current)
          .timeout(10.seconds)
    yield
      val (revoked, current) = outcome
      assert(illegalGeneration(revoked), s"a plain commit from a revoked assignment should be refused, got $revoked")
      assertEquals(current, Right(()))
      assertEquals(committed.length, 1, "only the offset read under the current assignment should reach the client")

  test("a revocation waits for a plain commit the client has not reported yet"):
    for
      calls      <- IO(js.Array[String]())
      delivered  <- IO(js.Array[confluent.RdMessage]())
      rebalances <- IO(js.Array[confluent.RdRebalance]())
      committed  <- IO(js.Array[js.Array[confluent.RdTopicPartitionOffset]]())
      held       <- IO(js.Array[js.Function0[Unit]]())
      rd = committing(rebalancingConsumer(delivered, calls), committed, held, hold = true)
      outcome <-
        KafkaClientPlatform.fromDriver[IO](driver(consumerValue = rd, rebalances = rebalances))
          .consumer(consumerSettings, Selection.Topics(NonEmptySet.one(topic("events")))).use: consumer =>
            for
              _          <- IO(rebalance(rebalances(0), rd, AssignPartitions, 0)) *> until(calls.contains("assign"))
              record     <- IO(delivered.push(commitMessage(0d))) *> consumer.records.take(1).compile.lastOrError
              committing <- record.offset.commit.attempt.start
              _          <- until(held.length == 1)
              _          <- IO(rebalance(rebalances(0), rd, RevokePartitions, 0))
              _          <- IO.sleep(200.millis)
              early      <- IO(calls.contains("unassign"))
              _          <- IO(held(0)())
              outcome    <- committing.joinWithNever
              _          <- until(calls.contains("unassign"))
            yield (early, outcome)
          .timeout(10.seconds)
    yield
      val (early, reported) = outcome
      assert(!early, "the partition was unassigned while its commit had not been reported")
      assertEquals(reported, Right(()))

  test("a revocation waits for a transaction recording one of its offsets before it unassigns"):
    for
      calls      <- IO(js.Array[String]())
      delivered  <- IO(js.Array[confluent.RdMessage]())
      rebalances <- IO(js.Array[confluent.RdRebalance]())
      sent       <- IO(js.Array[js.Function1[confluent.RdError | Null, Unit]]())
      rd     = rebalancingConsumer(delivered, calls)
      client = KafkaClientPlatform.fromDriver[IO](transactionalDriver(transactionalProducer(sent, hold = true), rd, rebalances))
      outcome <-
        (client.transactionalProducer(transactionalSettings), client.consumer(consumerSettings, Selection.Topics(NonEmptySet.one(topic("events")))))
          .tupled.use: (producer, consumer) =>
            for
              _         <- IO(rebalance(rebalances(0), rd, AssignPartitions, 0)) *> until(calls.contains("assign"))
              held      <- IO(delivered.push(commitMessage(0d))) *> consumer.records.take(1).compile.lastOrError
              recording <- producer.transactionally(_.commitOffsets(CommittableOffsetBatch.empty[IO].updated(held.offset))).attempt.start
              _         <- until(sent.length == 1)
              _         <- IO(rebalance(rebalances(0), rd, RevokePartitions, 0))
              _         <- IO.sleep(200.millis)
              early     <- IO(calls.contains("unassign"))
              _         <- IO(sent(0)(null))
              recorded  <- recording.joinWithNever
              _         <- until(calls.contains("unassign"))
            yield (early, recorded)
          .timeout(10.seconds)
    yield
      val (early, recorded) = outcome
      assert(!early, "the partition was unassigned while a transaction was still recording its offset")
      assertEquals(recorded, Right(()))

  test("a revocation during a consume leaves the records it returns unable to be recorded in a transaction"):
    for
      calls      <- IO(js.Array[String]())
      delivered  <- IO(js.Array[confluent.RdMessage]())
      rebalances <- IO(js.Array[confluent.RdRebalance]())
      sent       <- IO(js.Array[js.Function1[confluent.RdError | Null, Unit]]())
      trigger    <- IO(js.Array[Unit]())
      rd = rebalancingConsumer(delivered, calls)
      // Once triggered, a consume that is still running when the partition is revoked and assigned again, and returns afterwards.
      _ <-
        IO(dynamic(rd).updateDynamic("consume")(
          (
              (_: Int, done: js.Function2[confluent.RdError | Null, js.Array[confluent.RdMessage], Unit]) =>
                if trigger.length > 0 then
                  trigger.pop()
                  rebalance(rebalances(0), rd, RevokePartitions, 0)
                  rebalance(rebalances(0), rd, AssignPartitions, 0)
                  timers.setTimeout(100d)(done(null, js.Array(commitMessage(0d)))): Unit
                else done(null, delivered.splice(0, delivered.length).toJSArray)
          ): js.Function2[Int, js.Function2[confluent.RdError | Null, js.Array[confluent.RdMessage], Unit], Unit]
        ))
      client = KafkaClientPlatform.fromDriver[IO](transactionalDriver(transactionalProducer(sent, hold = false), rd, rebalances))
      outcome <-
        (client.transactionalProducer(transactionalSettings), client.consumer(consumerSettings, Selection.Topics(NonEmptySet.one(topic("events")))))
          .tupled.use: (producer, consumer) =>
            def recorded(value: CommittableConsumerRecord[IO, String, String]) =
              producer.transactionally(_.commitOffsets(CommittableOffsetBatch.empty[IO].updated(value.offset))).attempt
            for
              _       <- IO(rebalance(rebalances(0), rd, AssignPartitions, 0)) *> until(calls.contains("assign"))
              spanned <- IO(trigger.push(())) *> consumer.records.take(1).compile.lastOrError
              after   <- IO(delivered.push(commitMessage(1d))) *> consumer.records.take(1).compile.lastOrError
              first   <- recorded(spanned)
              second  <- recorded(after)
            yield (first, second)
          .timeout(10.seconds)
    yield
      val (spanned, after) = outcome
      assert(illegalGeneration(spanned), s"a record from a consume the revocation interrupted should be rejected, got $spanned")
      assertEquals(after, Right(()))

  test("the isolation level reaches the backend as the property it spells"):
    val settings = ConsumerSettings.from(clientSettings, group, utf8Deserializer, utf8Deserializer).toOption.get

    for
      captured <- IO(js.Array[Map[String, String]]())
      client = KafkaClientPlatform.fromDriver[IO](collectingDriver(captured))
      _ <- client.consumer(settings.withIsolationLevel(IsolationLevel.ReadCommitted), Selection.Topics(NonEmptySet.one(topic("events")))).use_.attempt
    yield assertEquals(captured.toList, List(Map("isolation.level" -> "read_committed")))

  test("the acks setting reaches the backend as the property it spells"):
    val serializer = Serializer.const[IO, String](None)
    val settings   = ProducerSettings.from(clientSettings, serializer, serializer).toOption.get.withoutIdempotence.withAcks(Acks.Leader).toOption.get

    for
      captured <- IO(js.Array[Map[String, String]]())
      _        <- KafkaClientPlatform.fromDriver[IO](collectingDriver(captured)).producer(settings).use_.attempt
    yield assertEquals(captured.toList, List(Map("acks" -> "1", "enable.idempotence" -> "false")))

  test("the transactional settings reach the backend as the properties Kafka reads"):
    val serializer      = Serializer.const[IO, String](None)
    val transactionalId = TransactionalId.from("writer").toOption.get
    val settings        =
      TransactionalProducerSettings
        .from(clientSettings, transactionalId, serializer, serializer, transactionTimeout = 30.seconds, closeTimeout = 5.seconds).toOption.get
    val closed   = js.Array[Int]()
    val producer =
      js.Dynamic.literal(
        connect =
          ((_: js.Any, done: js.Function2[confluent.RdError | Null, js.Any, Unit]) => done(null, ())): js.Function2[
            js.Any,
            js.Function2[confluent.RdError | Null, js.Any, Unit],
            Unit
          ],
        disconnect =
          (
              (timeout: Int, done: js.Function2[confluent.RdError | Null, js.Any, Unit]) =>
                closed.push(timeout): Unit
                done(null, ())
          ): js.Function2[Int, js.Function2[confluent.RdError | Null, js.Any, Unit], Unit],
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
    val expected = Map("transactional.id" -> "writer", "transaction.timeout.ms" -> "30000", DefaultAcks, DefaultIdempotence)

    // Releasing the producer bounds its delivery wait by the close timeout, so the disconnect is given it.
    KafkaClientPlatform.fromDriver[IO](driver(producerValue = producer, expectedProducerProperties = expected)).transactionalProducer(settings).use_
      .map(_ => assertEquals(closed.toList, List(5000)))

  /** Every consumer carries one, so a driver that is not given an expectation is given this. */
  private val DefaultIsolationLevel = "isolation.level" -> "read_uncommitted"

  /** Likewise every producer, which asks every replica for an answer unless told otherwise. */
  private val DefaultAcks = "acks" -> "all"

  /** Likewise idempotence, which every producer asks for unless told otherwise and every transactional one asks for always. */
  private val DefaultIdempotence = "enable.idempotence" -> "true"

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

  // What librdkafka reports to a rebalance callback for an assignment and a revocation.
  private val AssignPartitions = -175
  private val RevokePartitions = -174

  private val ignoreRebalance: confluent.RdRebalance =
    (_: confluent.RdConsumer, _: confluent.RdError | Null, _: js.Array[confluent.RdTopicPartition]) => ()

  /** Calls the consumer's rebalance callback the way the client does, with the consumer as `this`. */
  private def rebalance(callback: confluent.RdRebalance, consumer: confluent.RdConsumer, code: Int, partitions: Int*): Unit =
    callback(
      consumer,
      js.Dynamic.literal(code = code, message = "rebalance").asInstanceOf[confluent.RdError],
      partitions.map(value => confluent.Values.rdTopicPartition("events", value)).toJSArray
    )

  private def until(condition: => Boolean): IO[Unit] = (IO.sleep(10.millis) *> IO(condition)).iterateUntil(identity).void.timeout(5.seconds)

  private def illegalGeneration(outcome: Either[Throwable, Unit]): Boolean =
    outcome.left.exists:
      case failure: KafkaException.BackendFailure => failure.code.contains(ErrorCode.IllegalGeneration)
      case _                                      => false

  /** An eager-protocol consumer that applies what its rebalance callback tells it and records each call. */
  private def rebalancingConsumer(delivered: js.Array[confluent.RdMessage], calls: js.Array[String]): confluent.RdConsumer =
    val assigned = js.Array[confluent.RdTopicPartition]()
    js.Dynamic.literal(
      connect =
        ((_: js.Any, done: js.Function2[confluent.RdError | Null, js.Any, Unit]) => done(null, ())): js.Function2[
          js.Any,
          js.Function2[confluent.RdError | Null, js.Any, Unit],
          Unit
        ],
      disconnect =
        ((done: js.Function2[confluent.RdError | Null, js.Any, Unit]) =>
          calls.push("disconnect"): Unit
          done(null, ())
        ): js.Function1[js.Function2[confluent.RdError | Null, js.Any, Unit], Unit],
      setDefaultConsumeTimeout = ((_: Int) => ()): js.Function1[Int, Unit],
      on = ((_: String, _: CommitListener) => ()): js.Function2[String, CommitListener, Unit],
      removeListener = ignoreConsumerListener,
      subscribe = ((_: js.Array[confluent.SubscriptionTopic]) => ()): js.Function1[js.Array[confluent.SubscriptionTopic], Unit],
      consume =
        (
            (_: Int, done: js.Function2[confluent.RdError | Null, js.Array[confluent.RdMessage], Unit]) =>
              done(null, delivered.splice(0, delivered.length).toJSArray)
        ): js.Function2[Int, js.Function2[confluent.RdError | Null, js.Array[confluent.RdMessage], Unit], Unit],
      rebalanceProtocol = (() => "EAGER"): js.Function0[String],
      assign =
        ((partitions: js.Array[confluent.RdTopicPartition]) =>
          calls.push("assign"): Unit
          assigned.splice(0, assigned.length, partitions.toSeq*): Unit
        ): js.Function1[js.Array[confluent.RdTopicPartition], Unit],
      unassign =
        (() =>
          calls.push("unassign"): Unit
          assigned.splice(0, assigned.length): Unit
        ): js.Function0[Unit],
      assignments =
        (() =>
          calls.push("assignments"): Unit
          assigned.toSeq.toJSArray
        ): js.Function0[js.Array[confluent.RdTopicPartition]]
    ).asInstanceOf[confluent.RdConsumer]

  /** Gives `consumer` a commit the way the client has one: it takes no callback and reports on the `offset.commit` event. With `hold` the report is
    * put in `held` and waits until the test runs it.
    */
  private def committing(
      consumer: confluent.RdConsumer,
      committed: js.Array[js.Array[confluent.RdTopicPartitionOffset]],
      held: js.Array[js.Function0[Unit]],
      hold: Boolean
  ): confluent.RdConsumer =
    val listeners = js.Array[CommitListener]()
    dynamic(consumer).updateDynamic("on")(
      (
          (event: String, listener: CommitListener) => if event == "offset.commit" then listeners.push(listener): Unit
      ): js.Function2[String, CommitListener, Unit]
    )
    dynamic(consumer).updateDynamic("commit")(((offsets: js.Array[confluent.RdTopicPartitionOffset]) =>
      committed.push(offsets): Unit
      val report: js.Function0[Unit] = () => listeners.foreach(_(null, offsets.asInstanceOf[js.Array[confluent.RdTopicPartition]]))
      if hold then held.push(report): Unit else report()
    ): js.Function1[js.Array[confluent.RdTopicPartitionOffset], Unit])
    consumer

  /** A transactional producer whose every call succeeds. Each offset send is kept, and with `hold` it completes only when the test calls it. */
  private def transactionalProducer(sent: js.Array[js.Function1[confluent.RdError | Null, Unit]], hold: Boolean): confluent.RdProducer =
    val succeed: js.Function1[js.Function1[confluent.RdError | Null, Unit], Unit] = (done: js.Function1[confluent.RdError | Null, Unit]) => done(null)
    val timed: js.Function2[Int, js.Function1[confluent.RdError | Null, Unit], Unit] =
      (_: Int, done: js.Function1[confluent.RdError | Null, Unit]) => done(null)
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
      initTransactions = timed,
      beginTransaction = succeed,
      commitTransaction = timed,
      abortTransaction = timed,
      sendOffsetsToTransaction =
        (
            (_: js.Array[confluent.RdTopicPartitionOffset], _: confluent.RdConsumer, _: Int, done: js.Function1[confluent.RdError | Null, Unit]) =>
              sent.push(done): Unit
              if !hold then done(null)
        ): js.Function4[js.Array[confluent.RdTopicPartitionOffset], confluent.RdConsumer, Int, js.Function1[confluent.RdError | Null, Unit], Unit]
    ).asInstanceOf[confluent.RdProducer]

  private val transactionalSettings =
    TransactionalProducerSettings.from(clientSettings, TransactionalId.from("writer").toOption.get, utf8Serializer, utf8Serializer).toOption.get

  private def transactionalDriver(
      producer: confluent.RdProducer,
      consumer: confluent.RdConsumer,
      rebalances: js.Array[confluent.RdRebalance]
  ): ConfluentKafkaDriver =
    driver(
      producerValue = producer,
      consumerValue = consumer,
      expectedProducerProperties = Map("transactional.id" -> "writer", "transaction.timeout.ms" -> "60000", DefaultAcks, DefaultIdempotence),
      rebalances = rebalances
    )

  private def commitMessage(offset: Double): confluent.RdMessage =
    js.Dynamic.literal(topic = "events", partition = 0, offset = offset, key = uint8("key"), value = uint8("value"), headers = js.Array())
      .asInstanceOf[confluent.RdMessage]

  /** What the client emits for a commit whose offset librdkafka reports as negative: the partition without its offset. */
  private def partitionOnlyReport: confluent.RdTopicPartition =
    js.Dynamic.literal(topic = "events", partition = 0).asInstanceOf[confluent.RdTopicPartition]

  private def commitReport(offset: Double): confluent.RdTopicPartition =
    js.Dynamic.literal(topic = "events", partition = 0, offset = offset).asInstanceOf[confluent.RdTopicPartition]

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
          properties: Map[String, String],
          rebalance: confluent.RdRebalance
      ): confluent.RdConsumer =
        captured.push(properties): Unit
        null

  private def driver(
      producerValue: confluent.RdProducer = null,
      consumerValue: confluent.RdConsumer = null,
      expectedProducerProperties: Map[String, String] = Map(DefaultAcks, DefaultIdempotence),
      expectedConsumerProperties: Map[String, String] = Map(DefaultIsolationLevel),
      rebalances: js.Array[confluent.RdRebalance] = js.Array()
  ): ConfluentKafkaDriver =
    new ConfluentKafkaDriver:
      override def producer(settings: ClientSettings, properties: Map[String, String]): confluent.RdProducer =
        assertEquals(properties, expectedProducerProperties)
        producerValue

      override def consumer(
          settings: ClientSettings,
          groupId: ConsumerGroup,
          autoOffsetReset: AutoOffsetReset,
          properties: Map[String, String],
          rebalance: confluent.RdRebalance
      ): confluent.RdConsumer =
        assertEquals(properties, expectedConsumerProperties)
        rebalances.push(rebalance): Unit
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

  /** For the cases about commit reporting, whose stubs hand over records without ever assigning a partition, so no record carries a lease. */
  private val unleasedSettings = consumerSettings.withoutAssignmentFencing

  private def uint8(value: String): Uint8Array =
    val bytes  = value.getBytes("UTF-8")
    val result = new Uint8Array(bytes.length)
    bytes.iterator.zipWithIndex.foreach:
      case (byte, index) => result(index) = byte.toShort
    result

  private def byteVector(value: Uint8Array): Vector[Byte] = Vector.tabulate(value.length)(index => value(index).toByte)

  private def dynamic(value: Any): js.Dynamic = value.asInstanceOf[js.Dynamic]
