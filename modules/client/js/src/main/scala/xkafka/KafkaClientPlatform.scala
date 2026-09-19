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

import scala.scalajs.js
import scala.scalajs.js.JSConverters.*
import scala.scalajs.js.typedarray.Uint8Array

import cats.data.NonEmptyList
import cats.effect.{Async, Deferred, Resource}
import cats.effect.std.{Dispatcher, Queue}
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import internal.confluent

private[xkafka] object KafkaClientPlatform:
  def apply[F[_]: Async]: KafkaClient[F] = fromDriver(ConfluentKafkaDriver.live)

  private[xkafka] def fromDriver[F[_]: Async](driver: ConfluentKafkaDriver): KafkaClient[F] = new ConfluentKafkaClient[F](driver)

private[xkafka] trait ConfluentKafkaDriver:
  def producer(settings: ClientSettings, properties: Map[String, String]): confluent.Producer

  def consumer(
      settings: ClientSettings,
      groupId: ConsumerGroup,
      autoOffsetReset: AutoOffsetReset,
      properties: Map[String, String]
  ): confluent.Consumer

private object ConfluentKafkaDriver:
  val live: ConfluentKafkaDriver =
    new ConfluentKafkaDriver:
      override def producer(settings: ClientSettings, properties: Map[String, String]): confluent.Producer =
        kafka(settings).producer(confluent.Values.producerConfig(properties))

      override def consumer(
          settings: ClientSettings,
          groupId: ConsumerGroup,
          autoOffsetReset: AutoOffsetReset,
          properties: Map[String, String]
      ): confluent.Consumer = kafka(settings).consumer(confluent.Values.consumerConfig(groupId.value, autoOffsetReset, properties))

      private def kafka(settings: ClientSettings): confluent.Kafka =
        confluent.Values
          .kafka(confluent.Values.kafkaConfig(settings.bootstrapServers.toList.toJSArray, settings.clientId.orUndefined, settings.properties))

private final class ConfluentKafkaClient[F[_]](driver: ConfluentKafkaDriver)(using F: Async[F]) extends KafkaClient[F]:

  override def producer[K, V](settings: ProducerSettings[F, K, V]): Resource[F, KafkaProducer[F, K, V]] =
    Resource.eval(F.delay(driver.producer(settings.client, settings.properties))).flatMap: producer =>
      Resource.make(await(producer.connect()))(_ => await(producer.disconnect())).as(new ConfluentKafkaProducer(producer, settings))

  override def consumer[K, V](settings: ConsumerSettings[F, K, V], subscription: Subscription): Resource[F, KafkaConsumer[F, K, V]] =
    for
      underlying <- Resource.eval(F.delay(driver.consumer(settings.client, settings.groupId, settings.autoOffsetReset, settings.properties)))
      dispatcher <- Dispatcher.parallel[F]
      _          <- Resource.make(await(underlying.connect()))(_ => await(underlying.disconnect()))
      _          <- Resource.eval(subscribe(underlying, subscription))
      queue      <- Resource.eval(Queue.bounded[F, CommittableConsumerRecord[F, K, V]](256))
      failure    <- Resource.eval(Deferred[F, Throwable])
      shutdown   <- Resource.eval(Deferred[F, Unit])
      adapter = new ConfluentKafkaConsumer(underlying, settings, dispatcher, queue, failure, shutdown)
      _ <- Resource.eval(adapter.run)
      _ <- Resource.make(F.unit)(_ => shutdown.complete(()).void)
    yield adapter

  private def await[A](promise: => js.Promise[A]): F[A] = F.fromPromise(F.delay(promise))

  private def subscribe(consumer: confluent.Consumer, subscription: Subscription): F[Unit] =
    subscription match
      case Subscription.Topics(topics) =>
        val values = topics.toList.map[confluent.SubscriptionTopic](_.value).toJSArray
        await(consumer.subscribe(confluent.Values.subscription(values)))
      case Subscription.Pattern(pattern) => F.delay(new js.RegExp(pattern.anchored)).flatMap: compiled =>
          await(consumer.subscribe(confluent.Values.subscription(js.Array[confluent.SubscriptionTopic](compiled))))

  private def invalidBackendValue(field: String, value: String, error: Any): IllegalStateException =
    new IllegalStateException(s"Confluent Kafka JavaScript returned an invalid $field '$value': $error")

  private def parseLong(field: String, value: String): F[Long] =
    F.catchNonFatal(java.lang.Long.parseLong(value)).adaptError:
      case error => invalidBackendValue(field, value, error.getMessage)

  private def topic(value: String): F[Topic] = F.fromEither(Topic.from(value).leftMap(error => invalidBackendValue("topic", value, error)))

  private def partition(value: Int): F[Partition] =
    F.fromEither(Partition.from(value).leftMap(error => invalidBackendValue("partition", value.toString, error)))

  private def offset(field: String, value: String): F[Offset] =
    parseLong(field, value).flatMap: parsed =>
      F.fromEither(Offset.from(parsed).leftMap(error => invalidBackendValue(field, value, error)))

  private def bytes(value: Uint8Array | Null): Option[Chunk[Byte]] =
    Option(value).map: raw =>
      val array = raw.asInstanceOf[Uint8Array]
      Chunk.array(Array.tabulate(array.length)(index => array(index).toByte))

  private def nodeBuffer(value: Option[Chunk[Byte]]): Uint8Array | Null =
    value.fold[Uint8Array | Null](null): chunk =>
      val array = new Uint8Array(chunk.size)
      chunk.iterator.zipWithIndex.foreach:
        case (byte, index) => array(index) = byte.toShort
      confluent.Buffer.from(array)

  private def jsHeaders(headers: Headers): confluent.JsHeaders =
    val result = js.Dictionary.empty[js.Any]
    headers.values.groupMap(_.key)(_.value).foreach:
      case (key, values) =>
        val encoded = values.map(value => nodeBuffer(value).asInstanceOf[js.Any])
        result(key) = if encoded.sizeIs == 1 then encoded.head else encoded.toJSArray
    result

  private def portableHeaders(headers: js.UndefOr[confluent.JsHeaders]): Headers =
    headers.toOption.fold(Headers.empty): values =>
      val result =
        js.Object.keys(values.asInstanceOf[js.Object]).iterator.flatMap: key =>
          val value = values(key)
          if js.Array.isArray(value) then value.asInstanceOf[js.Array[Uint8Array | Null]].iterator.map(raw => Header(key, bytes(raw)))
          else Iterator.single(Header(key, bytes(value.asInstanceOf)))
        .toVector
      Headers.fromVector(result)

  private final class ConfluentKafkaProducer[K, V](underlying: confluent.Producer, settings: ProducerSettings[F, K, V])
      extends KafkaProducer[F, K, V]:

    override def produce(records: NonEmptyList[ProducerRecord[K, V]]): F[ProducerResult[K, V]] =
      records.toList.traverse(encodeRecord).flatMap: encoded =>
        val grouped =
          encoded.groupMap(_._1)(_._2).iterator.map:
            case (topic, messages) => confluent.Values.topicMessages(topic, messages.toJSArray)
          .toJSArray

        await(underlying.sendBatch(confluent.Values.producerBatch(grouped))).flatMap(_.toList.traverse(recordMetadata))
      .map(metadata => ProducerResult(records, metadata))

    private def encodeRecord(record: ProducerRecord[K, V]): F[(String, confluent.Message)] =
      (
        settings.keySerializer.serialize(record.topic, record.headers, record.key),
        settings.valueSerializer.serialize(record.topic, record.headers, record.value)
      ).mapN: (key, value) =>
        record.topic.value -> confluent.Values.message(
          nodeBuffer(key),
          nodeBuffer(value),
          record.partition.map(_.value).orUndefined,
          record.timestamp.map(_.epochMillis.toString).orUndefined,
          jsHeaders(record.headers)
        )

    private def recordMetadata(metadata: confluent.RecordMetadata): F[RecordMetadata] =
      if metadata.errorCode != 0 then
        F.raiseError(new IllegalStateException(s"Confluent Kafka JavaScript returned producer error code ${metadata.errorCode}"))
      else
        for
          portableTopic     <- topic(metadata.topicName)
          portablePartition <- partition(metadata.partition)
          portableOffset    <- metadata.offset.toOption.orElse(metadata.baseOffset.toOption).traverse(offset("offset", _))
          portableTimestamp <- metadata.timestamp.toOption.orElse(metadata.logAppendTime.toOption).traverse(parseLong("timestamp", _))
        yield RecordMetadata(TopicPartition(portableTopic, portablePartition), portableOffset, portableTimestamp.map(Timestamp.fromEpochMillis))

  private final class ConfluentKafkaConsumer[K, V](
      underlying: confluent.Consumer,
      settings: ConsumerSettings[F, K, V],
      dispatcher: Dispatcher[F],
      queue: Queue[F, CommittableConsumerRecord[F, K, V]],
      failure: Deferred[F, Throwable],
      shutdown: Deferred[F, Unit]
  ) extends KafkaConsumer[F, K, V]:

    private val offsetCommitter: OffsetCommitter[F] =
      new OffsetCommitter[F]:
        override def commit(offsets: Map[TopicPartition, Offset]): F[Unit] =
          val values =
            offsets.iterator.map:
              case (topicPartition, offset) => confluent.Values
                  .topicPartitionOffset(topicPartition.topic.value, topicPartition.partition.value, offset.value.toString)
            .toJSArray
          await(underlying.commitOffsets(values))

    override val records: Stream[F, CommittableConsumerRecord[F, K, V]] =
      Stream.fromQueueUnterminated(queue).mergeHaltBoth(Stream.eval(failure.get).flatMap(Stream.raiseError[F]))

    override def assignment: F[Set[TopicPartition]] = F.delay(underlying.assignment()).flatMap(_.toList.traverse(portableTopicPartition).map(_.toSet))

    override def committed(topicPartitions: Set[TopicPartition]): F[Map[TopicPartition, Option[Offset]]] =
      if topicPartitions.isEmpty then F.pure(Map.empty)
      else
        val requested = topicPartitions.iterator.map(value => confluent.Values.topicPartition(value.topic.value, value.partition.value)).toJSArray
        await(underlying.committed(requested)).flatMap(
          _.toList.traverse: value =>
            (portableTopicPartition(value), optionalOffset("committed offset", value.offset)).mapN(_ -> _)
        ).map(_.toMap)

    override def seek(topicPartition: TopicPartition, offset: Offset): F[Unit] =
      F.delay(
        underlying.seek(confluent.Values.topicPartitionOffset(topicPartition.topic.value, topicPartition.partition.value, offset.value.toString))
      )

    val run: F[Unit] =
      await(underlying.run(confluent.Values.consumerRun(payload =>
        dispatcher.unsafeToPromise(
          processBatch(payload).handleErrorWith: error =>
            failure.complete(error).void >> F.raiseError(error)
        )
      ))).handleErrorWith(error => failure.complete(error).void >> F.raiseError(error))

    private def processBatch(payload: confluent.EachBatchPayload): F[Unit] =
      val batch    = payload.batch
      val messages = batch.messages.toList
      if !payload.isRunning() || payload.isStale() then F.unit
      else
        (topic(batch.topic), partition(batch.partition)).tupled.flatMap:
          case (portableTopic, portablePartition) => messages.traverse(consumerRecord(portableTopic, portablePartition, _)).flatMap: records =>
              if !payload.isRunning() || payload.isStale() then F.unit
              else
                // Resolution advances Confluent's local cursor; enable.auto.commit remains disabled.
                messages.lastOption.traverse_(message => F.delay(payload.resolveOffset(message.offset))) >>
                  records.traverse_(record => F.race(queue.offer(record), shutdown.get).void)

    private def consumerRecord(
        portableTopic: Topic,
        portablePartition: Partition,
        message: confluent.KafkaMessage
    ): F[CommittableConsumerRecord[F, K, V]] =
      val headers = portableHeaders(message.headers)
      for
        portableOffset     <- offset("offset", message.offset)
        portableNextOffset <- F.fromEither(portableOffset.next.leftMap(error => invalidBackendValue("next offset", message.offset, error)))
        timestamp          <- Option(message.timestamp).filter(_.nonEmpty).traverse(parseLong("timestamp", _))
        key                <- settings.keyDeserializer.deserialize(portableTopic, headers, bytes(message.key))
        value              <- settings.valueDeserializer.deserialize(portableTopic, headers, bytes(message.value))
      yield
        val topicPartition    = TopicPartition(portableTopic, portablePartition)
        val record            = ConsumerRecord(topicPartition, portableOffset, timestamp.map(Timestamp.fromEpochMillis), key, value, headers)
        val committableOffset =
          new CommittableOffset[F]:
            override val topicPartition: TopicPartition = TopicPartition(portableTopic, portablePartition)

            override val nextOffset: Offset = portableNextOffset

            override val committer: OffsetCommitter[F] = offsetCommitter

        CommittableConsumerRecord(record, committableOffset)

    private def portableTopicPartition(value: confluent.TopicPartition): F[TopicPartition] =
      (topic(value.topic), partition(value.partition)).mapN(TopicPartition.apply)

    private def optionalOffset(field: String, value: String | Null): F[Option[Offset]] =
      Option(value).fold(F.pure(Option.empty[Offset])): raw =>
        parseLong(field, raw).flatMap: parsed =>
          if parsed < 0L then F.pure(None)
          else F.fromEither(Offset.from(parsed).leftMap(error => invalidBackendValue(field, raw, error)).map(Some(_)))
