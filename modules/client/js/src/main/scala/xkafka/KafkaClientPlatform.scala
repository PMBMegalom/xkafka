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
import cats.effect.{Async, Deferred, Ref, Resource}
import cats.effect.std.{Dispatcher, Queue}
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import internal.confluent

private[xkafka] object KafkaClientPlatform:
  def apply[F[_]: Async]: KafkaClient[F] = fromDriver(ConfluentKafkaDriver.live)

  private[xkafka] def fromDriver[F[_]: Async](driver: ConfluentKafkaDriver): KafkaClient[F] = new ConfluentKafkaClient[F](driver)

private[xkafka] trait ConfluentKafkaDriver:
  def producer(settings: ClientSettings, properties: Map[String, String]): confluent.RdProducer

  def consumer(
      settings: ClientSettings,
      groupId: ConsumerGroup,
      autoOffsetReset: AutoOffsetReset,
      properties: Map[String, String]
  ): confluent.Consumer

private object ConfluentKafkaDriver:
  val live: ConfluentKafkaDriver =
    new ConfluentKafkaDriver:
      override def producer(settings: ClientSettings, properties: Map[String, String]): confluent.RdProducer =
        val config =
          confluent.Values
            .rdProducerConfig(settings.bootstrapServers.toList.toJSArray, settings.clientId.orUndefined, settings.properties ++ properties)
        js.Dynamic.newInstance(confluent.RdKafka.Producer)(config).asInstanceOf[confluent.RdProducer]

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
  private val DeliveryPollIntervalMillis = 10
  private val MaxExactInteger            = 9007199254740991d

  private def ignore(value: js.Any): Unit = ()

  override def producer[K, V](settings: ProducerSettings[F, K, V]): Resource[F, KafkaProducer[F, K, V]] =
    for
      underlying <- Resource.eval(F.delay(driver.producer(settings.client, settings.properties)))
      dispatcher <- Dispatcher.sequential[F]
      pending    <- Resource.eval(Ref.of[F, Map[Double, Deferred[F, Either[Throwable, RecordMetadata]]]](Map.empty))
      counter    <- Resource.eval(Ref.of[F, Double](0d))
      _          <- Resource.eval(F.delay(underlying.on("delivery-report", deliveryReport(dispatcher, pending))))
      _ <- Resource.make(callback[js.Any](done => underlying.connect((), done)).void)(_ => callback[js.Any](done => underlying.disconnect(done)).void)
      // librdkafka only surfaces delivery reports while the client is polled.
      _ <- Resource.eval(F.delay(underlying.setPollInterval(DeliveryPollIntervalMillis)))
    yield new ConfluentKafkaProducer(underlying, settings, pending, counter)

  private def deliveryReport[K, V](
      dispatcher: Dispatcher[F],
      pending: Ref[F, Map[Double, Deferred[F, Either[Throwable, RecordMetadata]]]]
  ): js.Function2[confluent.RdError | Null, confluent.RdDeliveryReport, Unit] =
    (error, report) =>
      report.opaque.toOption.foreach: token =>
        val outcome =
          Option(error.asInstanceOf[confluent.RdError]) match
            case Some(failure) => Left(rdFailure(failure))
            case None          => reportMetadata(report)
        dispatcher.unsafeRunAndForget(pending.modify(current => (current - token, current.get(token))).flatMap(_.traverse_(_.complete(outcome))))

  private def reportMetadata(report: confluent.RdDeliveryReport): Either[Throwable, RecordMetadata] =
    for
      topic     <- Topic.from(report.topic).leftMap(error => invalidBackendValue("topic", report.topic, error))
      partition <- Partition.from(report.partition).leftMap(error => invalidBackendValue("partition", report.partition.toString, error))
      offset    <- report.offset.toOption.filter(_ >= 0d).traverse(exactOffset("offset", _))
    yield RecordMetadata(
      TopicPartition(topic, partition),
      offset,
      report.timestamp.toOption.filter(_ >= 0d).map(value => Timestamp.fromEpochMillis(value.toLong))
    )

  /** librdkafka reports offsets as JavaScript numbers, which are exact only below 2^53. */
  private def exactOffset(field: String, value: Double): Either[Throwable, Offset] =
    if value > MaxExactInteger then Left(new KafkaException.InvalidBackendResponse(s"$field $value exceeds the range JavaScript represents exactly"))
    else Offset.from(value.toLong).leftMap(error => invalidBackendValue(field, value.toString, error))

  private def rdFailure(error: confluent.RdError): KafkaException.BackendFailure =
    new KafkaException.BackendFailure(error.message, Some(error.code.toString), error.isRetriable.toOption, error.isFatal.toOption)

  private def callback[A](register: js.Function2[confluent.RdError | Null, A, Unit] => Unit): F[A] =
    F.async_ : resume =>
      register: (error, value) =>
        Option(error.asInstanceOf[confluent.RdError]) match
          case Some(failure) => resume(Left(rdFailure(failure)))
          case None          => resume(Right(value))

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

  private def await[A](promise: => js.Promise[A]): F[A] =
    F.fromPromise(F.delay(promise)).adaptError:
      case error: KafkaException => error
      case error                 => new KafkaException.BackendFailure(Option(error.getMessage).getOrElse(error.getClass.getName), cause = error)

  private def subscribe(consumer: confluent.Consumer, subscription: Subscription): F[Unit] =
    subscription match
      case Subscription.Topics(topics) =>
        val values = topics.toList.map[confluent.SubscriptionTopic](_.value).toJSArray
        await(consumer.subscribe(confluent.Values.subscription(values)))
      case Subscription.Pattern(pattern) => F.delay(new js.RegExp(pattern.anchored)).flatMap: compiled =>
          await(consumer.subscribe(confluent.Values.subscription(js.Array[confluent.SubscriptionTopic](compiled))))

  private def invalidBackendValue(field: String, value: String, error: Any, cause: Throwable = null): KafkaException.InvalidBackendResponse =
    new KafkaException.InvalidBackendResponse(s"$field '$value': $error", cause)

  private def parseLong(field: String, value: String): F[Long] =
    F.catchNonFatal(java.lang.Long.parseLong(value)).adaptError:
      case error => invalidBackendValue(field, value, error.getMessage, error)

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

  private def portableHeaders(headers: js.UndefOr[confluent.JsHeaders]): Headers =
    headers.toOption.fold(Headers.empty): values =>
      val result =
        js.Object.keys(values.asInstanceOf[js.Object]).iterator.flatMap: key =>
          val value = values(key)
          if js.Array.isArray(value) then value.asInstanceOf[js.Array[Uint8Array | Null]].iterator.map(raw => Header(key, bytes(raw)))
          else Iterator.single(Header(key, bytes(value.asInstanceOf)))
        .toVector
      Headers.fromVector(result)

  private final class ConfluentKafkaProducer[K, V](
      underlying: confluent.RdProducer,
      settings: ProducerSettings[F, K, V],
      pending: Ref[F, Map[Double, Deferred[F, Either[Throwable, RecordMetadata]]]],
      counter: Ref[F, Double]
  ) extends KafkaProducer[F, K, V]:

    /** Each record is enqueued with its own opaque token, so its delivery report is matched back to it exactly. */
    override def produce(records: NonEmptyList[ProducerRecord[K, V]]): F[F[ProducerResult[K, V]]] =
      for
        encoded <- records.traverse(encodeRecord)
        awaited <- encoded.traverse(record => register.map(record -> _))
        _       <- F.delay(awaited.toList.foreach((record, token) => enqueue(record, token._1)))
      yield awaited.traverse((record, token) => token._2.get.flatMap(F.fromEither).map(metadata => record.source -> Some(metadata)))
        .map(ProducerResult(_))

    private def register: F[(Double, Deferred[F, Either[Throwable, RecordMetadata]])] =
      for
        token    <- counter.updateAndGet(_ + 1d)
        deferred <- Deferred[F, Either[Throwable, RecordMetadata]]
        _        <- pending.update(_.updated(token, deferred))
      yield token -> deferred

    private def enqueue(record: EncodedRecord[K, V], token: Double): Unit =
      val enqueued: js.Any =
        underlying.produce(
          record.source.topic.value,
          record.source.partition.map(_.value).orUndefined,
          nodeBuffer(record.value),
          nodeBuffer(record.key),
          record.source.timestamp.map(_.epochMillis.toDouble).orUndefined,
          token,
          record.headers
        )
      ignore(enqueued)

    private def encodeRecord(record: ProducerRecord[K, V]): F[EncodedRecord[K, V]] =
      (
        settings.keySerializer.serialize(record.topic, record.headers, record.key),
        settings.valueSerializer.serialize(record.topic, record.headers, record.value)
      ).mapN((key, value) => EncodedRecord(record, key, value, rdHeaders(record.headers)))

  /** librdkafka takes headers as an ordered array of single-entry objects, so duplicate names keep their produced order. */
  private def rdHeaders(headers: Headers): js.Array[confluent.RdHeader] =
    headers.values.map(header => confluent.Values.rdHeader(header.key, nodeBuffer(header.value))).toJSArray

  private final case class EncodedRecord[K, V](
      source: ProducerRecord[K, V],
      key: Option[Chunk[Byte]],
      value: Option[Chunk[Byte]],
      headers: js.Array[confluent.RdHeader]
  )

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

    override def beginningOffsets(topicPartitions: Set[TopicPartition]): F[Map[TopicPartition, Offset]] =
      boundaryOffsets(topicPartitions, "beginning offset", _.low)

    override def endOffsets(topicPartitions: Set[TopicPartition]): F[Map[TopicPartition, Offset]] =
      boundaryOffsets(topicPartitions, "end offset", _.high)

    override def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Timestamp]): F[Map[TopicPartition, Option[Offset]]] =
      if timestampsToSearch.isEmpty then F.pure(Map.empty)
      else
        admin.use: value =>
          timestampsToSearch.toList.groupBy(_._1.topic).toList.traverse: (topic, requested) =>
            await(value.fetchTopicOffsets(topic.value)).flatMap: boundaries =>
              val ends = boundaries.iterator.map(offsets => offsets.partition -> offsets.high).toMap
              requested.groupBy(_._2).toList.traverse: (timestamp, searches) =>
                jsTimestamp(timestamp).flatMap: instant =>
                  await(value.fetchTopicOffsetsByTimestamp(topic.value, instant)).flatMap: returned =>
                    val found = returned.iterator.map(offsets => offsets.partition -> offsets.offset).toMap
                    searches.traverse: (topicPartition, _) =>
                      (found.get(topicPartition.partition.value), ends.get(topicPartition.partition.value)) match
                        case (Some(raw), Some(rawEnd)) => (optionalOffset("timestamp offset", raw), offset("end offset", rawEnd)).mapN:
                            (candidate, end) => topicPartition -> candidate.filterNot(_ == end)
                        case (None, _) => F.raiseError(missingOffset("timestamp offset", topicPartition))
                        case (_, None) => F.raiseError(missingOffset("end offset", topicPartition))
              .map(_.flatten)
          .map(_.flatten.toMap)

    override def partitionsFor(topic: Topic): F[Set[Partition]] =
      topicMetadata(Some(js.Array(topic.value))).flatMap: values =>
        values.find(_.name == topic.value) match
          case Some(value) => value.partitions.toList.traverse(portablePartition).map(_.toSet)
          case None        => F.raiseError(new KafkaException.InvalidBackendResponse(s"missing metadata for topic '${topic.value}'"))

    override def listTopics: F[Map[Topic, Set[Partition]]] =
      topicMetadata(None).flatMap(
        _.toList.traverse: value =>
          (topic(value.name), value.partitions.toList.traverse(portablePartition).map(_.toSet)).mapN(_ -> _)
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

    private def portablePartition(value: confluent.PartitionMetadata): F[Partition] = partition(value.partitionId)

    private def optionalOffset(field: String, value: String | Null): F[Option[Offset]] =
      Option(value).fold(F.pure(Option.empty[Offset])): raw =>
        parseLong(field, raw).flatMap: parsed =>
          if parsed < 0L then F.pure(None)
          else F.fromEither(Offset.from(parsed).leftMap(error => invalidBackendValue(field, raw, error)).map(Some(_)))

    private def jsTimestamp(timestamp: Timestamp): F[Double] =
      val value = timestamp.epochMillis
      if value >= -9007199254740991L && value <= 9007199254740991L then F.pure(value.toDouble)
      else F.raiseError(new IllegalArgumentException(s"timestamp $value cannot be represented exactly by Confluent Kafka JavaScript"))

    private def boundaryOffsets(
        topicPartitions: Set[TopicPartition],
        field: String,
        select: confluent.TopicOffsets => String
    ): F[Map[TopicPartition, Offset]] =
      if topicPartitions.isEmpty then F.pure(Map.empty)
      else
        admin.use: value =>
          topicPartitions.groupBy(_.topic).toList.traverse: (topic, requested) =>
            await(value.fetchTopicOffsets(topic.value)).flatMap: returned =>
              val indexed = returned.iterator.map(offsets => offsets.partition -> offsets).toMap
              requested.toList.traverse: topicPartition =>
                indexed.get(topicPartition.partition.value) match
                  case Some(offsets) => offset(field, select(offsets)).map(topicPartition -> _)
                  case None          => F.raiseError(missingOffset(field, topicPartition))
          .map(_.flatten.toMap)

    private def missingOffset(field: String, topicPartition: TopicPartition): KafkaException.InvalidBackendResponse =
      new KafkaException.InvalidBackendResponse(s"missing $field for $topicPartition")

    private def topicMetadata(topics: Option[js.Array[String]]): F[js.Array[confluent.TopicMetadata]] =
      admin.use(value => await(value.fetchTopicMetadata(confluent.Values.topicMetadataOptions(topics.orUndefined))))

    private def admin: Resource[F, confluent.Admin] =
      Resource.eval(F.delay(underlying.dependentAdmin())).flatMap: value =>
        Resource.make(await(value.connect()))(_ => await(value.disconnect())).as(value)
