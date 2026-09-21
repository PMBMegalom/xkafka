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
import cats.effect.std.Dispatcher
import fs2.concurrent.SignallingRef
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import internal.confluent
import internal.security.SecurityProperties

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
  ): confluent.RdConsumer

private object ConfluentKafkaDriver:
  val live: ConfluentKafkaDriver =
    new ConfluentKafkaDriver:
      override def producer(settings: ClientSettings, properties: Map[String, String]): confluent.RdProducer =
        val config =
          confluent.Values.rdProducerConfig(
            settings.bootstrapServers.toList.toJSArray,
            settings.clientId.orUndefined,
            settings.properties ++ properties ++ SecurityProperties.librdkafka(settings.security)
          )
        js.Dynamic.newInstance(confluent.RdKafka.Producer)(config).asInstanceOf[confluent.RdProducer]

      override def consumer(
          settings: ClientSettings,
          groupId: ConsumerGroup,
          autoOffsetReset: AutoOffsetReset,
          properties: Map[String, String]
      ): confluent.RdConsumer =
        val config =
          confluent.Values.rdConsumerConfig(
            settings.bootstrapServers.toList.toJSArray,
            settings.clientId.orUndefined,
            groupId.value,
            autoOffsetReset,
            settings.properties ++ properties ++ SecurityProperties.librdkafka(settings.security)
          )
        js.Dynamic.newInstance(confluent.RdKafka.KafkaConsumer)(config).asInstanceOf[confluent.RdConsumer]

private final class ConfluentKafkaClient[F[_]](driver: ConfluentKafkaDriver)(using F: Async[F]) extends KafkaClient[F]:
  private val DeliveryPollIntervalMillis = 10
  private val ConsumeTimeoutMillis       = 500
  private val ConsumeBatchSize           = 256
  private val RequestTimeoutMillis       = 10000
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
          rdError(error) match
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

  /** node-rdkafka signals success with either null or undefined, and in Scala.js only the first of those is `null`. */
  private def rdError(error: confluent.RdError | Null): Option[confluent.RdError] =
    val value = error.asInstanceOf[js.Any]
    if value == null || js.isUndefined(value) then None else Some(error.asInstanceOf[confluent.RdError])

  private def rdFailure(error: confluent.RdError): KafkaException.BackendFailure =
    new KafkaException.BackendFailure(error.message, Some(ErrorCode.fromLibrdkafka(error.code)), error.isRetriable.toOption, error.isFatal.toOption)

  private def callback[A](register: js.Function2[confluent.RdError | Null, A, Unit] => Unit): F[A] =
    F.async_ : resume =>
      register: (error, value) =>
        rdError(error) match
          case Some(failure) => resume(Left(rdFailure(failure)))
          case None          => resume(Right(value))

  override def consumer[K, V](settings: ConsumerSettings[F, K, V], subscription: Subscription): Resource[F, KafkaConsumer[F, K, V]] =
    for
      underlying  <- Resource.eval(F.delay(driver.consumer(settings.client, settings.groupId, settings.autoOffsetReset, settings.properties)))
      dispatcher  <- Dispatcher.sequential[F]
      assignments <- Resource.eval(SignallingRef[F, Set[TopicPartition]](Set.empty))
      _           <- Resource.eval(F.delay(underlying.on("rebalance", rebalanced(underlying, dispatcher, assignments))))
      _ <- Resource.make(callback[js.Any](done => underlying.connect((), done)).void)(_ => callback[js.Any](done => underlying.disconnect(done)).void)
      _ <- Resource.eval(F.delay(underlying.setDefaultConsumeTimeout(ConsumeTimeoutMillis)))
      _ <- Resource.eval(subscribe(underlying, subscription))
    yield new ConfluentKafkaConsumer(underlying, settings, assignments)

  /** node-rdkafka emits the event before it applies the change, and reports only the partitions added or revoked, which differ by rebalance protocol.
    *
    * The effect the dispatcher schedules runs after the synchronous handler has assigned, and reads the whole assignment, so neither detail matters
    * here.
    */
  private def rebalanced(
      underlying: confluent.RdConsumer,
      dispatcher: Dispatcher[F],
      assignments: SignallingRef[F, Set[TopicPartition]]
  ): js.Function2[confluent.RdError | Null, js.Array[confluent.RdTopicPartition], Unit] =
    (_, _) =>
      dispatcher.unsafeRunAndForget(
        F.delay(underlying.assignments()).flatMap(_.toList.traverse(portableTopicPartition).map(_.toSet)).flatMap(assignments.set)
      )

  private def portableTopicPartition(value: confluent.RdTopicPartition): F[TopicPartition] =
    (topic(value.topic), partition(value.partition)).mapN(TopicPartition.apply)

  private def subscribe(consumer: confluent.RdConsumer, subscription: Subscription): F[Unit] =
    val topics =
      subscription match
        case Subscription.Topics(values) => values.toList.map[confluent.SubscriptionTopic](_.value).toJSArray
        // librdkafka reads a topic beginning with "^" as a regular expression, which is what anchoring already produces.
        case Subscription.Pattern(pattern) => js.Array[confluent.SubscriptionTopic](pattern.anchored)
    F.delay(consumer.subscribe(topics)).void

  private def invalidBackendValue(field: String, value: String, error: Any, cause: Throwable = null): KafkaException.InvalidBackendResponse =
    new KafkaException.InvalidBackendResponse(s"$field '$value': $error", cause)

  private def topic(value: String): F[Topic] = F.fromEither(Topic.from(value).leftMap(error => invalidBackendValue("topic", value, error)))

  private def partition(value: Int): F[Partition] =
    F.fromEither(Partition.from(value).leftMap(error => invalidBackendValue("partition", value.toString, error)))

  private def bytes(value: Uint8Array | Null): Option[Chunk[Byte]] =
    Option(value).map: raw =>
      val array = raw.asInstanceOf[Uint8Array]
      Chunk.array(Array.tabulate(array.length)(index => array(index).toByte))

  private def nodeBuffer(value: Option[Chunk[Byte]]): Uint8Array | Null =
    value.map: chunk =>
      val array = new Uint8Array(chunk.size)
      chunk.iterator.zipWithIndex.foreach:
        case (byte, index) => array(index) = byte.toShort
      confluent.Buffer.from(array)
    .orNull

  /** librdkafka hands back one single-entry object per header, so duplicate names and their order both survive the round trip. */
  private def portableHeaders(headers: js.UndefOr[js.Array[confluent.RdHeader]]): Headers =
    Headers.fromVector(
      headers.toOption.fold(Vector.empty[Header]): values =>
        values.toVector.flatMap: entry =>
          entry.iterator.map: (name, value) =>
            Header(name, Option(value.asInstanceOf[Uint8Array]).map(raw => Chunk.array(Array.tabulate(raw.length)(index => raw(index).toByte))))
    )

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
      underlying: confluent.RdConsumer,
      settings: ConsumerSettings[F, K, V],
      assignments: SignallingRef[F, Set[TopicPartition]]
  ) extends KafkaConsumer[F, K, V]:

    private val offsetCommitter: OffsetCommitter[F] =
      new OffsetCommitter[F]:
        override def commit(offsets: Map[TopicPartition, Offset]): F[Unit] =
          val values =
            offsets.iterator.map: (topicPartition, offset) =>
              confluent.Values.rdTopicPartitionOffset(topicPartition.topic.value, topicPartition.partition.value, offset.value.toDouble)
            .toJSArray
          F.delay(underlying.commit(values)).void

    /** Pulls batches from librdkafka. An empty batch means the consume timeout elapsed with nothing available. */
    override val records: Stream[F, CommittableConsumerRecord[F, K, V]] =
      Stream.repeatEval(fetch).flatMap(batch => Stream.emits(batch.toList)).evalMap(consumerRecord)

    private def fetch: F[js.Array[confluent.RdMessage]] = callback[js.Array[confluent.RdMessage]](done => underlying.consume(ConsumeBatchSize, done))

    /** librdkafka reports rebalances, so nothing is polled. */
    override val assignmentChanges: Stream[F, Set[TopicPartition]] = assignments.discrete

    override def assignment: F[Set[TopicPartition]] =
      F.delay(underlying.assignments()).flatMap(_.toList.traverse(portableTopicPartition).map(_.toSet))

    override def committed(topicPartitions: Set[TopicPartition]): F[Map[TopicPartition, Option[Offset]]] =
      if topicPartitions.isEmpty then F.pure(Map.empty)
      else
        val requested = topicPartitions.iterator.map(value => confluent.Values.rdTopicPartition(value.topic.value, value.partition.value)).toJSArray
        callback[js.Array[confluent.RdTopicPartitionOffset]](done => underlying.committed(requested, RequestTimeoutMillis, done): Unit).flatMap:
          values =>
            values.toList.traverse: value =>
              (portableTopicPartition(value), optionalOffset("committed offset", value.offset)).mapN(_ -> _)
            .map(_.toMap)

    override def beginningOffsets(topicPartitions: Set[TopicPartition]): F[Map[TopicPartition, Offset]] =
      watermarks(topicPartitions, "beginning offset", _.lowOffset)

    override def endOffsets(topicPartitions: Set[TopicPartition]): F[Map[TopicPartition, Offset]] =
      watermarks(topicPartitions, "end offset", _.highOffset)

    /** librdkafka queries the broker for both watermarks in one call per partition, so no separate admin client is needed. */
    private def watermarks(
        topicPartitions: Set[TopicPartition],
        field: String,
        select: confluent.RdWatermarks => Double
    ): F[Map[TopicPartition, Offset]] =
      topicPartitions.toList.traverse: topicPartition =>
        callback[confluent.RdWatermarks] { done =>
          underlying.queryWatermarkOffsets(topicPartition.topic.value, topicPartition.partition.value, RequestTimeoutMillis, done): Unit
        }.flatMap(value => F.fromEither(exactOffset(field, select(value))).map(topicPartition -> _))
      .map(_.toMap)

    /** Native offset lookup, so no watermark comparison is needed to tell "past the end" from a real match. */
    override def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Timestamp]): F[Map[TopicPartition, Option[Offset]]] =
      if timestampsToSearch.isEmpty then F.pure(Map.empty)
      else
        val requested =
          timestampsToSearch.iterator.map: (topicPartition, timestamp) =>
            confluent.Values.rdTopicPartitionOffset(topicPartition.topic.value, topicPartition.partition.value, timestamp.epochMillis.toDouble)
          .toJSArray
        callback[js.Array[confluent.RdTopicPartitionOffset]](done => underlying.offsetsForTimes(requested, RequestTimeoutMillis, done)).flatMap:
          values =>
            values.toList.traverse: value =>
              (portableTopicPartition(value), optionalOffset("timestamp offset", value.offset)).mapN(_ -> _)
            .map(found => timestampsToSearch.keys.map(topicPartition => topicPartition -> found.toMap.getOrElse(topicPartition, None)).toMap)

    override def partitionsFor(topic: Topic): F[Set[Partition]] =
      metadata(Some(topic.value)).flatMap: values =>
        values.find(_.name == topic.value) match
          case Some(value) => value.partitions.toList.traverse(entry => portablePartition(entry.id)).map(_.toSet)
          case None        => F.raiseError(new KafkaException.InvalidBackendResponse(s"missing metadata for topic '${topic.value}'"))

    override def listTopics: F[Map[Topic, Set[Partition]]] =
      metadata(None).flatMap:
        _.toList.traverse: value =>
          (portableTopic(value.name), value.partitions.toList.traverse(entry => portablePartition(entry.id)).map(_.toSet)).mapN(_ -> _)
      .map(_.toMap)

    private def metadata(topic: Option[String]): F[js.Array[confluent.RdTopicMetadata]] =
      callback[confluent.RdMetadata](done => underlying.getMetadata(confluent.Values.rdMetadataOptions(topic.orUndefined), done): Unit).map(_.topics)

    override def seek(topicPartition: TopicPartition, offset: Offset): F[Unit] =
      F.async_ : resume =>
        underlying.seek(
          confluent.Values.rdTopicPartitionOffset(topicPartition.topic.value, topicPartition.partition.value, offset.value.toDouble),
          RequestTimeoutMillis,
          error =>
            rdError(error) match
              case Some(failure) => resume(Left(rdFailure(failure)))
              case None          => resume(Right(()))
        ): Unit

    private def consumerRecord(message: confluent.RdMessage): F[CommittableConsumerRecord[F, K, V]] =
      val headers = portableHeaders(message.headers)
      for
        portableTopic      <- topic(message.topic)
        partition          <- portablePartition(message.partition)
        portableOffset     <- F.fromEither(exactOffset("offset", message.offset))
        portableNextOffset <- F.fromEither(portableOffset.next.leftMap(error => invalidBackendValue("next offset", message.offset.toString, error)))
        key                <- settings.keyDeserializer.deserialize(portableTopic, headers, bytes(message.key))
        value              <- settings.valueDeserializer.deserialize(portableTopic, headers, bytes(message.value))
      yield
        val topicPartition = TopicPartition(portableTopic, partition)
        val record         =
          ConsumerRecord(
            topicPartition,
            portableOffset,
            message.timestamp.toOption.filter(_ >= 0d).map(value => Timestamp.fromEpochMillis(value.toLong)),
            key,
            value,
            headers
          )
        val committableOffset =
          new CommittableOffset[F]:
            override val topicPartition: TopicPartition = TopicPartition(portableTopic, partition)
            override val nextOffset: Offset             = portableNextOffset
            override val committer: OffsetCommitter[F]  = offsetCommitter

        CommittableConsumerRecord(record, committableOffset)

    private def portableTopic(value: String): F[Topic] = topic(value)

    private def portablePartition(value: Int): F[Partition] = partition(value)

    /** librdkafka signals "no offset" either by omitting it or with a negative sentinel. */
    private def optionalOffset(field: String, value: js.UndefOr[Double]): F[Option[Offset]] =
      value.toOption.filter(_ >= 0d) match
        case Some(offset) => F.fromEither(exactOffset(field, offset)).map(Some(_))
        case None         => F.pure(None)
