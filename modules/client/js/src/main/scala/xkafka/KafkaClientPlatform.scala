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
import scala.scalajs.js.typedarray.{byteArray2Int8Array, int8Array2ByteArray, Int8Array, Uint8Array}

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{Async, Deferred, Ref, Resource}
import cats.effect.implicits.*
import cats.effect.std.{Dispatcher, Queue}
import fs2.concurrent.SignallingRef
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import internal.{confluent, ClientProperties}
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
            settings.properties ++ properties ++ SecurityProperties.librdkafka(settings.security) ++ ClientProperties(settings)
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
            settings.properties ++ properties ++ SecurityProperties.librdkafka(settings.security) ++ ClientProperties(settings)
          )
        js.Dynamic.newInstance(confluent.RdKafka.KafkaConsumer)(config).asInstanceOf[confluent.RdConsumer]

private final class ConfluentKafkaClient[F[_]](driver: ConfluentKafkaDriver)(using F: Async[F]) extends KafkaClient[F]:
  private val DeliveryPollIntervalMillis = 10
  private val ConsumeBatchSize           = 256
  // Records the poll loop has read but nothing has taken yet. The loop stops consuming when this fills, which is
  // also when Kafka would consider the consumer stalled.
  private val RecordQueueSize = 256
  private val MaxExactInteger = 9007199254740991d
  // Admin calls are metadata round trips, so they are bounded well below the metadata refresh interval.
  private val AdminTimeoutMillis = 30000
  // librdkafka's sentinels for the ends of a partition's log.
  private val BeginningOffset = -2d
  private val EndOffset       = -1d

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

  override def consumer[K, V](settings: ConsumerSettings[F, K, V], selection: Selection): Resource[F, KafkaConsumer[F, K, V]] =
    for
      underlying  <- Resource.eval(F.delay(driver.consumer(settings.client, settings.groupId, settings.autoOffsetReset, settings.properties)))
      dispatcher  <- Dispatcher.sequential[F]
      assignments <- Resource.eval(SignallingRef[F, Set[TopicPartition]](Set.empty))
      // Neither the consume nor the rebalance reporting can carry its own failure out, and a consumer that has lost
      // either one receives nothing further, so the first failure is kept and reported to whoever reads from it.
      failure <- Resource.eval(Deferred[F, Throwable])
      _       <- Resource.eval(F.delay(underlying.on("rebalance", rebalanced(underlying, dispatcher, assignments, failure))))
      _ <- Resource.make(callback[js.Any](done => underlying.connect((), done)).void)(_ => callback[js.Any](done => underlying.disconnect(done)).void)
      _ <- Resource.eval(F.delay(underlying.setDefaultConsumeTimeout(settings.pollTimeout.toMillis.toInt)))
      _ <- Resource.eval(select(underlying, selection))
      polled <- Resource.eval(Queue.bounded[F, confluent.RdMessage](RecordQueueSize))
      consumer = new ConfluentKafkaConsumer(underlying, settings, assignments, polled, failure)
      // Started after the selection and cancelled before the disconnect that follows it.
      _ <- consumer.pollLoop.compile.drain.onError(failure.complete(_).void).background
    yield consumer

  /** node-rdkafka emits the event before it applies the change, and reports only the partitions added or revoked, which differ by rebalance protocol.
    *
    * The effect the dispatcher schedules runs after the synchronous handler has assigned, and reads the whole assignment, so neither detail matters
    * here.
    */
  private def rebalanced(
      underlying: confluent.RdConsumer,
      dispatcher: Dispatcher[F],
      assignments: SignallingRef[F, Set[TopicPartition]],
      failure: Deferred[F, Throwable]
  ): js.Function2[confluent.RdError | Null, js.Array[confluent.RdTopicPartition], Unit] =
    (_, _) =>
      dispatcher.unsafeRunAndForget(
        F.delay(underlying.assignments()).flatMap(_.toList.traverse(portableTopicPartition).map(_.toSet)).flatMap(assignments.set)
          // The dispatcher discards this effect's outcome, so an assignment the backend reports in terms this client
          // rejects would otherwise stop the tracking without anything saying so.
          .onError(failure.complete(_).void)
      )

  private def portableTopicPartition(value: confluent.RdTopicPartition): F[TopicPartition] =
    (topic(value.topic), partition(value.partition)).mapN(TopicPartition.apply)

  override def admin(settings: ClientSettings): Resource[F, KafkaAdminClient[F]] =
    val config =
      confluent.Values.rdAdminConfig(
        settings.bootstrapServers.toList.toJSArray,
        settings.clientId.orUndefined,
        settings.properties ++ SecurityProperties.librdkafka(settings.security) ++ ClientProperties(settings)
      )
    Resource.make(F.delay(confluent.RdAdminClient.create(config)))(value => F.delay(value.disconnect()))
      .map(new ConfluentKafkaAdminClient(_, settings))

  private final class ConfluentKafkaAdminClient(underlying: confluent.RdAdmin, settings: ClientSettings) extends KafkaAdminClient[F]:
    private val requestTimeoutMillis = settings.metadataRefreshInterval.toMillis.toInt.min(AdminTimeoutMillis)

    /** The client creates one topic per call, so each topic's outcome arrives on its own. */
    override def createTopics(topics: NonEmptySet[NewTopic]): F[Unit] =
      topics.traverse_ : value =>
        outcome(done =>
          underlying.createTopic(
            confluent.Values.rdNewTopic(value.topic.value, value.partitions, value.replicationFactor, value.configuration),
            requestTimeoutMillis,
            done
          )
        )

    override def deleteTopics(topics: NonEmptySet[Topic]): F[Unit] =
      topics.traverse_(value => outcome(done => underlying.deleteTopic(value.value, requestTimeoutMillis, done)))

    override def createPartitions(topic: Topic, count: Int): F[Unit] =
      outcome(done => underlying.createPartitions(topic.value, count, requestTimeoutMillis, done))

    override def describeTopics(topics: NonEmptySet[Topic]): F[Map[Topic, Set[Partition]]] =
      F.async_[js.Array[confluent.RdTopicDescription]]: resume =>
        underlying.describeTopics(
          topics.toSortedSet.toList.map(_.value).toJSArray,
          js.Dictionary("timeout" -> requestTimeoutMillis),
          (error, described) =>
            rdError(error) match
              case Some(failure) => resume(Left(rdFailure(failure)))
              case None          => resume(Right(described))
        )
      .flatMap: described =>
        described.toList.traverse: value =>
          value.error.toOption match
            // The client reports a topic it could not describe in the description itself, rather than failing the call.
            case Some(failure) => F.raiseError[(Topic, Set[Partition])](rdFailure(failure))
            case None          =>
              for
                portableTopic <- topic(value.name)
                partitions    <- value.partitions.toList.traverse(info => partition(info.partition))
              yield portableTopic -> partitions.toSet
        .map(_.toMap)

    private def outcome(register: js.Function1[confluent.RdError | Null, Unit] => Unit): F[Unit] =
      F.async_ : resume =>
        register: error =>
          rdError(error) match
            case Some(failure) => resume(Left(rdFailure(failure)))
            case None          => resume(Right(()))

  private def select(consumer: confluent.RdConsumer, selection: Selection): F[Unit] =
    selection match
      case Selection.Topics(values) => F.delay(consumer.subscribe(values.toSortedSet.toList.map[confluent.SubscriptionTopic](_.value).toJSArray)).void
      // librdkafka reads a topic beginning with "^" as a regular expression, which is what anchoring already produces.
      case Selection.Pattern(pattern)            => F.delay(consumer.subscribe(js.Array[confluent.SubscriptionTopic](pattern.anchored))).void
      case Selection.Partitions(topicPartitions) =>
        val assigned =
          topicPartitions.toSortedSet.toList.map(value => confluent.Values.rdTopicPartition(value.topic.value, value.partition.value)).toJSArray
        F.delay(consumer.assign(assigned)).void

  private def invalidBackendValue(field: String, value: String, error: Any, cause: Throwable = null): KafkaException.InvalidBackendResponse =
    new KafkaException.InvalidBackendResponse(s"$field '$value': $error", cause)

  private def topic(value: String): F[Topic] = F.fromEither(Topic.from(value).leftMap(error => invalidBackendValue("topic", value, error)))

  private def partition(value: Int): F[Partition] =
    F.fromEither(Partition.from(value).leftMap(error => invalidBackendValue("partition", value.toString, error)))

  /** Records carry their payload through here, so both directions move the bytes as one typed array. */
  private def bytes(value: Uint8Array | Null): Option[Chunk[Byte]] = Option(value).map(raw => portableBytes(raw.asInstanceOf[Uint8Array]))

  private def portableBytes(value: Uint8Array): Chunk[Byte] =
    Chunk.array(int8Array2ByteArray(new Int8Array(value.buffer, value.byteOffset, value.length)))

  private def nodeBuffer(value: Option[Chunk[Byte]]): Uint8Array | Null =
    value.map: chunk =>
      val signed = byteArray2Int8Array(chunk.toArray)
      confluent.Buffer.from(new Uint8Array(signed.buffer, signed.byteOffset, signed.length))
    .orNull

  /** librdkafka hands back one single-entry object per header, so duplicate names and their order both survive the round trip. */
  private def portableHeaders(headers: js.UndefOr[js.Array[confluent.RdHeader]]): Headers =
    Headers.fromVector(
      headers.toOption.fold(Vector.empty[Header]): values =>
        values.toVector.flatMap: entry =>
          entry.iterator.map: (name, value) =>
            Header(name, Option(value.asInstanceOf[Uint8Array]).map(portableBytes))
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
      assignments: SignallingRef[F, Set[TopicPartition]],
      polled: Queue[F, confluent.RdMessage],
      failure: Deferred[F, Throwable]
  ) extends KafkaConsumer[F, K, V]:

    private val requestTimeoutMillis = settings.requestTimeout.toMillis.toInt

    override val pausing: PartitionPausing[F] =
      new PartitionPausing.Backend[F]:
        override def pause(topicPartitions: Set[TopicPartition]): F[Unit] =
          F.whenA(topicPartitions.nonEmpty)(F.delay(underlying.pause(requested(topicPartitions))))

        override def resume(topicPartitions: Set[TopicPartition]): F[Unit] =
          F.whenA(topicPartitions.nonEmpty)(F.delay(underlying.resume(requested(topicPartitions))))

    private def requested(topicPartitions: Set[TopicPartition]): js.Array[confluent.RdTopicPartition] =
      topicPartitions.iterator.map(value => confluent.Values.rdTopicPartition(value.topic.value, value.partition.value)).toJSArray

    private val offsetCommitter: OffsetCommitter[F] =
      new OffsetCommitter[F]:
        override def commit(offsets: Map[TopicPartition, Offset]): F[Unit] =
          val values =
            offsets.iterator.map: (topicPartition, offset) =>
              confluent.Values.rdTopicPartitionOffset(topicPartition.topic.value, topicPartition.partition.value, offset.value.toDouble)
            .toJSArray
          F.delay(underlying.commit(values)).void

    /** Pulls batches from librdkafka. An empty batch means the consume timeout elapsed with nothing available. */
    /** The consumer's single consume, which both its records and its rebalance events come from.
      *
      * The client reports a rebalance from its own consume, so nothing observes one unless this keeps running, whether or not anything is reading
      * records.
      */
    val pollLoop: Stream[F, Nothing] = Stream.repeatEval(fetch).flatMap(batch => Stream.emits(batch.toList)).evalMap(polled.offer).drain

    override val records: Stream[F, CommittableConsumerRecord[F, K, V]] =
      Stream.fromQueueUnterminated(polled).evalMap(consumerRecord).concurrently(Stream.exec(failure.get.flatMap(F.raiseError[Unit])))

    private def fetch: F[js.Array[confluent.RdMessage]] =
      callback[js.Array[confluent.RdMessage]](done => underlying.consume(ConsumeBatchSize, done)).recover:
        // librdkafka reports a subscribed topic that does not exist yet through the consume, and the topic can still be
        // created afterwards, so this keeps consuming for it.
        case unavailable: KafkaException.BackendFailure if unavailable.code.contains(ErrorCode.UnknownTopicOrPartition) => js.Array()

    override val assignmentChanges: Stream[F, Set[TopicPartition]] =
      assignments.discrete.concurrently(Stream.exec(failure.get.flatMap(F.raiseError[Unit])))

    override def assignment: F[Set[TopicPartition]] =
      F.delay(underlying.assignments()).flatMap(_.toList.traverse(portableTopicPartition).map(_.toSet))

    override def committed(topicPartitions: Set[TopicPartition]): F[Map[TopicPartition, Option[Offset]]] =
      if topicPartitions.isEmpty then F.pure(Map.empty)
      else
        val requested = topicPartitions.iterator.map(value => confluent.Values.rdTopicPartition(value.topic.value, value.partition.value)).toJSArray
        callback[js.Array[confluent.RdTopicPartitionOffset]](done => underlying.committed(requested, requestTimeoutMillis, done): Unit).flatMap:
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
          underlying.queryWatermarkOffsets(topicPartition.topic.value, topicPartition.partition.value, requestTimeoutMillis, done): Unit
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
        callback[js.Array[confluent.RdTopicPartitionOffset]](done => underlying.offsetsForTimes(requested, requestTimeoutMillis, done)).flatMap:
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
          requestTimeoutMillis,
          error =>
            rdError(error) match
              case Some(failure) => resume(Left(rdFailure(failure)))
              case None          => resume(Right(()))
        ): Unit

    override def seekToBeginning(topicPartitions: Set[TopicPartition]): F[Unit] = seekAll(topicPartitions, BeginningOffset)

    override def seekToEnd(topicPartitions: Set[TopicPartition]): F[Unit] = seekAll(topicPartitions, EndOffset)

    /** librdkafka reads these two offsets as the ends of the log, and the seek carries them through untouched. */
    private def seekAll(topicPartitions: Set[TopicPartition], offset: Double): F[Unit] =
      topicPartitions.toList.traverse_ : value =>
        F.async_ : resume =>
          underlying.seek(
            confluent.Values.rdTopicPartitionOffset(value.topic.value, value.partition.value, offset),
            requestTimeoutMillis,
            error =>
              rdError(error) match
                case Some(failure) => resume(Left(rdFailure(failure)))
                case None          => resume(Right(()))
          ): Unit

    override def position(topicPartition: TopicPartition): F[Option[Offset]] =
      F.delay(underlying.position(js.Array(requested(Set(topicPartition)).head))).flatMap: values =>
        values.headOption.flatMap(_.offset.toOption).filter(_ >= 0d) match
          case None        => F.pure(None)
          case Some(value) => F.fromEither(exactOffset("position", value)).map(_.some)

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
