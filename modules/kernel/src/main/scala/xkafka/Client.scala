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

import scala.concurrent.duration.FiniteDuration

import cats.{Applicative, Foldable, Show}
import cats.arrow.FunctionK
import cats.data.{NonEmptyList, Validated, ValidatedNel}
import cats.effect.{Async, Temporal}
import cats.tagless.FunctorK
import fs2.{Pipe, Stream}

/** Backend property names xkafka derives from the typed settings.
  *
  * Settings construction rejects them, so the portable model stays the single source for these values.
  */
val ManagedProperties: Set[String] =
  Set("bootstrap.servers", "client.id", "group.id", "auto.offset.reset", "enable.auto.commit", "enable.auto.offset.store")

enum SettingsError derives CanEqual:
  case BlankBootstrapServer(index: Int)
  case InvalidBootstrapServer(index: Int, value: String)
  case BlankPropertyName(scope: SettingsError.PropertyScope)
  case ManagedProperty(name: String, scope: SettingsError.PropertyScope)

  def message: String =
    this match
      case BlankBootstrapServer(index)          => s"bootstrapServers[$index] must not be blank"
      case InvalidBootstrapServer(index, value) => s"bootstrapServers[$index] must be host:port, was '$value'"
      case BlankPropertyName(scope)             => s"${scope.label} properties must not contain a blank name"
      case ManagedProperty(name, scope)         => s"${scope.label} property '$name' is managed by xkafka and must be set through typed settings"

object SettingsError:
  enum PropertyScope(val label: String) derives CanEqual:
    case Client   extends PropertyScope("client")
    case Producer extends PropertyScope("producer")
    case Consumer extends PropertyScope("consumer")

  given Show[SettingsError] = Show.show(_.message)

private def validateSettings(errors: List[SettingsError]): ValidatedNel[SettingsError, Unit] =
  NonEmptyList.fromList(errors).fold[ValidatedNel[SettingsError, Unit]](Validated.Valid(()))(Validated.Invalid(_))

private def propertyErrors(properties: Map[String, String], scope: SettingsError.PropertyScope): List[SettingsError] =
  val blank   = Option.when(properties.keysIterator.exists(_.trim.isEmpty))(SettingsError.BlankPropertyName(scope)).toList
  val managed = properties.keysIterator.filter(name => ManagedProperties.contains(name.trim.toLowerCase)).toList.sorted
  blank ++ managed.map(SettingsError.ManagedProperty(_, scope))

/** Accepts `host:port`, including bracketed IPv6 literals, so an unusable endpoint is caught at construction. */
private def bootstrapServerError(value: String, index: Int): Option[SettingsError] =
  val trimmed   = value.trim
  val separator = if trimmed.startsWith("[") then trimmed.indexOf(']') + 1 else trimmed.lastIndexOf(':')
  val host      = if separator > 0 then trimmed.substring(0, separator).stripPrefix("[").stripSuffix("]") else ""
  val port      = if separator > 0 && separator < trimmed.length then trimmed.substring(separator + 1) else ""
  val validPort = port.nonEmpty && port.forall(_.isDigit) && port.toIntOption.exists(value => value >= 1 && value <= 65535)
  if trimmed.isEmpty then Some(SettingsError.BlankBootstrapServer(index))
  else if host.isEmpty || !validPort then Some(SettingsError.InvalidBootstrapServer(index, value))
  else None

private def redacted(properties: Map[String, String]): Map[String, String] = properties.keysIterator.map(_ -> "<redacted>").toMap

sealed abstract case class ClientSettings private (bootstrapServers: NonEmptyList[String], clientId: Option[String], properties: Map[String, String]):
  def withBootstrapServers(values: NonEmptyList[String]): ValidatedNel[SettingsError, ClientSettings] =
    ClientSettings.from(values, clientId, properties)

  def withClientId(value: String): ClientSettings = new ClientSettings(bootstrapServers, Some(value), properties) {}

  def withoutClientId: ClientSettings = new ClientSettings(bootstrapServers, None, properties) {}

  def withProperty(name: String, value: String): ValidatedNel[SettingsError, ClientSettings] = withProperties(properties.updated(name, value))

  def withProperties(values: Map[String, String]): ValidatedNel[SettingsError, ClientSettings] =
    ClientSettings.from(bootstrapServers, clientId, values)

  override def toString: String = s"ClientSettings($bootstrapServers,$clientId,${redacted(properties)})"

object ClientSettings:
  def from(
      bootstrapServers: NonEmptyList[String],
      clientId: Option[String] = None,
      properties: Map[String, String] = Map.empty
  ): ValidatedNel[SettingsError, ClientSettings] =
    val bootstrapErrors = bootstrapServers.toList.zipWithIndex.flatMap((server, index) => bootstrapServerError(server, index))
    validateSettings(bootstrapErrors ++ propertyErrors(properties, SettingsError.PropertyScope.Client))
      .map(_ => new ClientSettings(bootstrapServers, clientId, properties) {})

sealed abstract case class ProducerSettings[F[_], K, V] private (
    client: ClientSettings,
    keySerializer: Serializer[F, K],
    valueSerializer: Serializer[F, V],
    properties: Map[String, String]
):
  def mapK[G[_]](fk: FunctionK[F, G]): ProducerSettings[G, K, V] =
    new ProducerSettings(client, keySerializer.mapK(fk), valueSerializer.mapK(fk), properties) {}

  def withClient(value: ClientSettings): ProducerSettings[F, K, V] = new ProducerSettings(value, keySerializer, valueSerializer, properties) {}

  def withProperty(name: String, value: String): ValidatedNel[SettingsError, ProducerSettings[F, K, V]] =
    withProperties(properties.updated(name, value))

  def withProperties(values: Map[String, String]): ValidatedNel[SettingsError, ProducerSettings[F, K, V]] =
    ProducerSettings.from(client, keySerializer, valueSerializer, values)

  override def toString: String = s"ProducerSettings($client,$keySerializer,$valueSerializer,${redacted(properties)})"

object ProducerSettings:
  def from[F[_], K, V](
      client: ClientSettings,
      keySerializer: Serializer[F, K],
      valueSerializer: Serializer[F, V],
      properties: Map[String, String] = Map.empty
  ): ValidatedNel[SettingsError, ProducerSettings[F, K, V]] =
    validateSettings(propertyErrors(properties, SettingsError.PropertyScope.Producer))
      .map(_ => new ProducerSettings(client, keySerializer, valueSerializer, properties) {})

  given [K, V]: FunctorK[[F[_]] =>> ProducerSettings[F, K, V]] with
    override def mapK[F[_], G[_]](settings: ProducerSettings[F, K, V])(fk: FunctionK[F, G]): ProducerSettings[G, K, V] = settings.mapK(fk)

enum AutoOffsetReset:
  case Earliest, Latest

sealed abstract case class ConsumerSettings[F[_], K, V] private (
    client: ClientSettings,
    groupId: ConsumerGroup,
    keyDeserializer: Deserializer[F, K],
    valueDeserializer: Deserializer[F, V],
    autoOffsetReset: AutoOffsetReset,
    properties: Map[String, String]
):
  def mapK[G[_]](fk: FunctionK[F, G]): ConsumerSettings[G, K, V] =
    new ConsumerSettings(client, groupId, keyDeserializer.mapK(fk), valueDeserializer.mapK(fk), autoOffsetReset, properties) {}

  def withClient(value: ClientSettings): ConsumerSettings[F, K, V] =
    new ConsumerSettings(value, groupId, keyDeserializer, valueDeserializer, autoOffsetReset, properties) {}

  def withGroupId(value: ConsumerGroup): ConsumerSettings[F, K, V] =
    new ConsumerSettings(client, value, keyDeserializer, valueDeserializer, autoOffsetReset, properties) {}

  def withAutoOffsetReset(value: AutoOffsetReset): ConsumerSettings[F, K, V] =
    new ConsumerSettings(client, groupId, keyDeserializer, valueDeserializer, value, properties) {}

  def withProperty(name: String, value: String): ValidatedNel[SettingsError, ConsumerSettings[F, K, V]] =
    withProperties(properties.updated(name, value))

  def withProperties(values: Map[String, String]): ValidatedNel[SettingsError, ConsumerSettings[F, K, V]] =
    ConsumerSettings.from(client, groupId, keyDeserializer, valueDeserializer, autoOffsetReset, values)

  override def toString: String = s"ConsumerSettings($client,$groupId,$keyDeserializer,$valueDeserializer,$autoOffsetReset,${redacted(properties)})"

object ConsumerSettings:
  def from[F[_], K, V](
      client: ClientSettings,
      groupId: ConsumerGroup,
      keyDeserializer: Deserializer[F, K],
      valueDeserializer: Deserializer[F, V],
      autoOffsetReset: AutoOffsetReset = AutoOffsetReset.Latest,
      properties: Map[String, String] = Map.empty
  ): ValidatedNel[SettingsError, ConsumerSettings[F, K, V]] =
    validateSettings(propertyErrors(properties, SettingsError.PropertyScope.Consumer))
      .map(_ => new ConsumerSettings(client, groupId, keyDeserializer, valueDeserializer, autoOffsetReset, properties) {})

  given [K, V]: FunctorK[[F[_]] =>> ConsumerSettings[F, K, V]] with
    override def mapK[F[_], G[_]](settings: ConsumerSettings[F, K, V])(fk: FunctionK[F, G]): ConsumerSettings[G, K, V] = settings.mapK(fk)

enum Subscription:
  case Topics(topics: NonEmptyList[Topic])
  case Pattern(pattern: TopicPattern)

trait OffsetCommitter[F[_]]:
  self =>

  def commit(offsets: Map[TopicPartition, Offset]): F[Unit]

  final def mapK[G[_]](fk: FunctionK[F, G]): OffsetCommitter[G] = OffsetCommitter.transformed(self, fk)

object OffsetCommitter:
  given FunctorK[OffsetCommitter] with
    override def mapK[F[_], G[_]](committer: OffsetCommitter[F])(fk: FunctionK[F, G]): OffsetCommitter[G] = committer.mapK(fk)

  private def transformed[F[_], G[_]](committer: OffsetCommitter[F], fk: FunctionK[F, G]): OffsetCommitter[G] =
    TransformedOffsetCommitter(committer, fk)

  private final case class TransformedOffsetCommitter[F[_], G[_]](underlying: OffsetCommitter[F], fk: FunctionK[F, G]) extends OffsetCommitter[G]:
    override def commit(offsets: Map[TopicPartition, Offset]): G[Unit] = fk(underlying.commit(offsets))

trait CommittableOffset[F[_]]:
  self =>

  def topicPartition: TopicPartition

  /** The next offset to consume after this commit succeeds. */
  def nextOffset: Offset

  def committer: OffsetCommitter[F]

  final def commit: F[Unit] = committer.commit(Map(topicPartition -> nextOffset))

  final def mapK[G[_]](fk: FunctionK[F, G]): CommittableOffset[G] =
    new CommittableOffset[G]:
      override def topicPartition: TopicPartition = self.topicPartition

      override def nextOffset: Offset = self.nextOffset

      override def committer: OffsetCommitter[G] = self.committer.mapK(fk)

object CommittableOffset:
  given FunctorK[CommittableOffset] with
    override def mapK[F[_], G[_]](offset: CommittableOffset[F])(fk: FunctionK[F, G]): CommittableOffset[G] = offset.mapK(fk)

opaque type CommittableOffsetBatch[F[_]] = Map[OffsetCommitter[F], Map[TopicPartition, Offset]]

object CommittableOffsetBatch:
  def empty[F[_]]: CommittableOffsetBatch[F] = Map.empty

  def fromFoldable[F[_], G[_]: Foldable](offsets: G[CommittableOffset[F]]): CommittableOffsetBatch[F] =
    Foldable[G].foldLeft(offsets, empty[F])((batch, offset) => batch.updated(offset))

  extension [F[_]](batch: CommittableOffsetBatch[F])
    def updated(offset: CommittableOffset[F]): CommittableOffsetBatch[F] =
      batch.updated(offset.committer, include(batch.getOrElse(offset.committer, Map.empty), offset.topicPartition, offset.nextOffset))

    def updated(other: CommittableOffsetBatch[F]): CommittableOffsetBatch[F] =
      other.foldLeft(batch):
        case (result, (committer, offsets)) => result.updated(
            committer,
            offsets.foldLeft(result.getOrElse(committer, Map.empty)):
              case (committerOffsets, (topicPartition, offset)) => include(committerOffsets, topicPartition, offset)
          )

    def offsets: Map[OffsetCommitter[F], Map[TopicPartition, Offset]] = batch

    def size: Int = batch.valuesIterator.map(_.size).sum

    def commit(using F: Applicative[F]): F[Unit] =
      batch.foldLeft(F.unit):
        case (result, (committer, offsets)) => F.productR(result)(committer.commit(offsets))

    def mapK[G[_]](fk: FunctionK[F, G]): CommittableOffsetBatch[G] = batch.map((committer, offsets) => committer.mapK(fk) -> offsets)

  given FunctorK[CommittableOffsetBatch] with
    override def mapK[F[_], G[_]](batch: CommittableOffsetBatch[F])(fk: FunctionK[F, G]): CommittableOffsetBatch[G] = batch.mapK(fk)

  private def include(offsets: Map[TopicPartition, Offset], topicPartition: TopicPartition, offset: Offset): Map[TopicPartition, Offset] =
    offsets.updatedWith(topicPartition):
      case current @ Some(value) if value.value >= offset.value => current
      case Some(_) | None                                       => Some(offset)

/** Commits non-empty batches every `n` offsets or after `d`, whichever happens first. */
def commitBatchWithin[F[_]: Temporal](n: Int, d: FiniteDuration): Pipe[F, CommittableOffset[F], Unit] =
  _.groupWithin(n, d).evalMap(offsets => CommittableOffsetBatch.fromFoldable(offsets).commit)

final case class CommittableConsumerRecord[F[_], K, V](record: ConsumerRecord[K, V], offset: CommittableOffset[F]):
  def mapK[G[_]](fk: FunctionK[F, G]): CommittableConsumerRecord[G, K, V] = CommittableConsumerRecord(record, offset.mapK(fk))

object CommittableConsumerRecord:
  given [K, V]: FunctorK[[F[_]] =>> CommittableConsumerRecord[F, K, V]] with
    override def mapK[F[_], G[_]](record: CommittableConsumerRecord[F, K, V])(fk: FunctionK[F, G]): CommittableConsumerRecord[G, K, V] =
      record.mapK(fk)

trait KafkaProducer[F[_], K, V]:
  self =>

  def produce(records: NonEmptyList[ProducerRecord[K, V]]): F[ProducerResult[K, V]]

  final def mapK[G[_]](fk: FunctionK[F, G]): KafkaProducer[G, K, V] =
    new KafkaProducer[G, K, V]:
      override def produce(records: NonEmptyList[ProducerRecord[K, V]]): G[ProducerResult[K, V]] = fk(self.produce(records))

object KafkaProducer:
  given [K, V]: FunctorK[[F[_]] =>> KafkaProducer[F, K, V]] with
    override def mapK[F[_], G[_]](producer: KafkaProducer[F, K, V])(fk: FunctionK[F, G]): KafkaProducer[G, K, V] = producer.mapK(fk)

trait KafkaConsumer[F[_], K, V]:
  self =>

  def records: Stream[F, CommittableConsumerRecord[F, K, V]]

  def assignment: F[Set[TopicPartition]]

  /** Polls `assignment` immediately and at the supplied interval, emitting only changes. */
  final def assignmentChanges(pollInterval: FiniteDuration)(using Temporal[F]): Stream[F, Set[TopicPartition]] =
    (Stream.emit(()).covary[F] ++ Stream.awakeEvery[F](pollInterval).map(_ => ())).evalMap(_ => assignment)
      .mapAccumulate(Option.empty[Set[TopicPartition]]):
        case (previous, current) => Some(current) -> Option.when(!previous.contains(current))(current)
      .map(_._2).unNone

  /** Splits `records` into bounded streams whose lifetimes follow the observed partition assignment.
    *
    * Every emitted stream must be consumed concurrently; backpressure from one partition otherwise backpressures the shared record source.
    *
    * @param pollInterval
    *   how often to observe assignment changes
    * @param maxQueuedRecords
    *   positive queue bound for each partition stream
    */
  final def partitionedRecords(pollInterval: FiniteDuration, maxQueuedRecords: Int = 256)(using Async[F]): Stream[F, PartitionRecords[F, K, V]] =
    PartitionRecords.fromConsumer(self, pollInterval, maxQueuedRecords)

  /** Returns the broker-stored next offset for each requested topic-partition, or `None` when no offset has been committed. */
  def committed(topicPartitions: Set[TopicPartition]): F[Map[TopicPartition, Option[Offset]]]

  /** Returns the earliest available offset for each requested topic-partition. */
  def beginningOffsets(topicPartitions: Set[TopicPartition]): F[Map[TopicPartition, Offset]]

  /** Returns the offset immediately after the latest available record for each requested topic-partition. */
  def endOffsets(topicPartitions: Set[TopicPartition]): F[Map[TopicPartition, Offset]]

  /** Returns the earliest offset whose record timestamp is greater than or equal to each requested timestamp, or `None` when none exists. */
  def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Timestamp]): F[Map[TopicPartition, Option[Offset]]]

  /** Returns the partitions currently known for `topic`. */
  def partitionsFor(topic: Topic): F[Set[Partition]]

  /** Returns the topics and partitions visible to this consumer. */
  def listTopics: F[Map[Topic, Set[Partition]]]

  def seek(topicPartition: TopicPartition, offset: Offset): F[Unit]

  final def mapK[G[_]](fk: FunctionK[F, G]): KafkaConsumer[G, K, V] =
    new KafkaConsumer[G, K, V]:
      override val records: Stream[G, CommittableConsumerRecord[G, K, V]] = self.records.map(_.mapK(fk)).translate(fk)

      override def assignment: G[Set[TopicPartition]] = fk(self.assignment)

      override def committed(topicPartitions: Set[TopicPartition]): G[Map[TopicPartition, Option[Offset]]] = fk(self.committed(topicPartitions))

      override def beginningOffsets(topicPartitions: Set[TopicPartition]): G[Map[TopicPartition, Offset]] = fk(self.beginningOffsets(topicPartitions))

      override def endOffsets(topicPartitions: Set[TopicPartition]): G[Map[TopicPartition, Offset]] = fk(self.endOffsets(topicPartitions))

      override def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Timestamp]): G[Map[TopicPartition, Option[Offset]]] =
        fk(self.offsetsForTimes(timestampsToSearch))

      override def partitionsFor(topic: Topic): G[Set[Partition]] = fk(self.partitionsFor(topic))

      override def listTopics: G[Map[Topic, Set[Partition]]] = fk(self.listTopics)

      override def seek(topicPartition: TopicPartition, offset: Offset): G[Unit] = fk(self.seek(topicPartition, offset))

object KafkaConsumer:
  given [K, V]: FunctorK[[F[_]] =>> KafkaConsumer[F, K, V]] with
    override def mapK[F[_], G[_]](consumer: KafkaConsumer[F, K, V])(fk: FunctionK[F, G]): KafkaConsumer[G, K, V] = consumer.mapK(fk)
