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

import scala.concurrent.duration.{FiniteDuration, *}

import cats.{Applicative, FlatMap, Foldable, Functor, Order, Show}
import cats.arrow.FunctionK
import cats.data.{NonEmptyList, NonEmptySet, Validated, ValidatedNel}
import cats.effect.{Async, Concurrent, Temporal}
import cats.syntax.all.*
import cats.tagless.FunctorK
import fs2.{Chunk, Pipe, Stream}

/** Backend property names xkafka derives from the typed settings.
  *
  * Settings construction rejects them, so the portable model stays the single source for these values.
  */
val ManagedProperties: Set[String] =
  Set(
    "bootstrap.servers",
    "client.id",
    "group.id",
    "auto.offset.reset",
    "enable.auto.commit",
    "enable.auto.offset.store",
    "acks",
    "request.required.acks",
    "isolation.level",
    "transactional.id",
    "transaction.timeout.ms",
    "default.api.timeout.ms",
    "metadata.max.age.ms",
    "topic.metadata.refresh.interval.ms",
    "security.protocol",
    "sasl.mechanism",
    "sasl.mechanisms",
    "sasl.username",
    "sasl.password",
    "sasl.jaas.config",
    "ssl.ca.location",
    "ssl.ca.pem",
    "ssl.truststore.location",
    "ssl.truststore.certificates",
    "ssl.truststore.type",
    "ssl.endpoint.identification.algorithm"
  )

enum SettingsError derives CanEqual:
  case BlankBootstrapServer(index: Int)
  case InvalidBootstrapServer(index: Int, value: String)
  case BlankPropertyName(scope: SettingsError.PropertyScope)
  case ManagedProperty(name: String, scope: SettingsError.PropertyScope)
  case BlankCertificateAuthority
  case BlankSaslUsername
  case BlankSaslPassword
  case NonPositiveDuration(field: SettingsError.DurationField, value: FiniteDuration)
  case DurationExceedsMaximum(field: SettingsError.DurationField, value: FiniteDuration)

  def message: String =
    this match
      case BlankBootstrapServer(index)          => s"bootstrapServers[$index] must not be blank"
      case InvalidBootstrapServer(index, value) => s"bootstrapServers[$index] must be host:port, was '$value'"
      case BlankPropertyName(scope)             => s"${scope.label} properties must not contain a blank name"
      case ManagedProperty(name, scope)         => s"${scope.label} property '$name' is managed by xkafka and must be set through typed settings"
      case BlankCertificateAuthority            => "certificate authority must not be blank"
      case BlankSaslUsername                    => "SASL username must not be blank"
      case BlankSaslPassword                    => "SASL password must not be blank"
      case NonPositiveDuration(field, value)    => s"${field.label} must be positive, was $value"
      case DurationExceedsMaximum(field, value) => s"${field.label} must not exceed ${SettingsError.MaxDuration}, was $value"

object SettingsError:
  val MaxDuration: FiniteDuration = Int.MaxValue.toLong.millis

  enum DurationField(val label: String) derives CanEqual:
    case MetadataRefreshInterval extends DurationField("metadataRefreshInterval")
    case CloseTimeout            extends DurationField("closeTimeout")
    case TransactionTimeout      extends DurationField("transactionTimeout")
    case CommitTimeout           extends DurationField("commitTimeout")
    case PollTimeout             extends DurationField("pollTimeout")
    case RequestTimeout          extends DurationField("requestTimeout")

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

private def durationErrors(field: SettingsError.DurationField, value: FiniteDuration): List[SettingsError] =
  if value <= Duration.Zero then List(SettingsError.NonPositiveDuration(field, value))
  else if value > SettingsError.MaxDuration then List(SettingsError.DurationExceedsMaximum(field, value))
  else Nil

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

sealed abstract case class ClientSettings private (
    bootstrapServers: NonEmptyList[String],
    clientId: Option[String],
    properties: Map[String, String],
    security: SecuritySettings,
    metadataRefreshInterval: FiniteDuration
):
  def withBootstrapServers(values: NonEmptyList[String]): ValidatedNel[SettingsError, ClientSettings] =
    ClientSettings.from(values, clientId, properties, security, metadataRefreshInterval)

  def withClientId(value: String): ClientSettings =
    new ClientSettings(bootstrapServers, Some(value), properties, security, metadataRefreshInterval) {}

  def withoutClientId: ClientSettings = new ClientSettings(bootstrapServers, None, properties, security, metadataRefreshInterval) {}

  def withSecurity(value: SecuritySettings): ClientSettings =
    new ClientSettings(bootstrapServers, clientId, properties, value, metadataRefreshInterval) {}

  /** How long a topic created after a client started can stay unseen. */
  def withMetadataRefreshInterval(value: FiniteDuration): ValidatedNel[SettingsError, ClientSettings] =
    ClientSettings.from(bootstrapServers, clientId, properties, security, value)

  def withProperty(name: String, value: String): ValidatedNel[SettingsError, ClientSettings] = withProperties(properties.updated(name, value))

  def withProperties(values: Map[String, String]): ValidatedNel[SettingsError, ClientSettings] =
    ClientSettings.from(bootstrapServers, clientId, values, security, metadataRefreshInterval)

  override def toString: String = s"ClientSettings($bootstrapServers,$clientId,${redacted(properties)},$security,$metadataRefreshInterval)"

object ClientSettings:
  /** Both backends refresh metadata every five minutes by default. */
  val DefaultMetadataRefreshInterval: FiniteDuration = 5.minutes

  def from(
      bootstrapServers: NonEmptyList[String],
      clientId: Option[String] = None,
      properties: Map[String, String] = Map.empty,
      security: SecuritySettings = SecuritySettings.Plaintext,
      metadataRefreshInterval: FiniteDuration = ClientSettings.DefaultMetadataRefreshInterval
  ): ValidatedNel[SettingsError, ClientSettings] =
    val bootstrapErrors = bootstrapServers.toList.zipWithIndex.flatMap((server, index) => bootstrapServerError(server, index))
    val errors          =
      bootstrapErrors ++ propertyErrors(properties, SettingsError.PropertyScope.Client) ++
        durationErrors(SettingsError.DurationField.MetadataRefreshInterval, metadataRefreshInterval)
    validateSettings(errors).map(_ => new ClientSettings(bootstrapServers, clientId, properties, security, metadataRefreshInterval) {})

sealed abstract case class ProducerSettings[F[_], K, V] private (
    client: ClientSettings,
    keySerializer: Serializer[F, K],
    valueSerializer: Serializer[F, V],
    properties: Map[String, String],
    acks: Acks,
    closeTimeout: FiniteDuration
):
  def mapK[G[_]](fk: FunctionK[F, G]): ProducerSettings[G, K, V] =
    new ProducerSettings(client, keySerializer.mapK(fk), valueSerializer.mapK(fk), properties, acks, closeTimeout) {}

  def withClient(value: ClientSettings): ProducerSettings[F, K, V] =
    new ProducerSettings(value, keySerializer, valueSerializer, properties, acks, closeTimeout) {}

  /** How many replicas must have a record before the broker answers for it. */
  def withAcks(value: Acks): ProducerSettings[F, K, V] =
    new ProducerSettings(client, keySerializer, valueSerializer, properties, value, closeTimeout) {}

  /** How long releasing a producer waits to deliver the records it has already accepted, before dropping whatever is left. */
  def withCloseTimeout(value: FiniteDuration): ValidatedNel[SettingsError, ProducerSettings[F, K, V]] =
    ProducerSettings.from(client, keySerializer, valueSerializer, properties, acks, value)

  def withProperty(name: String, value: String): ValidatedNel[SettingsError, ProducerSettings[F, K, V]] =
    withProperties(properties.updated(name, value))

  def withProperties(values: Map[String, String]): ValidatedNel[SettingsError, ProducerSettings[F, K, V]] =
    ProducerSettings.from(client, keySerializer, valueSerializer, values, acks, closeTimeout)

  override def toString: String = s"ProducerSettings($client,$keySerializer,$valueSerializer,${redacted(properties)},$acks,$closeTimeout)"

object ProducerSettings:
  /** What both backends already ask for, and the only setting that survives losing a leader. */
  val DefaultAcks: Acks = Acks.AllReplicas

  /** Long enough that a producer with records still in flight delivers them rather than dropping them on the way out. */
  val DefaultCloseTimeout: FiniteDuration = 60.seconds

  def from[F[_], K, V](
      client: ClientSettings,
      keySerializer: Serializer[F, K],
      valueSerializer: Serializer[F, V],
      properties: Map[String, String] = Map.empty,
      acks: Acks = ProducerSettings.DefaultAcks,
      closeTimeout: FiniteDuration = ProducerSettings.DefaultCloseTimeout
  ): ValidatedNel[SettingsError, ProducerSettings[F, K, V]] =
    val errors =
      propertyErrors(properties, SettingsError.PropertyScope.Producer) ++ durationErrors(SettingsError.DurationField.CloseTimeout, closeTimeout)
    validateSettings(errors).map(_ => new ProducerSettings(client, keySerializer, valueSerializer, properties, acks, closeTimeout) {})

  given [K, V]: FunctorK[[F[_]] =>> ProducerSettings[F, K, V]] with
    override def mapK[F[_], G[_]](settings: ProducerSettings[F, K, V])(fk: FunctionK[F, G]): ProducerSettings[G, K, V] = settings.mapK(fk)

/** A producer that writes inside transactions, which is all it writes, so these settings build a producer of their own. */
sealed abstract case class TransactionalProducerSettings[F[_], K, V] private (
    producer: ProducerSettings[F, K, V],
    transactionalId: TransactionalId,
    transactionTimeout: FiniteDuration
):
  def mapK[G[_]](fk: FunctionK[F, G]): TransactionalProducerSettings[G, K, V] =
    new TransactionalProducerSettings(producer.mapK(fk), transactionalId, transactionTimeout) {}

  def withClient(value: ClientSettings): TransactionalProducerSettings[F, K, V] =
    new TransactionalProducerSettings(producer.withClient(value), transactionalId, transactionTimeout) {}

  /** Kafka's `transaction.timeout.ms`. */
  def withTransactionTimeout(value: FiniteDuration): ValidatedNel[SettingsError, TransactionalProducerSettings[F, K, V]] =
    validateSettings(durationErrors(SettingsError.DurationField.TransactionTimeout, value))
      .map(_ => new TransactionalProducerSettings(producer, transactionalId, value) {})

  def withProperty(name: String, value: String): ValidatedNel[SettingsError, TransactionalProducerSettings[F, K, V]] =
    withProperties(producer.properties.updated(name, value))

  def withProperties(values: Map[String, String]): ValidatedNel[SettingsError, TransactionalProducerSettings[F, K, V]] =
    producer.withProperties(values).map(new TransactionalProducerSettings(_, transactionalId, transactionTimeout) {})

object TransactionalProducerSettings:
  /** What Kafka's own producers use for `transaction.timeout.ms`. */
  val DefaultTransactionTimeout: FiniteDuration = 60.seconds

  def from[F[_], K, V](
      client: ClientSettings,
      transactionalId: TransactionalId,
      keySerializer: Serializer[F, K],
      valueSerializer: Serializer[F, V],
      transactionTimeout: FiniteDuration = TransactionalProducerSettings.DefaultTransactionTimeout,
      properties: Map[String, String] = Map.empty
  ): ValidatedNel[SettingsError, TransactionalProducerSettings[F, K, V]] =
    (
      ProducerSettings.from(client, keySerializer, valueSerializer, properties),
      validateSettings(durationErrors(SettingsError.DurationField.TransactionTimeout, transactionTimeout))
    ).mapN((producer, _) => new TransactionalProducerSettings(producer, transactionalId, transactionTimeout) {})

  given [K, V]: FunctorK[[F[_]] =>> TransactionalProducerSettings[F, K, V]] with
    override def mapK[F[_], G[_]](settings: TransactionalProducerSettings[F, K, V])(fk: FunctionK[F, G]): TransactionalProducerSettings[G, K, V] =
      settings.mapK(fk)

/** How many replicas must have a record before the broker answers for it. */
enum Acks:
  /** What both backends read this as, which is the same string on each of them. */
  private[xkafka] def property: String =
    this match
      case Acks.NoAcknowledgement => "0"
      case Acks.Leader            => "1"
      case Acks.AllReplicas       => "all"

  /** The broker does not answer at all, so a record can be lost with nothing saying so. */
  case NoAcknowledgement

  /** The partition leader alone, so a record is lost where the leader fails before a follower has taken it. */
  case Leader

  /** Every in-sync replica, which is the only one of the three that survives losing the leader. */
  case AllReplicas

enum AutoOffsetReset:
  case Earliest, Latest

/** Whether a consumer reads records written by transactions that have not committed. */
enum IsolationLevel:
  /** Every record is delivered as soon as it is written, including records of a transaction that later aborts. */
  case ReadUncommitted

  /** A record written by a transaction is delivered once that transaction commits, and never where it aborts. */
  case ReadCommitted

sealed abstract case class ConsumerSettings[F[_], K, V] private (
    client: ClientSettings,
    groupId: ConsumerGroup,
    keyDeserializer: Deserializer[F, K],
    valueDeserializer: Deserializer[F, V],
    autoOffsetReset: AutoOffsetReset,
    isolationLevel: IsolationLevel,
    commitRecovery: CommitRecovery,
    commitTimeout: FiniteDuration,
    pollTimeout: FiniteDuration,
    requestTimeout: FiniteDuration,
    properties: Map[String, String]
):
  def mapK[G[_]](fk: FunctionK[F, G]): ConsumerSettings[G, K, V] =
    new ConsumerSettings(
      client,
      groupId,
      keyDeserializer.mapK(fk),
      valueDeserializer.mapK(fk),
      autoOffsetReset,
      isolationLevel,
      commitRecovery,
      commitTimeout,
      pollTimeout,
      requestTimeout,
      properties
    ) {}

  def withClient(value: ClientSettings): ConsumerSettings[F, K, V] =
    new ConsumerSettings(
      value,
      groupId,
      keyDeserializer,
      valueDeserializer,
      autoOffsetReset,
      isolationLevel,
      commitRecovery,
      commitTimeout,
      pollTimeout,
      requestTimeout,
      properties
    ) {}

  def withGroupId(value: ConsumerGroup): ConsumerSettings[F, K, V] =
    new ConsumerSettings(
      client,
      value,
      keyDeserializer,
      valueDeserializer,
      autoOffsetReset,
      isolationLevel,
      commitRecovery,
      commitTimeout,
      pollTimeout,
      requestTimeout,
      properties
    ) {}

  def withAutoOffsetReset(value: AutoOffsetReset): ConsumerSettings[F, K, V] =
    new ConsumerSettings(
      client,
      groupId,
      keyDeserializer,
      valueDeserializer,
      value,
      isolationLevel,
      commitRecovery,
      commitTimeout,
      pollTimeout,
      requestTimeout,
      properties
    ) {}

  /** Whether records of a transaction that has not committed are delivered. */
  def withIsolationLevel(value: IsolationLevel): ConsumerSettings[F, K, V] =
    new ConsumerSettings(
      client,
      groupId,
      keyDeserializer,
      valueDeserializer,
      autoOffsetReset,
      value,
      commitRecovery,
      commitTimeout,
      pollTimeout,
      requestTimeout,
      properties
    ) {}

  /** How long a commit waits for the broker before it fails as `ErrorCode.RequestTimedOut`, which the recovery policy retries. */
  def withCommitTimeout(value: FiniteDuration): ValidatedNel[SettingsError, ConsumerSettings[F, K, V]] =
    ConsumerSettings.from(
      client,
      groupId,
      keyDeserializer,
      valueDeserializer,
      autoOffsetReset,
      isolationLevel,
      commitRecovery,
      value,
      pollTimeout,
      requestTimeout,
      properties
    )

  /** How a failed offset commit is retried. */
  def withCommitRecovery(value: CommitRecovery): ConsumerSettings[F, K, V] =
    new ConsumerSettings(
      client,
      groupId,
      keyDeserializer,
      valueDeserializer,
      autoOffsetReset,
      isolationLevel,
      value,
      commitTimeout,
      pollTimeout,
      requestTimeout,
      properties
    ) {}

  /** How long one poll waits for records before it returns empty.
    *
    * It also bounds how long another call on the same consumer can queue behind a poll already in flight.
    */
  def withPollTimeout(value: FiniteDuration): ValidatedNel[SettingsError, ConsumerSettings[F, K, V]] =
    ConsumerSettings.from(
      client,
      groupId,
      keyDeserializer,
      valueDeserializer,
      autoOffsetReset,
      isolationLevel,
      commitRecovery,
      commitTimeout,
      value,
      requestTimeout,
      properties
    )

  /** How long a call that asks the broker something waits for its answer.
    *
    * It bounds `committed`, `beginningOffsets`, `endOffsets`, `offsetsForTimes`, `partitionsFor`, `listTopics`, and `seek`.
    */
  def withRequestTimeout(value: FiniteDuration): ValidatedNel[SettingsError, ConsumerSettings[F, K, V]] =
    ConsumerSettings.from(
      client,
      groupId,
      keyDeserializer,
      valueDeserializer,
      autoOffsetReset,
      isolationLevel,
      commitRecovery,
      commitTimeout,
      pollTimeout,
      value,
      properties
    )

  def withProperty(name: String, value: String): ValidatedNel[SettingsError, ConsumerSettings[F, K, V]] =
    withProperties(properties.updated(name, value))

  def withProperties(values: Map[String, String]): ValidatedNel[SettingsError, ConsumerSettings[F, K, V]] =
    ConsumerSettings.from(
      client,
      groupId,
      keyDeserializer,
      valueDeserializer,
      autoOffsetReset,
      isolationLevel,
      commitRecovery,
      commitTimeout,
      pollTimeout,
      requestTimeout,
      values
    )

  override def toString: String =
    s"ConsumerSettings($client,$groupId,$keyDeserializer,$valueDeserializer,$autoOffsetReset,$isolationLevel,$commitRecovery,$commitTimeout,$pollTimeout,$requestTimeout,${redacted(
        properties
      )})"

object ConsumerSettings:
  /** Short enough to keep other calls on the consumer responsive, long enough that an idle poll is not a spin. */
  val DefaultPollTimeout: FiniteDuration = 100.millis

  /** Long enough that a commit is not abandoned while the coordinator is merely busy. */
  val DefaultCommitTimeout: FiniteDuration = 15.seconds

  /** What Kafka's own clients wait for the same answers, through `default.api.timeout.ms`. */
  val DefaultRequestTimeout: FiniteDuration = 60.seconds

  def from[F[_], K, V](
      client: ClientSettings,
      groupId: ConsumerGroup,
      keyDeserializer: Deserializer[F, K],
      valueDeserializer: Deserializer[F, V],
      autoOffsetReset: AutoOffsetReset = AutoOffsetReset.Latest,
      isolationLevel: IsolationLevel = IsolationLevel.ReadUncommitted,
      commitRecovery: CommitRecovery = CommitRecovery.Default,
      commitTimeout: FiniteDuration = ConsumerSettings.DefaultCommitTimeout,
      pollTimeout: FiniteDuration = ConsumerSettings.DefaultPollTimeout,
      requestTimeout: FiniteDuration = ConsumerSettings.DefaultRequestTimeout,
      properties: Map[String, String] = Map.empty
  ): ValidatedNel[SettingsError, ConsumerSettings[F, K, V]] =
    val errors =
      propertyErrors(properties, SettingsError.PropertyScope.Consumer) ++ durationErrors(SettingsError.DurationField.CommitTimeout, commitTimeout) ++
        durationErrors(SettingsError.DurationField.PollTimeout, pollTimeout) ++
        durationErrors(SettingsError.DurationField.RequestTimeout, requestTimeout)
    validateSettings(errors).map(_ =>
      new ConsumerSettings(
        client,
        groupId,
        keyDeserializer,
        valueDeserializer,
        autoOffsetReset,
        isolationLevel,
        commitRecovery,
        commitTimeout,
        pollTimeout,
        requestTimeout,
        properties
      ) {}
    )

  given [K, V]: FunctorK[[F[_]] =>> ConsumerSettings[F, K, V]] with
    override def mapK[F[_], G[_]](settings: ConsumerSettings[F, K, V])(fk: FunctionK[F, G]): ConsumerSettings[G, K, V] = settings.mapK(fk)

/** What a consumer reads. */
sealed trait Selection

object Selection:
  /** Joins a consumer group, so the partitions a consumer holds follow the group's rebalances. */
  enum Subscription extends Selection:
    case Topics(topics: NonEmptySet[Topic])
    case Pattern(pattern: TopicPattern)

  export Subscription.{Pattern, Topics}

  /** Names the partitions to read and joins no group, so the assignment never changes and nothing rebalances it away. */
  final case class Partitions(topicPartitions: NonEmptySet[TopicPartition]) extends Selection

/** A topic to create, with the partition count and replication factor the broker will give it. */
sealed abstract case class NewTopic private (topic: Topic, partitions: Int, replicationFactor: Short, configuration: Map[String, String]):
  def withConfiguration(values: Map[String, String]): NewTopic = new NewTopic(topic, partitions, replicationFactor, values) {}

object NewTopic:
  def from(
      topic: Topic,
      partitions: Int,
      replicationFactor: Short,
      configuration: Map[String, String] = Map.empty
  ): Either[ValidationError, NewTopic] =
    if partitions <= 0 then Left(ValidationError.NonPositivePartitionCount(partitions))
    else if replicationFactor <= 0 then Left(ValidationError.NonPositiveReplicationFactor(replicationFactor))
    else Right(new NewTopic(topic, partitions, replicationFactor, configuration) {})

  /** A topic can only be created once, so its name is what distinguishes one of these from another. */
  given Order[NewTopic] = Order.by(_.topic)
  given Show[NewTopic]  = Show.show(value => s"${value.topic.value}(partitions=${value.partitions}, replication=${value.replicationFactor})")

/** Topic administration.
  *
  * Kafka reports an outcome for each topic, and the backends do not agree on whether that detail survives, so these report the first failure and
  * nothing more.
  */
trait KafkaAdminClient[F[_]]:
  self =>

  /** Creates each topic, failing if any of them cannot be created, including where it already exists. */
  def createTopics(topics: NonEmptySet[NewTopic]): F[Unit]

  /** Deletes each topic, failing if any of them cannot be deleted, including where it does not exist. */
  def deleteTopics(topics: NonEmptySet[Topic]): F[Unit]

  /** Adds partitions to a topic, which Kafka allows only as an increase. */
  def createPartitions(topic: Topic, count: Int): F[Unit]

  /** Reports the partitions of each requested topic, failing where the cluster does not have one of them. */
  def describeTopics(topics: NonEmptySet[Topic]): F[Map[Topic, Set[Partition]]]

  final def mapK[G[_]](fk: FunctionK[F, G]): KafkaAdminClient[G] =
    new KafkaAdminClient[G]:
      override def createTopics(topics: NonEmptySet[NewTopic]): G[Unit] = fk(self.createTopics(topics))

      override def deleteTopics(topics: NonEmptySet[Topic]): G[Unit] = fk(self.deleteTopics(topics))

      override def createPartitions(topic: Topic, count: Int): G[Unit] = fk(self.createPartitions(topic, count))

      override def describeTopics(topics: NonEmptySet[Topic]): G[Map[Topic, Set[Partition]]] = fk(self.describeTopics(topics))

object KafkaAdminClient:
  private[xkafka] def validatePartitionCount(value: Int): Either[KafkaException.InvalidValue, Int] =
    if value > 0 then Right(value) else Left(new KafkaException.InvalidValue(ValidationError.NonPositivePartitionCount(value)))

  given FunctorK[KafkaAdminClient] with
    override def mapK[F[_], G[_]](client: KafkaAdminClient[F])(fk: FunctionK[F, G]): KafkaAdminClient[G] = client.mapK(fk)

trait OffsetCommitter[F[_]]:
  self =>

  def commit(offsets: Map[TopicPartition, Offset]): F[Unit]

  /** How a transaction names the consumer group these offsets belong to.
    *
    * A committer that came from a consumer carries what its own backend needs, so a transaction records offsets against the group that read them
    * without being handed the consumer again.
    */
  private[xkafka] def membership: GroupMembership[F] = GroupMembership.Absent()

  final def mapK[G[_]](fk: FunctionK[F, G]): OffsetCommitter[G] = OffsetCommitter.transformed(self, fk)

object OffsetCommitter:
  given FunctorK[OffsetCommitter] with
    override def mapK[F[_], G[_]](committer: OffsetCommitter[F])(fk: FunctionK[F, G]): OffsetCommitter[G] = committer.mapK(fk)

  private def transformed[F[_], G[_]](committer: OffsetCommitter[F], fk: FunctionK[F, G]): OffsetCommitter[G] =
    TransformedOffsetCommitter(committer, fk)

  private final case class TransformedOffsetCommitter[F[_], G[_]](underlying: OffsetCommitter[F], fk: FunctionK[F, G]) extends OffsetCommitter[G]:
    override def commit(offsets: Map[TopicPartition, Offset]): G[Unit] = fk(underlying.commit(offsets))

    override private[xkafka] def membership: GroupMembership[G] = underlying.membership.mapK(fk)

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

  /** Enqueues `records` with the backend and returns the effect that completes once the broker has acknowledged them.
    *
    * The outer effect completes as soon as the records are accepted for delivery, so several batches can be in flight at once. Awaiting the inner
    * effect immediately makes production fully synchronous.
    */
  def produce(records: NonEmptyList[ProducerRecord[K, V]]): F[F[ProducerResult[K, V]]]

  /** Enqueues `records` and waits for the broker to acknowledge them. */
  final def produceAndAwait(records: NonEmptyList[ProducerRecord[K, V]])(using F: FlatMap[F]): F[ProducerResult[K, V]] = F.flatten(produce(records))

  /** Returns the partitions currently known for `topic`, which is what choosing one to produce to needs. */
  def partitionsFor(topic: Topic): F[Set[Partition]]

  /** Produces each batch and reports its acknowledgement, in the order the batches arrived.
    *
    * Enqueueing carries on while earlier batches are still being acknowledged, which is what the two stages of `produce` are for, and is why this is
    * not the same as mapping `produceAndAwait` over the stream. At most `maxInFlight` batches wait for the broker at once.
    *
    * @param maxInFlight
    *   positive bound on the batches awaiting acknowledgement
    */
  final def pipe(maxInFlight: Int = 256)(using Concurrent[F]): Pipe[F, NonEmptyList[ProducerRecord[K, V]], ProducerResult[K, V]] =
    if maxInFlight <= 0 then _ => Stream.raiseError(new IllegalArgumentException("maxInFlight must be positive"))
    else _.evalMap(produce).parEvalMap(maxInFlight)(identity)

  /** Transforms the effect. Translating the acknowledgement nested inside the enqueue needs a `Functor[G]`, so this is not a lawful `FunctorK`. */
  final def mapK[G[_]](fk: FunctionK[F, G])(using G: Functor[G]): KafkaProducer[G, K, V] =
    new KafkaProducer[G, K, V]:
      override def produce(records: NonEmptyList[ProducerRecord[K, V]]): G[G[ProducerResult[K, V]]] = G.map(fk(self.produce(records)))(fk.apply)

      override def partitionsFor(topic: Topic): G[Set[Partition]] = fk(self.partitionsFor(topic))

/** Produces records and records consumer offsets as one unit.
  *
  * Everything written through a transaction becomes visible together once it commits. A consumer reading `IsolationLevel.ReadCommitted` sees none of
  * it before then, and none of it at all where the transaction aborts.
  */
trait Transaction[F[_], K, V]:
  self =>

  /** Enqueues `records` and waits for the broker to acknowledge them, which a transaction must do before it can commit. */
  def produce(records: NonEmptyList[ProducerRecord[K, V]]): F[ProducerResult[K, V]]

  /** Records `batch` against the group that read it, so those offsets land only where this transaction commits.
    *
    * The offsets carry the committer they came from, and that committer knows its consumer, so nothing here can name the wrong group.
    */
  def commitOffsets(batch: CommittableOffsetBatch[F]): F[Unit]

  /** Transforms the effect. Carrying a batch back to the underlying transaction needs the reverse direction, so this is not a lawful `FunctorK`. */
  final def imapK[G[_]](fk: FunctionK[F, G])(gk: FunctionK[G, F]): Transaction[G, K, V] =
    new Transaction[G, K, V]:
      override def produce(records: NonEmptyList[ProducerRecord[K, V]]): G[ProducerResult[K, V]] = fk(self.produce(records))

      override def commitOffsets(batch: CommittableOffsetBatch[G]): G[Unit] = fk(self.commitOffsets(batch.mapK(gk)))

trait KafkaTransactionalProducer[F[_], K, V]:
  self =>

  /** Runs `use` inside a transaction, committing it where `use` succeeds and aborting it where `use` fails or is cancelled.
    *
    * A producer carries one transactional id, so its transactions run one after another.
    */
  def transactionally[A](use: Transaction[F, K, V] => F[A]): F[A]

  /** Transforms the effect. Running the caller's function in the underlying effect needs the reverse direction, so this is not a lawful `FunctorK`.
    */
  final def imapK[G[_]](fk: FunctionK[F, G])(gk: FunctionK[G, F]): KafkaTransactionalProducer[G, K, V] =
    new KafkaTransactionalProducer[G, K, V]:
      override def transactionally[A](use: Transaction[G, K, V] => G[A]): G[A] =
        fk(self.transactionally(transaction => gk(use(transaction.imapK(fk)(gk)))))

/** Says a processed chunk can have its offsets committed, so the commit that follows is visible where the records are handled. */
case object CommitNow
type CommitNow = CommitNow.type

trait KafkaConsumer[F[_], K, V]:
  self =>

  def records: Stream[F, CommittableConsumerRecord[F, K, V]]

  def assignment: F[Set[TopicPartition]]

  /** How this backend stops and restarts delivery for individual partitions.
    *
    * A backend whose records all arrive from one source overrides this, so that a partition nobody is reading stops being fetched and the partitions
    * beside it keep flowing. One that feeds each partition on its own has nothing to hold back and keeps the default.
    */
  private[xkafka] def pausing: PartitionPausing[F] = PartitionPausing.Absent()

  /** Emits the current assignment and then each distinct one afterwards.
    *
    * A consumer reading `Selection.Partitions` joins no group, so its assignment is fixed and only the first value arrives.
    */
  def assignmentChanges: Stream[F, Set[TopicPartition]]

  /** Stops fetching, so the record streams end once the records already fetched have been handed over.
    *
    * This is how a consumer stops gracefully: nothing already fetched is dropped, and offsets stay committable afterwards. It returns as soon as
    * fetching has been told to stop, without waiting for the streams to drain. A stream started afterwards is empty, and calling it again does
    * nothing further.
    *
    * Releasing the consumer, or cancelling whatever reads it, stops it abruptly instead, and records that were fetched but not handed over are lost.
    */
  def stopConsuming: F[Unit]

  /** Hands every record to `process` a chunk at a time and commits each chunk once it returns.
    *
    * Partitions are processed alongside one another, so a slow chunk holds back only the partition it came from. It returns once `stopConsuming` has
    * been called and everything already fetched has been processed, and otherwise runs until it is cancelled or something fails.
    *
    * @param maxQueuedRecords
    *   positive per-partition threshold at which a shared-source backend pauses fetching; records already in flight can temporarily exceed it
    */
  final def consumeChunk(process: Chunk[ConsumerRecord[K, V]] => F[CommitNow], maxQueuedRecords: Int = 256)(using F: Async[F]): F[Unit] =
    partitionedRecords(maxQueuedRecords).map(
      _.records.chunks.evalMap: chunk =>
        val (offsets, records) =
          chunk.mapAccumulate(CommittableOffsetBatch.empty[F])((batch, committable) => (batch.updated(committable.offset), committable.record))
        F.productR(process(records))(offsets.commit)
    ).parJoinUnbounded.compile.drain

  /** Splits `records` into bounded streams whose lifetimes follow the observed partition assignment.
    *
    * Every emitted stream must be consumed concurrently; backpressure from one partition otherwise backpressures the shared record source.
    *
    * @param maxQueuedRecords
    *   positive per-partition threshold at which a shared-source backend pauses fetching; records already in flight can temporarily exceed it
    */
  def partitionedRecords(maxQueuedRecords: Int = 256)(using Async[F]): Stream[F, PartitionRecords[F, K, V]] =
    PartitionRecords.fromConsumer(self, assignmentChanges, maxQueuedRecords, pausing)

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

  /** Moves the next fetch for each requested topic-partition to the earliest record the broker still holds. */
  def seekToBeginning(topicPartitions: Set[TopicPartition]): F[Unit]

  /** Moves the next fetch for each requested topic-partition past the latest record the broker holds. */
  def seekToEnd(topicPartitions: Set[TopicPartition]): F[Unit]

  /** Returns the offset this consumer reads next, or `None` where it has not consumed from the partition yet.
    *
    * A backend may settle on a position earlier than that, such as when a seek names an offset, so the value every backend agrees on is the one after
    * records have been consumed.
    */
  def position(topicPartition: TopicPartition): F[Option[Offset]]

  final def mapK[G[_]](fk: FunctionK[F, G]): KafkaConsumer[G, K, V] =
    new KafkaConsumer[G, K, V]:
      override val records: Stream[G, CommittableConsumerRecord[G, K, V]] = self.records.map(_.mapK(fk)).translate(fk)

      override def assignment: G[Set[TopicPartition]] = fk(self.assignment)

      override val assignmentChanges: Stream[G, Set[TopicPartition]] = self.assignmentChanges.translate(fk)

      override def committed(topicPartitions: Set[TopicPartition]): G[Map[TopicPartition, Option[Offset]]] = fk(self.committed(topicPartitions))

      override def beginningOffsets(topicPartitions: Set[TopicPartition]): G[Map[TopicPartition, Offset]] = fk(self.beginningOffsets(topicPartitions))

      override def endOffsets(topicPartitions: Set[TopicPartition]): G[Map[TopicPartition, Offset]] = fk(self.endOffsets(topicPartitions))

      override def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Timestamp]): G[Map[TopicPartition, Option[Offset]]] =
        fk(self.offsetsForTimes(timestampsToSearch))

      override def partitionsFor(topic: Topic): G[Set[Partition]] = fk(self.partitionsFor(topic))

      override def listTopics: G[Map[Topic, Set[Partition]]] = fk(self.listTopics)

      override def seek(topicPartition: TopicPartition, offset: Offset): G[Unit] = fk(self.seek(topicPartition, offset))

      override def seekToBeginning(topicPartitions: Set[TopicPartition]): G[Unit] = fk(self.seekToBeginning(topicPartitions))

      override def seekToEnd(topicPartitions: Set[TopicPartition]): G[Unit] = fk(self.seekToEnd(topicPartitions))

      override def position(topicPartition: TopicPartition): G[Option[Offset]] = fk(self.position(topicPartition))

      override def stopConsuming: G[Unit] = fk(self.stopConsuming)

      override private[xkafka] def pausing: PartitionPausing[G] = self.pausing.mapK(fk)

object KafkaConsumer:
  given [K, V]: FunctorK[[F[_]] =>> KafkaConsumer[F, K, V]] with
    override def mapK[F[_], G[_]](consumer: KafkaConsumer[F, K, V])(fk: FunctionK[F, G]): KafkaConsumer[G, K, V] = consumer.mapK(fk)
