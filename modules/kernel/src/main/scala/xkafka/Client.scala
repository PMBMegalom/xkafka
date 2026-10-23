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
import cats.data.{NonEmptyList, NonEmptyMap, NonEmptySet, Validated, ValidatedNel}
import cats.effect.{Async, Concurrent, Temporal}
import cats.syntax.all.*
import cats.tagless.FunctorK
import fs2.{Chunk, Pipe, Stream}

/** Kafka property names xkafka sets from the typed settings.
  *
  * Passing one through `properties` or `withProperty` fails as `SettingsError.ManagedProperty`; use the typed setting instead.
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
    "enable.idempotence",
    "isolation.level",
    "transactional.id",
    "transaction.timeout.ms",
    "compression.type",
    "compression.codec",
    "linger.ms",
    "group.instance.id",
    "session.timeout.ms",
    "heartbeat.interval.ms",
    "max.poll.interval.ms",
    "batch.size",
    "queue.buffering.max.ms",
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
    "ssl.endpoint.identification.algorithm",
    "ssl.keystore.type",
    "ssl.keystore.location",
    "ssl.keystore.password",
    "ssl.keystore.key",
    "ssl.keystore.certificate.chain",
    "ssl.key.password",
    "ssl.key.location",
    "ssl.key.pem",
    "ssl.certificate.location",
    "ssl.certificate.pem"
  )

enum SettingsError derives CanEqual:
  case BlankBootstrapServer(index: Int)
  case InvalidBootstrapServer(index: Int, value: String)
  case BlankPropertyName(scope: SettingsError.PropertyScope)
  case ManagedProperty(name: String, scope: SettingsError.PropertyScope)
  case BlankCertificateAuthority
  case BlankClientCertificate
  case PrivateKeyNotPkcs8
  case BlankKeyPassword
  case BlankSaslUsername
  case BlankSaslPassword
  case NonPositiveDuration(field: SettingsError.DurationField, value: FiniteDuration)
  case DurationExceedsMaximum(field: SettingsError.DurationField, value: FiniteDuration)
  case IdempotenceRequiresAllReplicas(acks: Acks)
  case NegativeDuration(field: SettingsError.DurationField, value: FiniteDuration)
  case DurationAboveLimit(field: SettingsError.DurationField, value: FiniteDuration, limit: FiniteDuration)
  case NonPositiveBatchSize(value: Int)
  case BlankGroupInstanceId
  case HeartbeatNotBelowSessionTimeout(heartbeatInterval: FiniteDuration, sessionTimeout: FiniteDuration)
  case MaxPollIntervalBelowSessionTimeout(maxPollInterval: FiniteDuration, sessionTimeout: FiniteDuration)
  case RebalanceSafeCommitLimitNotBelowMaxPollInterval(limit: FiniteDuration, maxPollInterval: FiniteDuration)
  case StartingOffsetsLimitNotBelowMaxPollInterval(limit: FiniteDuration, maxPollInterval: FiniteDuration)

  def message: String =
    this match
      case BlankBootstrapServer(index)             => s"bootstrapServers[$index] must not be blank"
      case InvalidBootstrapServer(index, value)    => s"bootstrapServers[$index] must be host:port, was '$value'"
      case BlankPropertyName(scope)                => s"${scope.label} properties must not contain a blank name"
      case ManagedProperty(name, scope)            => s"${scope.label} property '$name' is managed by xkafka and must be set through typed settings"
      case BlankCertificateAuthority               => "certificate authority must not be blank"
      case BlankClientCertificate                  => "client certificate must not be blank"
      case PrivateKeyNotPkcs8                      => "client private key must be PKCS#8, a PEM PRIVATE KEY or ENCRYPTED PRIVATE KEY"
      case BlankKeyPassword                        => "client key password must not be empty; leave it out for a key that has none"
      case BlankSaslUsername                       => "SASL username must not be blank"
      case BlankSaslPassword                       => "SASL password must not be blank"
      case NonPositiveDuration(field, value)       => s"${field.label} must be positive, was $value"
      case DurationExceedsMaximum(field, value)    => s"${field.label} must not exceed ${SettingsError.MaxDuration}, was $value"
      case IdempotenceRequiresAllReplicas(acks)    => s"acks must be AllReplicas while idempotence is on, was $acks"
      case NegativeDuration(field, value)          => s"${field.label} must not be negative, was $value"
      case DurationAboveLimit(field, value, limit) => s"${field.label} must not exceed $limit, was $value"
      case NonPositiveBatchSize(value)             => s"batchSize must be positive, was $value"
      case BlankGroupInstanceId                    => "group instance id must not be blank"
      case HeartbeatNotBelowSessionTimeout(heartbeat, session) =>
        s"heartbeatInterval must be less than sessionTimeout, was $heartbeat against $session"
      case MaxPollIntervalBelowSessionTimeout(maxPoll, session) => s"maxPollInterval must be at least sessionTimeout, was $maxPoll against $session"
      case RebalanceSafeCommitLimitNotBelowMaxPollInterval(limit, maxPoll) =>
        s"the rebalance-safe commit limit must be less than maxPollInterval, was $limit against $maxPoll"
      case StartingOffsetsLimitNotBelowMaxPollInterval(limit, maxPoll) =>
        s"the starting offsets limit must be less than maxPollInterval, was $limit against $maxPoll"

object SettingsError:
  val MaxDuration: FiniteDuration = Int.MaxValue.toLong.millis

  enum DurationField(val label: String) derives CanEqual:
    case MetadataRefreshInterval  extends DurationField("metadataRefreshInterval")
    case CloseTimeout             extends DurationField("closeTimeout")
    case TransactionTimeout       extends DurationField("transactionTimeout")
    case CommitTimeout            extends DurationField("commitTimeout")
    case PollTimeout              extends DurationField("pollTimeout")
    case RequestTimeout           extends DurationField("requestTimeout")
    case Linger                   extends DurationField("linger")
    case SessionTimeout           extends DurationField("sessionTimeout")
    case HeartbeatInterval        extends DurationField("heartbeatInterval")
    case MaxPollInterval          extends DurationField("maxPollInterval")
    case RebalanceSafeCommitLimit extends DurationField("rebalanceSafeCommits.limit")
    case StartingOffsetsLimit     extends DurationField("startingOffsets.limit")

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

/** Checks a positive duration both backends accept up to a limit, which is the lower of their two maximums. */
private def limitedDurationErrors(field: SettingsError.DurationField, value: FiniteDuration, limit: FiniteDuration): List[SettingsError] =
  if value <= Duration.Zero then List(SettingsError.NonPositiveDuration(field, value))
  else if value > limit then List(SettingsError.DurationAboveLimit(field, value, limit))
  else Nil

/** Checks a duration both backends accept from zero up to a limit, which is the lower of their two maximums. */
private def boundedDurationErrors(field: SettingsError.DurationField, value: FiniteDuration, limit: FiniteDuration): List[SettingsError] =
  if value < Duration.Zero then List(SettingsError.NegativeDuration(field, value))
  else if value > limit then List(SettingsError.DurationAboveLimit(field, value, limit))
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

  /** How often the client refreshes what it knows of the cluster, which bounds how long a topic created after it started can stay unseen. */
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
    closeTimeout: FiniteDuration,
    idempotence: Boolean,
    compression: Compression,
    linger: FiniteDuration,
    batchSize: Option[Int]
):
  def mapK[G[_]](fk: FunctionK[F, G]): ProducerSettings[G, K, V] =
    new ProducerSettings(
      client,
      keySerializer.mapK(fk),
      valueSerializer.mapK(fk),
      properties,
      acks,
      closeTimeout,
      idempotence,
      compression,
      linger,
      batchSize
    ) {}

  def withClient(value: ClientSettings): ProducerSettings[F, K, V] =
    new ProducerSettings(value, keySerializer, valueSerializer, properties, acks, closeTimeout, idempotence, compression, linger, batchSize) {}

  /** How many replicas must have a record before the broker answers for it. Anything short of `AllReplicas` needs idempotence turned off first. */
  def withAcks(value: Acks): ValidatedNel[SettingsError, ProducerSettings[F, K, V]] =
    ProducerSettings.from(client, keySerializer, valueSerializer, properties, value, closeTimeout, idempotence, compression, linger, batchSize)

  /** Turns on Kafka's idempotent producer, `enable.idempotence`, which is the default. It needs `acks` to be `AllReplicas`. */
  def withIdempotence: ValidatedNel[SettingsError, ProducerSettings[F, K, V]] =
    ProducerSettings.from(client, keySerializer, valueSerializer, properties, acks, closeTimeout, true, compression, linger, batchSize)

  /** Turns off Kafka's idempotent producer, `enable.idempotence`, which any `acks` short of `AllReplicas` needs. */
  def withoutIdempotence: ProducerSettings[F, K, V] =
    new ProducerSettings(client, keySerializer, valueSerializer, properties, acks, closeTimeout, false, compression, linger, batchSize) {}

  /** How long releasing a producer waits to deliver the records it has already accepted, before dropping whatever is left. */
  def withCloseTimeout(value: FiniteDuration): ValidatedNel[SettingsError, ProducerSettings[F, K, V]] =
    ProducerSettings.from(client, keySerializer, valueSerializer, properties, acks, value, idempotence, compression, linger, batchSize)

  /** How record batches are compressed on their way to the broker. */
  def withCompression(value: Compression): ProducerSettings[F, K, V] =
    new ProducerSettings(client, keySerializer, valueSerializer, properties, acks, closeTimeout, idempotence, value, linger, batchSize) {}

  /** How long a record waits for others to join its batch before the batch is sent, from zero up to `ProducerSettings.LingerLimit`. */
  def withLinger(value: FiniteDuration): ValidatedNel[SettingsError, ProducerSettings[F, K, V]] =
    ProducerSettings.from(client, keySerializer, valueSerializer, properties, acks, closeTimeout, idempotence, compression, value, batchSize)

  /** The most bytes of records one batch holds before it is sent without waiting out the linger. */
  def withBatchSize(value: Int): ValidatedNel[SettingsError, ProducerSettings[F, K, V]] =
    ProducerSettings.from(client, keySerializer, valueSerializer, properties, acks, closeTimeout, idempotence, compression, linger, Some(value))

  /** Leaves the batch size to the backend, which is the default. */
  def withoutBatchSize: ProducerSettings[F, K, V] =
    new ProducerSettings(client, keySerializer, valueSerializer, properties, acks, closeTimeout, idempotence, compression, linger, None) {}

  def withProperty(name: String, value: String): ValidatedNel[SettingsError, ProducerSettings[F, K, V]] =
    withProperties(properties.updated(name, value))

  def withProperties(values: Map[String, String]): ValidatedNel[SettingsError, ProducerSettings[F, K, V]] =
    ProducerSettings.from(client, keySerializer, valueSerializer, values, acks, closeTimeout, idempotence, compression, linger, batchSize)

  /** The Kafka properties these settings own, spelled the same for both clients. */
  private[xkafka] def managedProperties: Map[String, String] =
    Map(
      "acks"               -> acks.property,
      "enable.idempotence" -> idempotence.toString,
      "compression.type"   -> compression.property,
      "linger.ms"          -> linger.toMillis.toString
    ) ++ batchSize.map(size => "batch.size" -> size.toString)

  override def toString: String =
    s"ProducerSettings($client,$keySerializer,$valueSerializer,${redacted(
        properties
      )},$acks,$closeTimeout,$idempotence,$compression,$linger,$batchSize)"

object ProducerSettings:
  /** What both backends already ask for, and the only setting that survives losing a leader. */
  val DefaultAcks: Acks = Acks.AllReplicas

  /** Long enough that a producer with records still in flight delivers them rather than dropping them on the way out. */
  val DefaultCloseTimeout: FiniteDuration = 60.seconds

  /** What both backends wait by default. */
  val DefaultLinger: FiniteDuration = 5.millis

  /** The longest linger the settings accept. */
  val LingerLimit: FiniteDuration = 15.minutes

  def from[F[_], K, V](
      client: ClientSettings,
      keySerializer: Serializer[F, K],
      valueSerializer: Serializer[F, V],
      properties: Map[String, String] = Map.empty,
      acks: Acks = ProducerSettings.DefaultAcks,
      closeTimeout: FiniteDuration = ProducerSettings.DefaultCloseTimeout,
      idempotence: Boolean = true,
      compression: Compression = Compression.Uncompressed,
      linger: FiniteDuration = ProducerSettings.DefaultLinger,
      batchSize: Option[Int] = None
  ): ValidatedNel[SettingsError, ProducerSettings[F, K, V]] =
    val errors =
      propertyErrors(properties, SettingsError.PropertyScope.Producer) ++ durationErrors(SettingsError.DurationField.CloseTimeout, closeTimeout) ++
        Option.when(idempotence && acks != Acks.AllReplicas)(SettingsError.IdempotenceRequiresAllReplicas(acks)) ++
        boundedDurationErrors(SettingsError.DurationField.Linger, linger, LingerLimit) ++ batchSize.filter(_ <= 0)
          .map(SettingsError.NonPositiveBatchSize(_))
    validateSettings(errors).map(_ =>
      new ProducerSettings(client, keySerializer, valueSerializer, properties, acks, closeTimeout, idempotence, compression, linger, batchSize) {}
    )

  given [K, V]: FunctorK[[F[_]] =>> ProducerSettings[F, K, V]] with
    override def mapK[F[_], G[_]](settings: ProducerSettings[F, K, V])(fk: FunctionK[F, G]): ProducerSettings[G, K, V] = settings.mapK(fk)

/** Kafka requires idempotence of every transactional producer, so each backend asks for it outright. */
private[xkafka] val TransactionalIdempotence: (String, String) = "enable.idempotence" -> "true"

/** Settings for a producer that writes only inside transactions. It always waits for every in-sync replica and is always idempotent. */
sealed abstract case class TransactionalProducerSettings[F[_], K, V] private (
    producer: ProducerSettings[F, K, V],
    transactionalId: TransactionalId,
    transactionTimeout: FiniteDuration
):
  def mapK[G[_]](fk: FunctionK[F, G]): TransactionalProducerSettings[G, K, V] =
    new TransactionalProducerSettings(producer.mapK(fk), transactionalId, transactionTimeout) {}

  def withClient(value: ClientSettings): TransactionalProducerSettings[F, K, V] =
    new TransactionalProducerSettings(producer.withClient(value), transactionalId, transactionTimeout) {}

  /** How long the broker lets a transaction stay open before it aborts it, Kafka's `transaction.timeout.ms`. */
  def withTransactionTimeout(value: FiniteDuration): ValidatedNel[SettingsError, TransactionalProducerSettings[F, K, V]] =
    validateSettings(durationErrors(SettingsError.DurationField.TransactionTimeout, value))
      .map(_ => new TransactionalProducerSettings(producer, transactionalId, value) {})

  /** How long releasing the producer waits for records it has not delivered yet. */
  def withCloseTimeout(value: FiniteDuration): ValidatedNel[SettingsError, TransactionalProducerSettings[F, K, V]] =
    producer.withCloseTimeout(value).map(new TransactionalProducerSettings(_, transactionalId, transactionTimeout) {})

  /** How record batches are compressed on their way to the broker. */
  def withCompression(value: Compression): TransactionalProducerSettings[F, K, V] =
    new TransactionalProducerSettings(producer.withCompression(value), transactionalId, transactionTimeout) {}

  /** How long a record waits for others to join its batch before the batch is sent, from zero up to `ProducerSettings.LingerLimit`. */
  def withLinger(value: FiniteDuration): ValidatedNel[SettingsError, TransactionalProducerSettings[F, K, V]] =
    producer.withLinger(value).map(new TransactionalProducerSettings(_, transactionalId, transactionTimeout) {})

  /** The most bytes of records one batch holds before it is sent without waiting out the linger. */
  def withBatchSize(value: Int): ValidatedNel[SettingsError, TransactionalProducerSettings[F, K, V]] =
    producer.withBatchSize(value).map(new TransactionalProducerSettings(_, transactionalId, transactionTimeout) {})

  /** Leaves the batch size to the backend, which is the default. */
  def withoutBatchSize: TransactionalProducerSettings[F, K, V] =
    new TransactionalProducerSettings(producer.withoutBatchSize, transactionalId, transactionTimeout) {}

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
      properties: Map[String, String] = Map.empty,
      closeTimeout: FiniteDuration = ProducerSettings.DefaultCloseTimeout,
      compression: Compression = Compression.Uncompressed,
      linger: FiniteDuration = ProducerSettings.DefaultLinger,
      batchSize: Option[Int] = None
  ): ValidatedNel[SettingsError, TransactionalProducerSettings[F, K, V]] =
    (
      ProducerSettings.from(
        client,
        keySerializer,
        valueSerializer,
        properties,
        closeTimeout = closeTimeout,
        compression = compression,
        linger = linger,
        batchSize = batchSize
      ),
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

/** How a producer compresses its record batches. */
enum Compression:
  /** What both backends read this as, which is the same string on each of them. */
  private[xkafka] def property: String =
    this match
      case Compression.Uncompressed => "none"
      case Compression.Gzip         => "gzip"
      case Compression.Snappy       => "snappy"
      case Compression.Lz4          => "lz4"
      case Compression.Zstd         => "zstd"

  /** Batches are sent as they are, which is the default. */
  case Uncompressed

  case Gzip

  case Snappy

  case Lz4

  case Zstd

/** Where a consumer starts in a partition where its group has no committed offset, or where that offset is no longer in the partition's log. */
enum AutoOffsetReset:
  /** At the first record still in the partition. */
  case Earliest

  /** After the last record, reading only what arrives from then on. */
  case Latest

  /** Nowhere: its record stream fails as `ErrorCode.OffsetResetRequired`. */
  case Fail

/** Whether a consumer reads records written by transactions that have not committed. */
enum IsolationLevel:
  /** Every record is delivered as soon as it is written, including records of a transaction that later aborts. */
  case ReadUncommitted

  /** A record written by a transaction is delivered once that transaction commits, and never where it aborts. */
  case ReadCommitted

/** How long a consumer holds back the revocation of a partition for the records it handed out from it to be committed, and what happens if they still
  * are not.
  */
final case class RebalanceSafeCommits(limit: FiniteDuration, atLimit: RebalanceSafeCommits.AtLimit)

object RebalanceSafeCommits:
  /** What a revocation does once it has waited `limit` with records still uncommitted. */
  enum AtLimit derives CanEqual:
    /** The revocation goes ahead, and whoever the partition goes to reads those records again. */
    case Proceed

    /** The revocation goes ahead, and the consumer's record stream fails as `KafkaException.UncommittedRevocation`. */
    case Fail

/** Where an assigned partition starts reading, with the leader epoch of the record before it where the application kept one. */
final case class StartingOffset(offset: Offset, leaderEpoch: Option[LeaderEpoch] = None)

/** Offsets an application keeps itself, which each partition assigned to a consumer starts from.
  *
  * `offsets` gets the partitions just assigned and answers with where each starts. It must answer within `limit`.
  */
final case class StartingOffsets[F[_]](offsets: Set[TopicPartition] => F[Map[TopicPartition, StartingOffset]], limit: FiniteDuration):
  def mapK[G[_]](fk: FunctionK[F, G]): StartingOffsets[G] = StartingOffsets(partitions => fk(offsets(partitions)), limit)

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
    properties: Map[String, String],
    assignmentFencing: Boolean,
    groupInstanceId: Option[String],
    sessionTimeout: Option[FiniteDuration],
    heartbeatInterval: Option[FiniteDuration],
    maxPollInterval: FiniteDuration,
    rebalanceSafeCommits: Option[RebalanceSafeCommits],
    startingOffsets: Option[StartingOffsets[F]]
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
      properties,
      assignmentFencing,
      groupInstanceId,
      sessionTimeout,
      heartbeatInterval,
      maxPollInterval,
      rebalanceSafeCommits,
      startingOffsets.map(_.mapK(fk))
    ) {}

  def withClient(value: ClientSettings): ConsumerSettings[F, K, V] = copied(client = value)

  def withGroupId(value: ConsumerGroup): ConsumerSettings[F, K, V] = copied(groupId = value)

  def withAutoOffsetReset(value: AutoOffsetReset): ConsumerSettings[F, K, V] = copied(autoOffsetReset = value)

  /** Whether records of a transaction that has not committed are delivered. */
  def withIsolationLevel(value: IsolationLevel): ConsumerSettings[F, K, V] = copied(isolationLevel = value)

  /** How long a commit waits for the broker before it fails as `ErrorCode.RequestTimedOut`, which the recovery policy retries. */
  def withCommitTimeout(value: FiniteDuration): ValidatedNel[SettingsError, ConsumerSettings[F, K, V]] = revalidated(commitTimeout = value)

  /** How a failed offset commit is retried. */
  def withCommitRecovery(value: CommitRecovery): ConsumerSettings[F, K, V] = copied(commitRecovery = value)

  /** Refuses offsets from a partition this consumer no longer owns, which is the default.
    *
    * An offset read before its partition was revoked fails to commit with `ErrorCode.IllegalGeneration`, whether it is committed on its own, in a
    * batch, or in a transaction.
    */
  def withAssignmentFencing: ConsumerSettings[F, K, V] = copied(assignmentFencing = true)

  /** Commits offsets even after their partition has moved to another consumer, which can move that partition back to an earlier position. */
  def withoutAssignmentFencing: ConsumerSettings[F, K, V] = copied(assignmentFencing = false)

  /** How long one poll waits for records before it returns empty. */
  def withPollTimeout(value: FiniteDuration): ValidatedNel[SettingsError, ConsumerSettings[F, K, V]] = revalidated(pollTimeout = value)

  /** How long a call that asks the broker something waits for its answer.
    *
    * It bounds `committed`, `beginningOffsets`, `endOffsets`, `offsetsForTimes`, `partitionsFor`, `listTopics`, `seek`, `seekToBeginning`, and
    * `seekToEnd`.
    */
  def withRequestTimeout(value: FiniteDuration): ValidatedNel[SettingsError, ConsumerSettings[F, K, V]] = revalidated(requestTimeout = value)

  /** Makes the consumer a static member of its group under this identity, Kafka's `group.instance.id`.
    *
    * A static member that is released, or stops, keeps its partitions until its session times out, so one that comes back under the same identity
    * within the session timeout gets them back without the rest of the group rebalancing.
    */
  def withGroupInstanceId(value: String): ValidatedNel[SettingsError, ConsumerSettings[F, K, V]] = revalidated(groupInstanceId = Some(value))

  /** Makes the consumer a dynamic member of its group, which is the default: it leaves the group when it is released. */
  def withoutGroupInstanceId: ConsumerSettings[F, K, V] = copied(groupInstanceId = None)

  /** How long the group waits to hear from the consumer before it hands the consumer's partitions to the rest, from more than the heartbeat interval
    * up to `ConsumerSettings.SessionTimeoutLimit`. The broker also bounds it, through `group.min.session.timeout.ms` and
    * `group.max.session.timeout.ms`.
    */
  def withSessionTimeout(value: FiniteDuration): ValidatedNel[SettingsError, ConsumerSettings[F, K, V]] = revalidated(sessionTimeout = Some(value))

  /** Leaves the session timeout unset, which is the default, and is `ConsumerSettings.DefaultSessionTimeout` on every platform. */
  def withoutSessionTimeout: ValidatedNel[SettingsError, ConsumerSettings[F, K, V]] = revalidated(sessionTimeout = None)

  /** How often the consumer tells the group it is alive, which must be less than the session timeout. */
  def withHeartbeatInterval(value: FiniteDuration): ValidatedNel[SettingsError, ConsumerSettings[F, K, V]] =
    revalidated(heartbeatInterval = Some(value))

  /** Leaves the heartbeat interval unset, which is the default, and is `ConsumerSettings.DefaultHeartbeatInterval` on every platform. */
  def withoutHeartbeatInterval: ValidatedNel[SettingsError, ConsumerSettings[F, K, V]] = revalidated(heartbeatInterval = None)

  /** How long the consumer can go without fetching records before it leaves its group and its partitions go to the others. Fetching stops while too
    * many fetched records wait unread, so records left unread for this long make the consumer leave. It must be at least the session timeout, and at
    * most `ConsumerSettings.MaxPollIntervalLimit`.
    */
  def withMaxPollInterval(value: FiniteDuration): ValidatedNel[SettingsError, ConsumerSettings[F, K, V]] = revalidated(maxPollInterval = value)

  /** Holds back the revocation of a partition until every record `records` handed out from it is committed, for at most `limit`, which must be less
    * than the maximum poll interval.
    *
    * While it waits, the partition still belongs to this consumer, so those commits succeed, and the records of the partition that have not been
    * handed out yet are dropped. A partition the group has already given away is let go at once, since nothing can be committed for it.
    */
  def withRebalanceSafeCommits(
      limit: FiniteDuration,
      atLimit: RebalanceSafeCommits.AtLimit = RebalanceSafeCommits.AtLimit.Proceed
  ): ValidatedNel[SettingsError, ConsumerSettings[F, K, V]] = revalidated(rebalanceSafeCommits = Some(RebalanceSafeCommits(limit, atLimit)))

  /** Lets a revocation go ahead without waiting for the records handed out to be committed, which is the default. With assignment fencing on, it
    * still waits for commits and transactions already under way for the partition.
    */
  def withoutRebalanceSafeCommits: ConsumerSettings[F, K, V] = copied(rebalanceSafeCommits = None)

  /** Starts each assigned partition at the offset `offsets` answers for it, which must answer within `limit`, itself less than the maximum poll
    * interval.
    *
    * It is called with the partitions each rebalance assigns, before any of their records are fetched, and, for `Selection.Partitions`, once when the
    * consumer is created. A partition it answers nothing for starts where it would without it. If it fails, or passes its limit as
    * `KafkaException.StartingOffsetsTimedOut`, the record stream fails, or for `Selection.Partitions`, creating the consumer fails.
    */
  def withStartingOffsets(limit: FiniteDuration)(
      offsets: Set[TopicPartition] => F[Map[TopicPartition, StartingOffset]]
  ): ValidatedNel[SettingsError, ConsumerSettings[F, K, V]] = revalidated(startingOffsets = Some(StartingOffsets(offsets, limit)))

  /** Starts each assigned partition at its committed offset, or where `autoOffsetReset` says, which is the default. */
  def withoutStartingOffsets: ConsumerSettings[F, K, V] = copied(startingOffsets = None)

  def withProperty(name: String, value: String): ValidatedNel[SettingsError, ConsumerSettings[F, K, V]] =
    withProperties(properties.updated(name, value))

  def withProperties(values: Map[String, String]): ValidatedNel[SettingsError, ConsumerSettings[F, K, V]] = revalidated(properties = values)

  /** The Kafka properties of group membership these settings own, spelled the same for both clients. A timeout left to the backend is not set, since
    * both clients refuse one under `group.protocol=consumer`, where the broker decides it.
    */
  private[xkafka] def managedProperties: Map[String, String] =
    Map("max.poll.interval.ms" -> maxPollInterval.toMillis.toString) ++ groupInstanceId.map("group.instance.id" -> _) ++
      sessionTimeout.map(value => "session.timeout.ms" -> value.toMillis.toString) ++
      heartbeatInterval.map(value => "heartbeat.interval.ms" -> value.toMillis.toString)

  private def copied(
      client: ClientSettings = client,
      groupId: ConsumerGroup = groupId,
      autoOffsetReset: AutoOffsetReset = autoOffsetReset,
      isolationLevel: IsolationLevel = isolationLevel,
      commitRecovery: CommitRecovery = commitRecovery,
      assignmentFencing: Boolean = assignmentFencing,
      groupInstanceId: Option[String] = groupInstanceId,
      rebalanceSafeCommits: Option[RebalanceSafeCommits] = rebalanceSafeCommits,
      startingOffsets: Option[StartingOffsets[F]] = startingOffsets
  ): ConsumerSettings[F, K, V] =
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
      properties,
      assignmentFencing,
      groupInstanceId,
      sessionTimeout,
      heartbeatInterval,
      maxPollInterval,
      rebalanceSafeCommits,
      startingOffsets
    ) {}

  private def revalidated(
      commitTimeout: FiniteDuration = commitTimeout,
      pollTimeout: FiniteDuration = pollTimeout,
      requestTimeout: FiniteDuration = requestTimeout,
      properties: Map[String, String] = properties,
      groupInstanceId: Option[String] = groupInstanceId,
      sessionTimeout: Option[FiniteDuration] = sessionTimeout,
      heartbeatInterval: Option[FiniteDuration] = heartbeatInterval,
      maxPollInterval: FiniteDuration = maxPollInterval,
      rebalanceSafeCommits: Option[RebalanceSafeCommits] = rebalanceSafeCommits,
      startingOffsets: Option[StartingOffsets[F]] = startingOffsets
  ): ValidatedNel[SettingsError, ConsumerSettings[F, K, V]] =
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
      properties,
      assignmentFencing,
      groupInstanceId,
      sessionTimeout,
      heartbeatInterval,
      maxPollInterval,
      rebalanceSafeCommits,
      startingOffsets
    )

  override def toString: String =
    s"ConsumerSettings($client,$groupId,$keyDeserializer,$valueDeserializer,$autoOffsetReset,$isolationLevel,$commitRecovery,$commitTimeout,$pollTimeout,$requestTimeout,${redacted(
        properties
      )},$assignmentFencing,$groupInstanceId,$sessionTimeout,$heartbeatInterval,$maxPollInterval,$rebalanceSafeCommits,${startingOffsets
        .map(value => s"StartingOffsets(${value.limit})")})"

object ConsumerSettings:
  /** Short enough to keep other calls on the consumer responsive, long enough that an idle poll is not a spin. */
  val DefaultPollTimeout: FiniteDuration = 100.millis

  /** Long enough that a commit is not abandoned while the coordinator is merely busy. */
  val DefaultCommitTimeout: FiniteDuration = 15.seconds

  /** What Kafka's own clients wait for the same answers, through `default.api.timeout.ms`. */
  val DefaultRequestTimeout: FiniteDuration = 60.seconds

  /** The session timeout both backends use when it is left to them. */
  val DefaultSessionTimeout: FiniteDuration = 45.seconds

  /** The heartbeat interval both backends use when it is left to them. */
  val DefaultHeartbeatInterval: FiniteDuration = 3.seconds

  /** What both backends use. */
  val DefaultMaxPollInterval: FiniteDuration = 5.minutes

  /** The longest session timeout the settings accept. */
  val SessionTimeoutLimit: FiniteDuration = 1.hour

  /** The longest heartbeat interval the settings accept. */
  val HeartbeatIntervalLimit: FiniteDuration = 1.hour

  /** The longest maximum poll interval the settings accept. */
  val MaxPollIntervalLimit: FiniteDuration = 24.hours

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
      properties: Map[String, String] = Map.empty,
      assignmentFencing: Boolean = true,
      groupInstanceId: Option[String] = None,
      sessionTimeout: Option[FiniteDuration] = None,
      heartbeatInterval: Option[FiniteDuration] = None,
      maxPollInterval: FiniteDuration = ConsumerSettings.DefaultMaxPollInterval,
      rebalanceSafeCommits: Option[RebalanceSafeCommits] = None,
      startingOffsets: Option[StartingOffsets[F]] = None
  ): ValidatedNel[SettingsError, ConsumerSettings[F, K, V]] =
    val session   = sessionTimeout.getOrElse(DefaultSessionTimeout)
    val heartbeat = heartbeatInterval.getOrElse(DefaultHeartbeatInterval)
    val intervals =
      sessionTimeout.toList.flatMap(limitedDurationErrors(SettingsError.DurationField.SessionTimeout, _, SessionTimeoutLimit)) ++
        heartbeatInterval.toList.flatMap(limitedDurationErrors(SettingsError.DurationField.HeartbeatInterval, _, HeartbeatIntervalLimit)) ++
        limitedDurationErrors(SettingsError.DurationField.MaxPollInterval, maxPollInterval, MaxPollIntervalLimit) ++
        rebalanceSafeCommits.toList.flatMap(safe => durationErrors(SettingsError.DurationField.RebalanceSafeCommitLimit, safe.limit)) ++
        startingOffsets.toList.flatMap(starting => durationErrors(SettingsError.DurationField.StartingOffsetsLimit, starting.limit))
    // The Java client refuses the first combination and librdkafka the second, so both are refused here for every backend. They are only
    // compared once each is valid on its own.
    val combinations =
      if intervals.nonEmpty then Nil
      else
        Option.when(heartbeat >= session)(SettingsError.HeartbeatNotBelowSessionTimeout(heartbeat, session)).toList ++
          Option.when(maxPollInterval < session)(SettingsError.MaxPollIntervalBelowSessionTimeout(maxPollInterval, session)) ++
          // A revocation held longer than this outlasts the group's patience, which removes the consumer from it.
          rebalanceSafeCommits.filter(_.limit >= maxPollInterval)
            .map(safe => SettingsError.RebalanceSafeCommitLimitNotBelowMaxPollInterval(safe.limit, maxPollInterval)) ++
          // The rest of the group waits while the offsets are fetched, so the same holds for them.
          startingOffsets.filter(_.limit >= maxPollInterval)
            .map(starting => SettingsError.StartingOffsetsLimitNotBelowMaxPollInterval(starting.limit, maxPollInterval))
    val errors =
      propertyErrors(properties, SettingsError.PropertyScope.Consumer) ++ durationErrors(SettingsError.DurationField.CommitTimeout, commitTimeout) ++
        durationErrors(SettingsError.DurationField.PollTimeout, pollTimeout) ++
        durationErrors(SettingsError.DurationField.RequestTimeout, requestTimeout) ++
        Option.when(groupInstanceId.exists(_.trim.isEmpty))(SettingsError.BlankGroupInstanceId) ++ intervals ++ combinations
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
        properties,
        assignmentFencing,
        groupInstanceId,
        sessionTimeout,
        heartbeatInterval,
        maxPollInterval,
        rebalanceSafeCommits,
        startingOffsets
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

/** A topic's effective configuration, by property name, holding every entry the broker reported. */
final case class TopicConfiguration(entries: Map[String, ConfigurationEntry]):
  /** The entry for this property, or `None` where the broker reported none. */
  def get(name: String): Option[ConfigurationEntry] = entries.get(name)

/** One configuration property of a topic, with where its value comes from. */
final case class ConfigurationEntry(value: ConfigurationValue, source: ConfigurationSource, readOnly: Boolean)

object ConfigurationEntry:
  /** Builds an entry from what a backend reports, where a sensitive value always arrives without its value. */
  private[xkafka] def reported(value: Option[String], sensitive: Boolean, source: ConfigurationSource, readOnly: Boolean): ConfigurationEntry =
    val portable = if sensitive then ConfigurationValue.Redacted else value.fold(ConfigurationValue.Absent)(ConfigurationValue.Present(_))
    ConfigurationEntry(portable, source, readOnly)

enum ConfigurationValue derives CanEqual:
  /** The broker reported this value. */
  case Present(value: String)

  /** The property has no value. */
  case Absent

  /** The value is sensitive, so the broker withheld it. */
  case Redacted

/** Where a topic's configuration value comes from. */
enum ConfigurationSource derives CanEqual:
  /** Set on the topic itself. */
  case TopicOverride

  /** Set dynamically on the broker that answered. */
  case DynamicBroker

  /** Set dynamically as the default for every broker in the cluster. */
  case DynamicClusterDefault

  /** Set in the broker's own configuration file. */
  case StaticBroker

  /** Kafka's default, set nowhere. */
  case Default

  /** A source none of the other cases describe. */
  case Unknown

object ConfigurationSource:
  /** Classifies librdkafka's `rd_kafka_ConfigSource_t`. */
  private[xkafka] def fromLibrdkafka(value: Int): ConfigurationSource =
    value match
      case 1 => TopicOverride         // DYNAMIC_TOPIC_CONFIG
      case 2 => DynamicBroker         // DYNAMIC_BROKER_CONFIG
      case 3 => DynamicClusterDefault // DYNAMIC_DEFAULT_BROKER_CONFIG
      case 4 => StaticBroker          // STATIC_BROKER_CONFIG
      case 5 => Default               // DEFAULT_CONFIG
      case _ => Unknown

/** A broker of the cluster, as it advertises itself to clients. */
final case class Broker(id: Int, host: String, port: Int, rack: Option[String])

/** Where a consumer group is in its life. */
enum ConsumerGroupState derives CanEqual:
  /** Its members hold their assignments. */
  case Stable

  /** Members are joining or leaving, so its partitions are about to be assigned again. */
  case PreparingRebalance

  /** Its members have joined and are waiting for their assignments. */
  case CompletingRebalance

  /** It has no members, though it may still hold committed offsets. */
  case Empty

  /** A state none of the other cases describe. */
  case Unknown

object ConsumerGroupState:
  /** Classifies librdkafka's `rd_kafka_consumer_group_state_t`. */
  private[xkafka] def fromLibrdkafka(value: Int): ConsumerGroupState =
    value match
      case 1 => PreparingRebalance  // PREPARING_REBALANCE
      case 2 => CompletingRebalance // COMPLETING_REBALANCE
      case 3 => Stable              // STABLE
      case 5 => Empty               // EMPTY
      case _ => Unknown

/** A member of a consumer group and the partitions it is assigned.
  *
  * @param instanceId
  *   the member's static group instance id, where it has one
  */
final case class GroupMember(memberId: String, instanceId: Option[String], clientId: String, host: String, assignment: Set[TopicPartition])

/** What a consumer group is doing and who is in it.
  *
  * @param simple
  *   whether the group holds committed offsets but no consumer has joined it, such as one created by `alterConsumerGroupOffsets`
  * @param assignor
  *   the name of the assignor that chose the members' assignments, where the group has one
  * @param coordinator
  *   the broker that coordinates the group, where it has one
  */
final case class ConsumerGroupDescription(
    state: ConsumerGroupState,
    simple: Boolean,
    assignor: Option[String],
    coordinator: Option[Broker],
    members: List[GroupMember]
)

/** Topic and consumer group administration.
  *
  * Creating, deleting, and growing topics report only the first failure, even when several topics fail. Deleting records, describing topic
  * configurations, and the consumer group calls that name partitions or groups answer for each one.
  */
trait KafkaAdminClient[F[_]]:
  self =>

  /** Creates each topic, failing if any of them cannot be created, including where it already exists. */
  def createTopics(topics: NonEmptySet[NewTopic]): F[Unit]

  /** Deletes each topic, failing if any of them cannot be deleted, including where it does not exist. */
  def deleteTopics(topics: NonEmptySet[Topic]): F[Unit]

  /** Grows the topic to `count` partitions in all, failing where it already has `count` or more. */
  def createPartitions(topic: Topic, count: Int): F[Unit]

  /** Reports the partitions of each requested topic, failing where the cluster does not have one of them. */
  def describeTopics(topics: NonEmptySet[Topic]): F[Map[Topic, Set[Partition]]]

  /** Deletes each partition's records below its offset, answering for each one its new low watermark or the reason the broker refused it.
    *
    * Every requested partition gets an answer, including a topic or partition the cluster does not have, which answers as
    * `ErrorCode.UnknownTopicOrPartition`. The call fails as a whole only when the cluster answered for none of the partitions, such as when it cannot
    * be reached in time. A partition already beginning at or after its offset keeps its records and answers with its current low watermark. An offset
    * past the end of the partition is refused as `ErrorCode.OffsetOutOfRange`. Deleting records moves no consumer group's offsets.
    */
  def deleteRecords(before: NonEmptyMap[TopicPartition, Offset]): F[Map[TopicPartition, Either[KafkaException, Offset]]]

  /** Reports each topic's effective configuration, including the values it inherits, or the reason the broker would not describe it.
    *
    * Every requested topic gets an answer, including one the cluster does not have, which answers as `ErrorCode.UnknownTopicOrPartition`. The call
    * fails as a whole only when the cluster answered for none of the topics, such as when it cannot be reached in time.
    */
  def describeTopicConfigurations(topics: NonEmptySet[Topic]): F[Map[Topic, Either[KafkaException, TopicConfiguration]]]

  /** Every consumer group the cluster knows, with its state, including a group with no members that still holds committed offsets.
    *
    * Kafka keeps committed offsets under an empty group name and lists that group, but it is left out here, because `ConsumerGroup` does not accept
    * an empty name.
    */
  def listConsumerGroups: F[Map[ConsumerGroup, ConsumerGroupState]]

  /** Describes each group, or answers why it could not be described.
    *
    * Each group is answered on its own, and one the cluster does not have answers as `ErrorCode.GroupIdNotFound`. The call fails as a whole only when
    * the cluster answered for none of the groups, such as when it cannot be reached in time.
    */
  def describeConsumerGroups(groups: NonEmptySet[ConsumerGroup]): F[NonEmptyMap[ConsumerGroup, Either[KafkaException, ConsumerGroupDescription]]]

  /** Every offset the group has committed, which is empty where it has committed none or the cluster does not have the group. */
  def listConsumerGroupOffsets(group: ConsumerGroup): F[Map[TopicPartition, OffsetAndMetadata]]

  /** The offset the group has committed for each partition, or `None` where it has committed none. */
  def listConsumerGroupOffsets(
      group: ConsumerGroup,
      partitions: NonEmptySet[TopicPartition]
  ): F[NonEmptyMap[TopicPartition, Option[OffsetAndMetadata]]]

  /** Sets the group's committed offsets, answering for each partition whether it was set.
    *
    * Each partition is answered on its own, and a topic the cluster does not have answers as `ErrorCode.UnknownTopicOrPartition`. The call fails as a
    * whole only when the cluster answered for none of the partitions, such as when it cannot be reached in time. A group the cluster does not have is
    * created holding these offsets. A group with members refuses them as `ErrorCode.UnknownMemberId`, so stop its consumers first.
    */
  def alterConsumerGroupOffsets(
      group: ConsumerGroup,
      offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]
  ): F[NonEmptyMap[TopicPartition, Either[KafkaException, Unit]]]

  /** Deletes the group's committed offsets for these partitions, answering for each partition whether they were deleted.
    *
    * Each partition is answered on its own. The call fails as a whole only when the cluster answered for none of the partitions, such as when it
    * cannot be reached in time. A partition whose topic the group is subscribed to answers as `ErrorCode.GroupSubscribedToTopic`, and every partition
    * of a group the cluster does not have as `ErrorCode.GroupIdNotFound`.
    */
  def deleteConsumerGroupOffsets(
      group: ConsumerGroup,
      partitions: NonEmptySet[TopicPartition]
  ): F[NonEmptyMap[TopicPartition, Either[KafkaException, Unit]]]

  /** Deletes each group with its committed offsets, answering for each group whether it was deleted.
    *
    * Each group is answered on its own. The call fails as a whole only when the cluster answered for none of the groups, such as when it cannot be
    * reached in time. A group with members answers as `ErrorCode.NonEmptyGroup`, so stop its consumers first, and one the cluster does not have as
    * `ErrorCode.GroupIdNotFound`.
    */
  def deleteConsumerGroups(groups: NonEmptySet[ConsumerGroup]): F[NonEmptyMap[ConsumerGroup, Either[KafkaException, Unit]]]

  final def mapK[G[_]](fk: FunctionK[F, G]): KafkaAdminClient[G] =
    new KafkaAdminClient[G]:
      override def createTopics(topics: NonEmptySet[NewTopic]): G[Unit] = fk(self.createTopics(topics))

      override def deleteTopics(topics: NonEmptySet[Topic]): G[Unit] = fk(self.deleteTopics(topics))

      override def createPartitions(topic: Topic, count: Int): G[Unit] = fk(self.createPartitions(topic, count))

      override def describeTopics(topics: NonEmptySet[Topic]): G[Map[Topic, Set[Partition]]] = fk(self.describeTopics(topics))

      override def deleteRecords(before: NonEmptyMap[TopicPartition, Offset]): G[Map[TopicPartition, Either[KafkaException, Offset]]] =
        fk(self.deleteRecords(before))

      override def describeTopicConfigurations(topics: NonEmptySet[Topic]): G[Map[Topic, Either[KafkaException, TopicConfiguration]]] =
        fk(self.describeTopicConfigurations(topics))

      override def listConsumerGroups: G[Map[ConsumerGroup, ConsumerGroupState]] = fk(self.listConsumerGroups)

      override def describeConsumerGroups(
          groups: NonEmptySet[ConsumerGroup]
      ): G[NonEmptyMap[ConsumerGroup, Either[KafkaException, ConsumerGroupDescription]]] = fk(self.describeConsumerGroups(groups))

      override def listConsumerGroupOffsets(group: ConsumerGroup): G[Map[TopicPartition, OffsetAndMetadata]] =
        fk(self.listConsumerGroupOffsets(group))

      override def listConsumerGroupOffsets(
          group: ConsumerGroup,
          partitions: NonEmptySet[TopicPartition]
      ): G[NonEmptyMap[TopicPartition, Option[OffsetAndMetadata]]] = fk(self.listConsumerGroupOffsets(group, partitions))

      override def alterConsumerGroupOffsets(
          group: ConsumerGroup,
          offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]
      ): G[NonEmptyMap[TopicPartition, Either[KafkaException, Unit]]] = fk(self.alterConsumerGroupOffsets(group, offsets))

      override def deleteConsumerGroupOffsets(
          group: ConsumerGroup,
          partitions: NonEmptySet[TopicPartition]
      ): G[NonEmptyMap[TopicPartition, Either[KafkaException, Unit]]] = fk(self.deleteConsumerGroupOffsets(group, partitions))

      override def deleteConsumerGroups(groups: NonEmptySet[ConsumerGroup]): G[NonEmptyMap[ConsumerGroup, Either[KafkaException, Unit]]] =
        fk(self.deleteConsumerGroups(groups))

object KafkaAdminClient:
  private[xkafka] def validatePartitionCount(value: Int): Either[KafkaException.InvalidValue, Int] =
    if value > 0 then Right(value) else Left(new KafkaException.InvalidValue(ValidationError.NonPositivePartitionCount(value)))

  given FunctorK[KafkaAdminClient] with
    override def mapK[F[_], G[_]](client: KafkaAdminClient[F])(fk: FunctionK[F, G]): KafkaAdminClient[G] = client.mapK(fk)

/** An offset to commit, with what the broker stores beside it.
  *
  * @param leaderEpoch
  *   the leader epoch of the record the offset follows
  * @param metadata
  *   text stored with the offset. The broker keeps no difference between empty text and none, so empty means none.
  */
final case class OffsetAndMetadata(offset: Offset, leaderEpoch: Option[LeaderEpoch] = None, metadata: String = "")

trait OffsetCommitter[F[_]]:
  self =>

  /** Commits these offsets, each with its leader epoch and metadata. With assignment fencing on, the commit fails as `ErrorCode.IllegalGeneration`,
    * before it reaches the broker, where the consumer no longer owns one of the partitions.
    */
  def commitWithMetadata(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit]

  /** Commits these offsets alone, with no leader epoch and no metadata. */
  final def commit(offsets: Map[TopicPartition, Offset]): F[Unit] = commitWithMetadata(offsets.view.mapValues(OffsetAndMetadata(_)).toMap)

  /** Commits offsets together with the lease each was read under, which a consumer that fences its assignment checks before committing.
    *
    * `CommittableOffset.commit` and `CommittableOffsetBatch.commit` come through here. A committer that does not fence commits the offsets alone.
    */
  private[xkafka] def commitLeased(offsets: Map[TopicPartition, (OffsetAndMetadata, Option[Lease])]): F[Unit] =
    commitWithMetadata(offsets.view.mapValues(_._1).toMap)

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
    override def commitWithMetadata(offsets: Map[TopicPartition, OffsetAndMetadata]): G[Unit] = fk(underlying.commitWithMetadata(offsets))

    override private[xkafka] def commitLeased(offsets: Map[TopicPartition, (OffsetAndMetadata, Option[Lease])]): G[Unit] =
      fk(underlying.commitLeased(offsets))

    override private[xkafka] def membership: GroupMembership[G] = underlying.membership.mapK(fk)

trait CommittableOffset[F[_]]:
  self =>

  def topicPartition: TopicPartition

  /** The next offset to consume after this commit succeeds. */
  def nextOffset: Offset

  /** The leader epoch of the record this offset follows, where its backend reported one. */
  def leaderEpoch: Option[LeaderEpoch] = None

  /** The text committed with this offset, empty unless `withMetadata` set some. */
  def metadata: String = ""

  def committer: OffsetCommitter[F]

  /** What committing this offset sends: the next offset, the leader epoch and the metadata. */
  final def offsetAndMetadata: OffsetAndMetadata = OffsetAndMetadata(nextOffset, leaderEpoch, metadata)

  /** The same offset, committed with `value` as its metadata. */
  final def withMetadata(value: String): CommittableOffset[F] =
    new CommittableOffset[F]:
      override def topicPartition: TopicPartition = self.topicPartition

      override def nextOffset: Offset = self.nextOffset

      override def leaderEpoch: Option[LeaderEpoch] = self.leaderEpoch

      override def metadata: String = value

      override def committer: OffsetCommitter[F] = self.committer

      override private[xkafka] def membership: GroupMembership[F] = self.membership

      override private[xkafka] def lease: Option[Lease] = self.lease

  /** The group membership this offset was read under, which is what a transaction records it against.
    *
    * A backend that cannot name that membership falls back to its committer's, which is the consumer's current one.
    */
  private[xkafka] def membership: GroupMembership[F] = committer.membership

  /** The lease this offset was read under, where its consumer fences its assignment and could name one. */
  private[xkafka] def lease: Option[Lease] = None

  final def commit: F[Unit] = committer.commitLeased(Map(topicPartition -> (offsetAndMetadata, lease)))

  final def mapK[G[_]](fk: FunctionK[F, G]): CommittableOffset[G] =
    new CommittableOffset[G]:
      override def topicPartition: TopicPartition = self.topicPartition

      override def nextOffset: Offset = self.nextOffset

      override def leaderEpoch: Option[LeaderEpoch] = self.leaderEpoch

      override def metadata: String = self.metadata

      override def committer: OffsetCommitter[G] = self.committer.mapK(fk)

      override private[xkafka] def membership: GroupMembership[G] = self.membership.mapK(fk)

      override private[xkafka] def lease: Option[Lease] = self.lease

object CommittableOffset:
  given FunctorK[CommittableOffset] with
    override def mapK[F[_], G[_]](offset: CommittableOffset[F])(fk: FunctionK[F, G]): CommittableOffset[G] = offset.mapK(fk)

/** The offsets to commit, keeping the greatest one per topic-partition for each consumer. */
opaque type CommittableOffsetBatch[F[_]] = Map[OffsetCommitter[F], Map[TopicPartition, CommittableOffsetBatch.Entry[F]]]

object CommittableOffsetBatch:
  private[xkafka] final case class Entry[F[_]](offset: OffsetAndMetadata, membership: GroupMembership[F], lease: Option[Lease])

  def empty[F[_]]: CommittableOffsetBatch[F] = Map.empty

  def fromFoldable[F[_], G[_]: Foldable](offsets: G[CommittableOffset[F]]): CommittableOffsetBatch[F] =
    Foldable[G].foldLeft(offsets, empty[F])((batch, offset) => batch.updated(offset))

  extension [F[_]](batch: CommittableOffsetBatch[F])
    def updated(offset: CommittableOffset[F]): CommittableOffsetBatch[F] =
      batch.updated(
        offset.committer,
        include(batch.getOrElse(offset.committer, Map.empty), offset.topicPartition, Entry(offset.offsetAndMetadata, offset.membership, offset.lease))
      )

    def updated(other: CommittableOffsetBatch[F]): CommittableOffsetBatch[F] =
      other.foldLeft(batch):
        case (result, (committer, entries)) => result.updated(
            committer,
            entries.foldLeft(result.getOrElse(committer, Map.empty)):
              case (committerEntries, (topicPartition, entry)) => include(committerEntries, topicPartition, entry)
          )

    def offsets: Map[OffsetCommitter[F], Map[TopicPartition, Offset]] = batch.view.mapValues(_.view.mapValues(_.offset.offset).toMap).toMap

    /** The offsets with the leader epoch and metadata each is committed with. */
    def offsetsAndMetadata: Map[OffsetCommitter[F], Map[TopicPartition, OffsetAndMetadata]] =
      batch.view.mapValues(_.view.mapValues(_.offset).toMap).toMap

    /** The offsets grouped by the membership each was read under, which is how a transaction records them. */
    private[xkafka] def memberships: Map[GroupMembership[F], Map[TopicPartition, OffsetAndMetadata]] =
      batch.valuesIterator.flatten.toList.groupMap(_._2.membership)((topicPartition, entry) => topicPartition -> entry.offset).view.mapValues(_.toMap)
        .toMap

    def size: Int = batch.valuesIterator.map(_.size).sum

    def commit(using F: Applicative[F]): F[Unit] =
      batch.foldLeft(F.unit):
        case (result, (committer, entries)) => F
            .productR(result)(committer.commitLeased(entries.view.mapValues(entry => entry.offset -> entry.lease).toMap))

    def mapK[G[_]](fk: FunctionK[F, G]): CommittableOffsetBatch[G] =
      batch.map((committer, entries) =>
        committer.mapK(fk) -> entries.view.mapValues(entry => Entry(entry.offset, entry.membership.mapK(fk), entry.lease)).toMap
      )

  given FunctorK[CommittableOffsetBatch] with
    override def mapK[F[_], G[_]](batch: CommittableOffsetBatch[F])(fk: FunctionK[F, G]): CommittableOffsetBatch[G] = batch.mapK(fk)

  /** Keeps the higher offset. Where both are equal the incoming one wins, so its membership, usually the later one, replaces the earlier. */
  private def include[F[_]](entries: Map[TopicPartition, Entry[F]], topicPartition: TopicPartition, entry: Entry[F]): Map[TopicPartition, Entry[F]] =
    entries.updatedWith(topicPartition):
      case current @ Some(existing) if existing.offset.offset.value > entry.offset.offset.value => current
      case Some(_) | None                                                                       => Some(entry)

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

  /** Enqueues `records` and returns the effect that completes once the broker has acknowledged them.
    *
    * The outer effect completes as soon as the records are accepted for delivery, so several batches can be in flight at once, and fails where the
    * client refuses one of them as it is enqueued. Awaiting the inner effect immediately makes production fully synchronous. It fails where any
    * record was not delivered, with the failure of the first such record.
    */
  def produce(records: NonEmptyList[ProducerRecord[K, V]]): F[F[NonEmptyList[Produced[K, V]]]]

  /** Enqueues `records` and waits for the broker to acknowledge them. */
  final def produceAndAwait(records: NonEmptyList[ProducerRecord[K, V]])(using F: FlatMap[F]): F[NonEmptyList[Produced[K, V]]] =
    F.flatten(produce(records))

  /** Enqueues `records` and returns the effect that completes once each of them has been delivered or has failed.
    *
    * Each record is answered on its own, in the order given, so the records that were delivered are known when some fail. A record the client refuses
    * as it is enqueued fails with that refusal, as do the ones after it. A record that is not delivered fails neither effect. The outer effect still
    * fails, with nothing enqueued, where a record cannot be serialized.
    */
  def produceAttemptEach(records: NonEmptyList[ProducerRecord[K, V]]): F[F[NonEmptyList[Attempted[K, V]]]]

  /** Enqueues `records` and waits until each of them has been delivered or has failed. */
  final def produceAttemptEachAndAwait(records: NonEmptyList[ProducerRecord[K, V]])(using F: FlatMap[F]): F[NonEmptyList[Attempted[K, V]]] =
    F.flatten(produceAttemptEach(records))

  /** Sends every record enqueued before it without waiting out `linger`, and completes once each of them has been delivered or has failed.
    *
    * A record that fails does not fail the flush: its own `produce` or `produceAttemptEach` result reports it. Records enqueued while it waits are
    * not waited for. Cancelling it stops the wait, and the records stay enqueued.
    */
  def flush: F[Unit]

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
  final def pipe(maxInFlight: Int = 256)(using Concurrent[F]): Pipe[F, NonEmptyList[ProducerRecord[K, V]], NonEmptyList[Produced[K, V]]] =
    pipelined(maxInFlight)(produce)

  /** Produces each batch with `produceAttemptEach` and reports how each of its records fared, in the order the batches arrived.
    *
    * It pipelines batches the way `pipe` does, and carries on past records that fail.
    *
    * @param maxInFlight
    *   positive bound on the batches awaiting acknowledgement
    */
  final def pipeAttemptEach(maxInFlight: Int = 256)(using Concurrent[F]): Pipe[F, NonEmptyList[ProducerRecord[K, V]], NonEmptyList[Attempted[K, V]]] =
    pipelined(maxInFlight)(produceAttemptEach)

  private def pipelined[A](
      maxInFlight: Int
  )(enqueue: NonEmptyList[ProducerRecord[K, V]] => F[F[A]])(using Concurrent[F]): Pipe[F, NonEmptyList[ProducerRecord[K, V]], A] =
    if maxInFlight <= 0 then _ => Stream.raiseError(new IllegalArgumentException("maxInFlight must be positive"))
    else _.evalMap(enqueue).parEvalMap(maxInFlight)(identity)

  /** Transforms the effect. Translating the acknowledgement nested inside the enqueue needs a `Functor[G]`, so this is not a lawful `FunctorK`. */
  final def mapK[G[_]](fk: FunctionK[F, G])(using G: Functor[G]): KafkaProducer[G, K, V] =
    new KafkaProducer[G, K, V]:
      override def produce(records: NonEmptyList[ProducerRecord[K, V]]): G[G[NonEmptyList[Produced[K, V]]]] =
        G.map(fk(self.produce(records)))(fk.apply)

      override def produceAttemptEach(records: NonEmptyList[ProducerRecord[K, V]]): G[G[NonEmptyList[Attempted[K, V]]]] =
        G.map(fk(self.produceAttemptEach(records)))(fk.apply)

      override def flush: G[Unit] = fk(self.flush)

      override def partitionsFor(topic: Topic): G[Set[Partition]] = fk(self.partitionsFor(topic))

/** Produces records and records consumer offsets as one unit.
  *
  * Everything written through a transaction becomes visible together once it commits. A consumer reading `IsolationLevel.ReadCommitted` sees none of
  * it before then, and none of it at all where the transaction aborts.
  */
trait Transaction[F[_], K, V]:
  self =>

  /** Enqueues `records` and waits for the broker to acknowledge them, which a transaction must do before it can commit. */
  def produce(records: NonEmptyList[ProducerRecord[K, V]]): F[NonEmptyList[Produced[K, V]]]

  /** Records `batch` against the consumer group each of its offsets was read by, so those offsets are committed only if this transaction commits. */
  def commitOffsets(batch: CommittableOffsetBatch[F]): F[Unit]

  /** Transforms the effect. Carrying a batch back to the underlying transaction needs the reverse direction, so this is not a lawful `FunctorK`. */
  final def imapK[G[_]](fk: FunctionK[F, G])(gk: FunctionK[G, F]): Transaction[G, K, V] =
    new Transaction[G, K, V]:
      override def produce(records: NonEmptyList[ProducerRecord[K, V]]): G[NonEmptyList[Produced[K, V]]] = fk(self.produce(records))

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

/** What a `consumeChunk` handler returns once it has processed its chunk, after which the chunk's offsets are committed. */
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
    *   positive per-partition threshold at which the backend pauses fetching; records already in flight can temporarily exceed it
    */
  final def consumeChunk(process: Chunk[ConsumerRecord[K, V]] => F[CommitNow], maxQueuedRecords: Int = 256)(using F: Async[F]): F[Unit] =
    partitionedRecords(maxQueuedRecords).map(
      _.records.chunks.evalMap: chunk =>
        val (offsets, records) =
          chunk.mapAccumulate(CommittableOffsetBatch.empty[F])((batch, committable) => (batch.updated(committable.offset), committable.record))
        F.productR(process(records))(offsets.commit)
    ).parJoinUnbounded.compile.drain

  /** One stream for each partition this consumer is assigned, ending once the partition is revoked.
    *
    * Read the streams concurrently, such as with `parJoinUnbounded`: a stream does not end while its partition is held, and a partition whose stream
    * goes unread is paused once `maxQueuedRecords` of its records are waiting.
    *
    * @param maxQueuedRecords
    *   positive per-partition threshold at which the backend pauses fetching; records already in flight can temporarily exceed it
    */
  def partitionedRecords(maxQueuedRecords: Int = 256)(using Async[F]): Stream[F, PartitionRecords[F, K, V]] =
    PartitionRecords.fromConsumer(self, assignmentChanges, maxQueuedRecords, pausing)

  /** Returns the broker-stored next offset for each requested topic-partition, with its leader epoch and metadata, or `None` when no offset has been
    * committed.
    */
  def committed(topicPartitions: Set[TopicPartition]): F[Map[TopicPartition, Option[OffsetAndMetadata]]]

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

  /** Returns the offset this consumer reads next.
    *
    * It can be `None` until the consumer has consumed from the partition, and right after a seek the platforms can answer differently. Read it after
    * consuming from the partition for the same answer everywhere.
    */
  def position(topicPartition: TopicPartition): F[Option[Offset]]

  final def mapK[G[_]](fk: FunctionK[F, G]): KafkaConsumer[G, K, V] =
    new KafkaConsumer[G, K, V]:
      override val records: Stream[G, CommittableConsumerRecord[G, K, V]] = self.records.map(_.mapK(fk)).translate(fk)

      override def assignment: G[Set[TopicPartition]] = fk(self.assignment)

      override val assignmentChanges: Stream[G, Set[TopicPartition]] = self.assignmentChanges.translate(fk)

      override def committed(topicPartitions: Set[TopicPartition]): G[Map[TopicPartition, Option[OffsetAndMetadata]]] =
        fk(self.committed(topicPartitions))

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
