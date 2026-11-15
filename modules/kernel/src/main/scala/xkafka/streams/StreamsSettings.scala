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
package streams

import scala.concurrent.duration.*

import cats.{ApplicativeThrow, Show}
import cats.data.{NonEmptyList, ValidatedNel}
import cats.syntax.all.*

/** Names a streams application: its group, and the start of each internal topic's name. It takes the characters a topic name takes. */
opaque type ApplicationId = String

object ApplicationId:
  def from(value: String): Either[StreamsSettingsError, ApplicationId] =
    if value.isEmpty then Left(StreamsSettingsError.EmptyApplicationId)
    // The names of a directory and its parent, which a state directory would take for the application's own.
    else if value == "." || value == ".." then Left(StreamsSettingsError.ReservedApplicationId(value))
    else if value.length > Topology.MaxNameLength then Left(StreamsSettingsError.ApplicationIdTooLong(value.length))
    else if !value.forall(char => char.isLetterOrDigit && char < 128 || char == '.' || char == '_' || char == '-') then
      Left(StreamsSettingsError.InvalidApplicationIdCharacters(value))
    else Right(value)

  extension (id: ApplicationId) def value: String = id

  given Show[ApplicationId] = Show.show(_.value)

/** How a streams application commits what its tasks read, as Java Streams' `processing.guarantee` names them. */
enum ProcessingGuarantee derives CanEqual:
  /** After writing what the tasks wrote, so a task taken over reads again what it read since its last commit, and writes it again. */
  case AtLeastOnce

  /** In the transaction that writes what the tasks wrote, so a task taken over finds what it wrote only together with where it read to. An effectful
    * step runs again for the records read again.
    */
  case ExactlyOnce

/** Why streams settings could not be built. */
enum StreamsSettingsError derives CanEqual:
  case EmptyApplicationId
  case ApplicationIdTooLong(length: Int)
  case InvalidApplicationIdCharacters(value: String)
  case NonPositiveCommitInterval(value: FiniteDuration)
  case NonPositiveConcurrency(value: Int)
  case NonPositiveCloseTimeout(value: FiniteDuration)
  case EmptyStateDirectory
  case InvalidEndpoint(host: String, port: Int)
  case FetchWaitOutOfRange(value: FiniteDuration)
  case PartialMilliseconds(setting: String, value: FiniteDuration)
  case ReservedApplicationId(value: String)
  case NegativeTaskTimeout(value: FiniteDuration)
  case CommitIntervalAboveTransactionTimeout(value: FiniteDuration, transactionTimeout: FiniteDuration)

  def message: String =
    this match
      case EmptyApplicationId                    => "application id must not be empty"
      case ApplicationIdTooLong(length)          => s"application id must be at most ${Topology.MaxNameLength} characters, was $length"
      case InvalidApplicationIdCharacters(value) => s"application id '$value' must contain only [a-zA-Z0-9._-]"
      case NonPositiveCommitInterval(value)      => s"commit interval must be positive, was $value"
      case NonPositiveConcurrency(value)         => s"concurrency must be positive, was $value"
      case NonPositiveCloseTimeout(value)        => s"close timeout must be positive, was $value"
      case EmptyStateDirectory                   => "state directory must not be empty"
      case InvalidEndpoint(host, port)           => s"an endpoint needs a host and a port from 1 to 65535, was '$host' and $port"
      case FetchWaitOutOfRange(value)            => s"fetch wait must be from 0 to ${ConsumerSettings.FetchWaitLimit}, was $value"
      case PartialMilliseconds(setting, value)   => s"$setting must be whole milliseconds, was $value"
      case ReservedApplicationId(value)          => s"application id must not be '$value'"
      case NegativeTaskTimeout(value)            => s"task timeout must not be negative, was $value"
      case CommitIntervalAboveTransactionTimeout(value, timeout) =>
        s"commit interval must not exceed the transaction timeout of $timeout under exactly-once, was $value"

object StreamsSettingsError:
  given Show[StreamsSettingsError] = Show.show(_.message)

  /** Carries the reason a value was rejected into `F`. */
  extension [A](result: Either[StreamsSettingsError, A])
    def liftTo[F[_]](using F: ApplicativeThrow[F]): F[A] = F.fromEither(result.left.map(error => InvalidStreamsSettings(NonEmptyList.one(error))))

  /** Carries every reason settings were rejected into `F`. */
  extension [A](result: ValidatedNel[StreamsSettingsError, A])
    def liftTo[F[_]](using F: ApplicativeThrow[F]): F[A] = F.fromEither(result.toEither.left.map(InvalidStreamsSettings(_)))

/** Streams settings or an application id that could not be built, with every reason they could not. */
final class InvalidStreamsSettings(val errors: NonEmptyList[StreamsSettingsError])
    extends RuntimeException(errors.toList.map(_.message).mkString("; "))

/** How a streams application connects, names itself and runs.
  *
  * @param commitInterval
  *   how often tasks commit; by default Java Streams' interval for the guarantee, 30 seconds at least once and 100 ms exactly once
  * @param concurrency
  *   how many tasks process records at once, each its own records in order
  * @param closeTimeout
  *   how long releasing the application waits for its tasks to be given back and for the group to answer that it left
  * @param topologyDescriptionPush
  *   whether a member sends the program's description to the group's coordinator when it asks for one, as Java Streams does by default
  * @param stateDirectory
  *   where the application keeps its stores on disk, as Java Streams' `state.dir`; with none, every store is kept in memory and filled again from its
  *   changelog each time the application starts
  * @param queryEndpoint
  *   where this member answers the queries other members forward to it, as Java Streams' `application.server`; with none, other members cannot ask
  *   this one, and it answers only what its own stores hold
  * @param fetchWait
  *   the longest the broker holds a fetch of the application's consumers while fewer than `fetch.min.bytes` of records wait, as `ConsumerSettings`'
  *   `fetchWait` does for a consumer
  * @param taskTimeout
  *   how long the application asks the group's coordinator again while it is not ready to read or commit the group's offsets, while it does not know
  *   the member yet as the member joins again, or while a request to it fails in a way worth retrying, before it stops with the last failure, as Java
  *   Streams' `task.timeout.ms`; with zero, it stops at the first
  */
sealed abstract case class StreamsSettings private (
    client: ClientSettings,
    applicationId: ApplicationId,
    guarantee: ProcessingGuarantee,
    commitInterval: Option[FiniteDuration],
    concurrency: Int,
    closeTimeout: FiniteDuration,
    topologyDescriptionPush: Boolean,
    stateDirectory: Option[String],
    queryEndpoint: Option[Endpoint],
    fetchWait: FiniteDuration,
    taskTimeout: FiniteDuration
):
  def withClient(value: ClientSettings): StreamsSettings = copy(client = value)

  def withApplicationId(value: ApplicationId): StreamsSettings = copy(applicationId = value)

  /** The guarantee, which exactly-once refuses with a commit interval past its transaction timeout. */
  def withGuarantee(value: ProcessingGuarantee): ValidatedNel[StreamsSettingsError, StreamsSettings] =
    StreamsSettings.committing(value, commitInterval).map(_ => copy(guarantee = value))

  def withCommitInterval(value: FiniteDuration): ValidatedNel[StreamsSettingsError, StreamsSettings] =
    StreamsSettings.positive(value, "commit interval")(StreamsSettingsError.NonPositiveCommitInterval(_))
      .andThen(valid => StreamsSettings.committing(guarantee, Some(valid)).map(_ => copy(commitInterval = Some(valid))))

  /** Commits at Java Streams' interval for the guarantee. */
  def withoutCommitInterval: StreamsSettings = copy(commitInterval = None)

  def withConcurrency(value: Int): ValidatedNel[StreamsSettingsError, StreamsSettings] =
    StreamsSettings.atLeastOne(value).map(valid => copy(concurrency = valid))

  def withCloseTimeout(value: FiniteDuration): ValidatedNel[StreamsSettingsError, StreamsSettings] =
    StreamsSettings.positive(value, "close timeout")(StreamsSettingsError.NonPositiveCloseTimeout(_)).map(valid => copy(closeTimeout = valid))

  /** Sends the program's description to the group's coordinator when it asks for one. */
  def withTopologyDescriptionPush: StreamsSettings = copy(topologyDescriptionPush = true)

  /** Sends no description of the program, so describing the group shows none. */
  def withoutTopologyDescriptionPush: StreamsSettings = copy(topologyDescriptionPush = false)

  /** Keeps the application's stores on disk under `value`. */
  def withStateDirectory(value: String): ValidatedNel[StreamsSettingsError, StreamsSettings] =
    StreamsSettings.directory(value).map(valid => copy(stateDirectory = Some(valid)))

  /** Keeps every store in memory. */
  def withoutStateDirectory: StreamsSettings = copy(stateDirectory = None)

  /** Answers the queries other members forward to `value`, which the group tells them. */
  def withQueries(value: Endpoint): StreamsSettings = copy(queryEndpoint = Some(value))

  /** Answers no queries from other members. */
  def withoutQueries: StreamsSettings = copy(queryEndpoint = None)

  /** The longest the broker holds a fetch of the application's consumers, from zero up to `ConsumerSettings.FetchWaitLimit`, as `ConsumerSettings`'
    * `withFetchWait`.
    */
  def withFetchWait(value: FiniteDuration): ValidatedNel[StreamsSettingsError, StreamsSettings] =
    StreamsSettings.fetchWaitOf(value).map(valid => copy(fetchWait = valid))

  /** How long the application keeps asking the group's coordinator while it is not ready, does not know the member yet, or while asking fails in a
    * way worth retrying, and keeps starting its tasks again after a failure worth retrying, before it stops with the last failure; with zero, it
    * stops at the first.
    */
  def withTaskTimeout(value: FiniteDuration): ValidatedNel[StreamsSettingsError, StreamsSettings] =
    StreamsSettings.taskTimeoutOf(value).map(valid => copy(taskTimeout = valid))

  private def copy(
      client: ClientSettings = client,
      applicationId: ApplicationId = applicationId,
      guarantee: ProcessingGuarantee = guarantee,
      commitInterval: Option[FiniteDuration] = commitInterval,
      concurrency: Int = concurrency,
      closeTimeout: FiniteDuration = closeTimeout,
      topologyDescriptionPush: Boolean = topologyDescriptionPush,
      stateDirectory: Option[String] = stateDirectory,
      queryEndpoint: Option[Endpoint] = queryEndpoint,
      fetchWait: FiniteDuration = fetchWait,
      taskTimeout: FiniteDuration = taskTimeout
  ): StreamsSettings =
    new StreamsSettings(
      client,
      applicationId,
      guarantee,
      commitInterval,
      concurrency,
      closeTimeout,
      topologyDescriptionPush,
      stateDirectory,
      queryEndpoint,
      fetchWait,
      taskTimeout
    ) {}

object StreamsSettings:
  val DefaultCloseTimeout: FiniteDuration = 30.seconds

  /** Java Streams' default `task.timeout.ms`. */
  val DefaultTaskTimeout: FiniteDuration = 5.minutes

  def from(
      client: ClientSettings,
      applicationId: ApplicationId,
      guarantee: ProcessingGuarantee = ProcessingGuarantee.AtLeastOnce,
      commitInterval: Option[FiniteDuration] = None,
      concurrency: Int = 1,
      closeTimeout: FiniteDuration = DefaultCloseTimeout,
      topologyDescriptionPush: Boolean = true,
      stateDirectory: Option[String] = None,
      queryEndpoint: Option[Endpoint] = None,
      fetchWait: FiniteDuration = ConsumerSettings.DefaultFetchWait,
      taskTimeout: FiniteDuration = DefaultTaskTimeout
  ): ValidatedNel[StreamsSettingsError, StreamsSettings] =
    (
      commitInterval.traverse(positive(_, "commit interval")(StreamsSettingsError.NonPositiveCommitInterval(_))),
      atLeastOne(concurrency),
      positive(closeTimeout, "close timeout")(StreamsSettingsError.NonPositiveCloseTimeout(_)),
      stateDirectory.traverse(directory),
      fetchWaitOf(fetchWait),
      taskTimeoutOf(taskTimeout)
    ).mapN((interval, tasks, timeout, kept, wait, stall) =>
      new StreamsSettings(client, applicationId, guarantee, interval, tasks, timeout, topologyDescriptionPush, kept, queryEndpoint, wait, stall) {}
    ).andThen(settings => committing(guarantee, settings.commitInterval).map(_ => settings))

  /** The transaction timeout of an exactly-once application's producers, Java Streams' `transaction.timeout.ms`. */
  val TransactionTimeout: FiniteDuration = 10.seconds

  /** Exactly-once keeps a transaction open until it commits, so the broker would abort every one that waits past its timeout, as Java refuses. */
  private def committing(guarantee: ProcessingGuarantee, interval: Option[FiniteDuration]): ValidatedNel[StreamsSettingsError, Unit] =
    interval.filter(value => guarantee == ProcessingGuarantee.ExactlyOnce && value > TransactionTimeout) match
      case Some(value) => StreamsSettingsError.CommitIntervalAboveTransactionTimeout(value, TransactionTimeout).invalidNel
      case None        => ().validNel

  // 0.7.0's signature, before the fetch wait, which code compiled against 0.7.0 calls.
  private[streams] def from(
      client: ClientSettings,
      applicationId: ApplicationId,
      guarantee: ProcessingGuarantee,
      commitInterval: Option[FiniteDuration],
      concurrency: Int,
      closeTimeout: FiniteDuration,
      topologyDescriptionPush: Boolean,
      stateDirectory: Option[String],
      queryEndpoint: Option[Endpoint]
  ): ValidatedNel[StreamsSettingsError, StreamsSettings] =
    from(
      client,
      applicationId,
      guarantee,
      commitInterval,
      concurrency,
      closeTimeout,
      topologyDescriptionPush,
      stateDirectory,
      queryEndpoint,
      ConsumerSettings.DefaultFetchWait,
      DefaultTaskTimeout
    )

  private def directory(value: String): ValidatedNel[StreamsSettingsError, String] =
    if value.trim.nonEmpty then value.validNel else StreamsSettingsError.EmptyStateDirectory.invalidNel

  private def positive(value: FiniteDuration, setting: String)(
      error: FiniteDuration => StreamsSettingsError
  ): ValidatedNel[StreamsSettingsError, FiniteDuration] = if value <= Duration.Zero then error(value).invalidNel else whole(value, setting)

  private def fetchWaitOf(value: FiniteDuration): ValidatedNel[StreamsSettingsError, FiniteDuration] =
    if value >= Duration.Zero && value <= ConsumerSettings.FetchWaitLimit then whole(value, "fetch wait")
    else StreamsSettingsError.FetchWaitOutOfRange(value).invalidNel

  private def taskTimeoutOf(value: FiniteDuration): ValidatedNel[StreamsSettingsError, FiniteDuration] =
    if value >= Duration.Zero then whole(value, "task timeout") else StreamsSettingsError.NegativeTaskTimeout(value).invalidNel

  /** The clients count durations in milliseconds, so a part of one would be dropped, and a duration under one would become none. */
  private def whole(value: FiniteDuration, setting: String): ValidatedNel[StreamsSettingsError, FiniteDuration] =
    if value.toNanos % 1000000L == 0L then value.validNel else StreamsSettingsError.PartialMilliseconds(setting, value).invalidNel

  private def atLeastOne(value: Int): ValidatedNel[StreamsSettingsError, Int] =
    if value > 0 then value.validNel else StreamsSettingsError.NonPositiveConcurrency(value).invalidNel
