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
    queryEndpoint: Option[Endpoint]
):
  def withClient(value: ClientSettings): StreamsSettings = copy(client = value)

  def withApplicationId(value: ApplicationId): StreamsSettings = copy(applicationId = value)

  def withGuarantee(value: ProcessingGuarantee): StreamsSettings = copy(guarantee = value)

  def withCommitInterval(value: FiniteDuration): ValidatedNel[StreamsSettingsError, StreamsSettings] =
    StreamsSettings.positive(value)(StreamsSettingsError.NonPositiveCommitInterval(_)).map(valid => copy(commitInterval = Some(valid)))

  /** Commits at Java Streams' interval for the guarantee. */
  def withoutCommitInterval: StreamsSettings = copy(commitInterval = None)

  def withConcurrency(value: Int): ValidatedNel[StreamsSettingsError, StreamsSettings] =
    StreamsSettings.atLeastOne(value).map(valid => copy(concurrency = valid))

  def withCloseTimeout(value: FiniteDuration): ValidatedNel[StreamsSettingsError, StreamsSettings] =
    StreamsSettings.positive(value)(StreamsSettingsError.NonPositiveCloseTimeout(_)).map(valid => copy(closeTimeout = valid))

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

  private def copy(
      client: ClientSettings = client,
      applicationId: ApplicationId = applicationId,
      guarantee: ProcessingGuarantee = guarantee,
      commitInterval: Option[FiniteDuration] = commitInterval,
      concurrency: Int = concurrency,
      closeTimeout: FiniteDuration = closeTimeout,
      topologyDescriptionPush: Boolean = topologyDescriptionPush,
      stateDirectory: Option[String] = stateDirectory,
      queryEndpoint: Option[Endpoint] = queryEndpoint
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
      queryEndpoint
    ) {}

object StreamsSettings:
  val DefaultCloseTimeout: FiniteDuration = 30.seconds

  def from(
      client: ClientSettings,
      applicationId: ApplicationId,
      guarantee: ProcessingGuarantee = ProcessingGuarantee.AtLeastOnce,
      commitInterval: Option[FiniteDuration] = None,
      concurrency: Int = 1,
      closeTimeout: FiniteDuration = DefaultCloseTimeout,
      topologyDescriptionPush: Boolean = true,
      stateDirectory: Option[String] = None,
      queryEndpoint: Option[Endpoint] = None
  ): ValidatedNel[StreamsSettingsError, StreamsSettings] =
    (
      commitInterval.traverse(positive(_)(StreamsSettingsError.NonPositiveCommitInterval(_))),
      atLeastOne(concurrency),
      positive(closeTimeout)(StreamsSettingsError.NonPositiveCloseTimeout(_)),
      stateDirectory.traverse(directory)
    ).mapN((interval, tasks, timeout, kept) =>
      new StreamsSettings(client, applicationId, guarantee, interval, tasks, timeout, topologyDescriptionPush, kept, queryEndpoint) {}
    )

  private def directory(value: String): ValidatedNel[StreamsSettingsError, String] =
    if value.trim.nonEmpty then value.validNel else StreamsSettingsError.EmptyStateDirectory.invalidNel

  private def positive(value: FiniteDuration)(error: FiniteDuration => StreamsSettingsError): ValidatedNel[StreamsSettingsError, FiniteDuration] =
    if value > Duration.Zero then value.validNel else error(value).invalidNel

  private def atLeastOne(value: Int): ValidatedNel[StreamsSettingsError, Int] =
    if value > 0 then value.validNel else StreamsSettingsError.NonPositiveConcurrency(value).invalidNel
