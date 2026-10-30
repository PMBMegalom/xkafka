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

import cats.Show
import cats.data.ValidatedNel
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

  def message: String =
    this match
      case EmptyApplicationId                    => "application id must not be empty"
      case ApplicationIdTooLong(length)          => s"application id must be at most ${Topology.MaxNameLength} characters, was $length"
      case InvalidApplicationIdCharacters(value) => s"application id '$value' must contain only [a-zA-Z0-9._-]"
      case NonPositiveCommitInterval(value)      => s"commit interval must be positive, was $value"
      case NonPositiveConcurrency(value)         => s"concurrency must be positive, was $value"
      case NonPositiveCloseTimeout(value)        => s"close timeout must be positive, was $value"

object StreamsSettingsError:
  given Show[StreamsSettingsError] = Show.show(_.message)

/** How a streams application connects, names itself and runs.
  *
  * @param commitInterval
  *   how often tasks commit; by default Java Streams' interval for the guarantee, 30 seconds at least once and 100 ms exactly once
  * @param concurrency
  *   how many tasks process records at once, each its own records in order
  * @param closeTimeout
  *   how long releasing the application waits for its tasks to be given back and for the group to answer that it left
  */
sealed abstract case class StreamsSettings private (
    client: ClientSettings,
    applicationId: ApplicationId,
    guarantee: ProcessingGuarantee,
    commitInterval: Option[FiniteDuration],
    concurrency: Int,
    closeTimeout: FiniteDuration
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

  private def copy(
      client: ClientSettings = client,
      applicationId: ApplicationId = applicationId,
      guarantee: ProcessingGuarantee = guarantee,
      commitInterval: Option[FiniteDuration] = commitInterval,
      concurrency: Int = concurrency,
      closeTimeout: FiniteDuration = closeTimeout
  ): StreamsSettings = new StreamsSettings(client, applicationId, guarantee, commitInterval, concurrency, closeTimeout) {}

object StreamsSettings:
  val DefaultCloseTimeout: FiniteDuration = 30.seconds

  def from(
      client: ClientSettings,
      applicationId: ApplicationId,
      guarantee: ProcessingGuarantee = ProcessingGuarantee.AtLeastOnce,
      commitInterval: Option[FiniteDuration] = None,
      concurrency: Int = 1,
      closeTimeout: FiniteDuration = DefaultCloseTimeout
  ): ValidatedNel[StreamsSettingsError, StreamsSettings] =
    (
      commitInterval.traverse(positive(_)(StreamsSettingsError.NonPositiveCommitInterval(_))),
      atLeastOne(concurrency),
      positive(closeTimeout)(StreamsSettingsError.NonPositiveCloseTimeout(_))
    ).mapN((interval, tasks, timeout) => new StreamsSettings(client, applicationId, guarantee, interval, tasks, timeout) {})

  private def positive(value: FiniteDuration)(error: FiniteDuration => StreamsSettingsError): ValidatedNel[StreamsSettingsError, FiniteDuration] =
    if value > Duration.Zero then value.validNel else error(value).invalidNel

  private def atLeastOne(value: Int): ValidatedNel[StreamsSettingsError, Int] =
    if value > 0 then value.validNel else StreamsSettingsError.NonPositiveConcurrency(value).invalidNel
