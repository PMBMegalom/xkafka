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

package xkafka.schemaregistry

import scala.concurrent.duration.FiniteDuration

import cats.Show
import cats.data.NonEmptyList
import cats.syntax.all.*

/** Where the registry is, and how long its answers are kept.
  *
  * Requests go to the first of `urls`, and to the next one when one cannot be reached. The latest version of a subject is kept for `latestCachedFor`,
  * or until the program ends where it is none, as Confluent's serializers keep it by default; every other answer the registry gives never changes, so
  * it is always kept.
  */
final case class RegistrySettings private (urls: NonEmptyList[String], latestCachedFor: Option[FiniteDuration]):
  /** Asks again for a subject's latest version once `duration` has passed since it was last asked, refused where it is negative. */
  def withLatestCachedFor(duration: FiniteDuration): Either[SchemaRegistryError, RegistrySettings] = RegistrySettings.from(urls, Some(duration))

  /** Keeps a subject's latest version until the program ends. */
  def withoutLatestExpiry: RegistrySettings = copy(latestCachedFor = None)

object RegistrySettings:
  /** The registry at `urls`, each an `http` or `https` URL, refused where one is not or where `latestCachedFor` is negative. */
  def from(urls: NonEmptyList[String], latestCachedFor: Option[FiniteDuration] = None): Either[SchemaRegistryError, RegistrySettings] =
    (urls.traverse(url), latestCachedFor.traverse(cached)).mapN(new RegistrySettings(_, _))

  private def url(value: String): Either[SchemaRegistryError, String] =
    val trimmed = value.reverse.dropWhile(_ == '/').reverse
    val host    = trimmed.dropWhile(_ != ':').drop(3)
    Either.cond((trimmed.startsWith("http://") || trimmed.startsWith("https://")) && host.nonEmpty, trimmed, SchemaRegistryError.InvalidUrl(value))

  private def cached(duration: FiniteDuration): Either[SchemaRegistryError, FiniteDuration] =
    Either.cond(duration.length >= 0, duration, SchemaRegistryError.NegativeLatestCacheTime(duration))

/** Why a registry setting or name was refused. */
enum SchemaRegistryError derives CanEqual:
  case InvalidUrl(value: String)
  case NegativeLatestCacheTime(value: FiniteDuration)
  case EmptySubject

  def message: String =
    this match
      case InvalidUrl(value)              => s"a registry URL must be an http or https URL with a host, but was '$value'"
      case NegativeLatestCacheTime(value) => s"the time a latest version is kept must not be negative, but was $value"
      case EmptySubject                   => "a subject must not be empty"

object SchemaRegistryError:
  given Show[SchemaRegistryError] = Show.show(_.message)
