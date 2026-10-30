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

import cats.data.{NonEmptyList, Validated}
import munit.FunSuite

/** How streams settings are built and refused, on every platform. */
final class StreamsSettingsSuite extends FunSuite:
  private val client = ClientSettings.from(NonEmptyList.one("localhost:9092")).orFail
  private val app    = ApplicationId.from("app").orFail

  test("an application id takes the characters a topic name takes, up to its length"):
    assertEquals(ApplicationId.from("my.app_2-x").map(_.value), Right("my.app_2-x"))
    assertEquals(ApplicationId.from("a" * Topology.MaxNameLength).map(_.value.length), Right(Topology.MaxNameLength))
    assertEquals(ApplicationId.from(""), Left(StreamsSettingsError.EmptyApplicationId))
    assertEquals(ApplicationId.from("a" * (Topology.MaxNameLength + 1)), Left(StreamsSettingsError.ApplicationIdTooLong(Topology.MaxNameLength + 1)))
    List("my app", "app/1", "café", "app:1").foreach: id =>
      assertEquals(ApplicationId.from(id), Left(StreamsSettingsError.InvalidApplicationIdCharacters(id)))

  test("settings default to at least once at Java's interval, one task at a time, and a close timeout of 30 seconds"):
    val settings = StreamsSettings.from(client, app).orFail
    assertEquals(
      (settings.guarantee, settings.commitInterval, settings.concurrency, settings.closeTimeout),
      (ProcessingGuarantee.AtLeastOnce, None, 1, 30.seconds)
    )

  test("every invalid setting is reported at once"):
    assertEquals(
      StreamsSettings.from(client, app, commitInterval = Some(Duration.Zero), concurrency = 0, closeTimeout = -1.second),
      Validated.invalid(NonEmptyList.of(
        StreamsSettingsError.NonPositiveCommitInterval(Duration.Zero),
        StreamsSettingsError.NonPositiveConcurrency(0),
        StreamsSettingsError.NonPositiveCloseTimeout(-1.second)
      ))
    )

  test("each setting can be changed, and invalid changes are refused"):
    val settings = StreamsSettings.from(client, app).orFail
    val changed  =
      settings.withGuarantee(ProcessingGuarantee.ExactlyOnce).withApplicationId(ApplicationId.from("other").orFail).withCommitInterval(1.second)
        .andThen(_.withConcurrency(4)).andThen(_.withCloseTimeout(5.seconds)).orFail
    assertEquals(
      (changed.guarantee, changed.applicationId.value, changed.commitInterval, changed.concurrency, changed.closeTimeout),
      (ProcessingGuarantee.ExactlyOnce, "other", Some(1.second), 4, 5.seconds)
    )
    assertEquals(changed.withoutCommitInterval.commitInterval, None)
    assertEquals(settings.withConcurrency(-2), Validated.invalidNel(StreamsSettingsError.NonPositiveConcurrency(-2)))
    assertEquals(settings.withCommitInterval(-1.second), Validated.invalidNel(StreamsSettingsError.NonPositiveCommitInterval(-1.second)))
    assertEquals(settings.withCloseTimeout(Duration.Zero), Validated.invalidNel(StreamsSettingsError.NonPositiveCloseTimeout(Duration.Zero)))
