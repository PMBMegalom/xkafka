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
package internal

import scala.concurrent.duration.*

import cats.data.NonEmptyList
import cats.effect.IO
import munit.FunSuite

/** What the JVM consumer is told, where its own defaults would undo a setting. */
final class ClientPropertiesSuite extends FunSuite:
  private val client = ClientSettings.from(NonEmptyList.one("localhost:9092")).toOption.get
  private val group  = ConsumerGroup.from("workers").toOption.get

  private def consumer(fetchWait: FiniteDuration, properties: Map[String, String] = Map.empty) =
    ClientProperties.consumer(
      ConsumerSettings.from(client, group, Deserializer.utf8[IO], Deserializer.utf8[IO], properties = properties, fetchWait = fetchWait).toOption.get
    )

  test("a fetch the broker may hold past the Java consumer's request timeout gets one longer than it, unless one is set"):
    assertEquals(consumer(500.millis).get("request.timeout.ms"), None)
    assertEquals(consumer(1.minute).get("request.timeout.ms"), Some("90000"))
    assertEquals(consumer(1.minute, Map("request.timeout.ms" -> "120000")).get("request.timeout.ms"), None)
    // As the streams engine's consumers ask for it.
    assertEquals(ClientProperties.fetchWait(1.minute).get("request.timeout.ms"), Some("90000"))
