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

import cats.data.NonEmptyList
import cats.effect.IO
import fs2.Chunk
import munit.CatsEffectSuite

final class LibrdkafkaPlatformSuite extends CatsEffectSuite:
  test("loads librdkafka through the native shim") {
    assert(LibrdkafkaPlatform.version.nonEmpty)
  }

  test("allocates and releases a native producer") {
    val serializer = Serializer.const[IO, String](
      Some(Chunk.array(Array.emptyByteArray))
    )
    val settings = ProducerSettings(
      ClientSettings(NonEmptyList.one("localhost:9092"), Some("native-test")),
      serializer,
      serializer
    )

    KafkaClient[IO].producer(settings).use(_ => IO.unit)
  }

  test("passes custom producer properties to librdkafka") {
    val serializer = Serializer.const[IO, String](None)
    val settings   = ProducerSettings(
      ClientSettings(NonEmptyList.one("localhost:9092")),
      serializer,
      serializer,
      Map("message.timeout.ms" -> "not-a-duration")
    )

    interceptIO[RuntimeException](KafkaClient[IO].producer(settings).use(_ => IO.unit))
  }
