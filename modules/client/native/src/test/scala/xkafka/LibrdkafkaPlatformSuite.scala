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
  test("loads librdkafka through the native shim"):
    assert(LibrdkafkaPlatform.version.nonEmpty)

  test("allocates and releases a native producer"):
    val serializer = Serializer.const[IO, String](Some(Chunk.array(Array.emptyByteArray)))
    val client     = ClientSettings.from(NonEmptyList.one("localhost:9092"), Some("native-test")).toOption.get
    val settings   = ProducerSettings.from(client, serializer, serializer).toOption.get

    KafkaClient[IO].producer(settings).use(_ => IO.unit)

  test("a consumer refuses work once its resource has closed"):
    val deserializer = Deserializer.utf8[IO]
    val client       = ClientSettings.from(NonEmptyList.one("localhost:9092"), Some("native-test")).toOption.get
    val group        = ConsumerGroup.from("native-lifecycle").toOption.get
    val settings     = ConsumerSettings.from(client, group, deserializer, deserializer).toOption.get
    val topic        = Topic.from("events").toOption.get

    // Leaking the consumer past its resource used to reach a destroyed handle.
    KafkaClient[IO].consumer(settings, Subscription.Topics(NonEmptyList.one(topic))).use(IO.pure).flatMap: escaped =>
      interceptIO[IllegalStateException](escaped.assignment)

  test("a producer refuses work once its resource has closed"):
    val serializer = Serializer.const[IO, String](Some(Chunk.array(Array.emptyByteArray)))
    val client     = ClientSettings.from(NonEmptyList.one("localhost:9092"), Some("native-test")).toOption.get
    val settings   = ProducerSettings.from(client, serializer, serializer).toOption.get
    val topic      = Topic.from("events").toOption.get

    KafkaClient[IO].producer(settings).use(IO.pure).flatMap: escaped =>
      interceptIO[IllegalStateException](escaped.produce(NonEmptyList.one(ProducerRecord(topic, "key", "value"))))

  test("passes custom producer properties to librdkafka"):
    val serializer = Serializer.const[IO, String](None)
    val client     = ClientSettings.from(NonEmptyList.one("localhost:9092")).toOption.get
    val settings   = ProducerSettings.from(client, serializer, serializer, Map("message.timeout.ms" -> "not-a-duration")).toOption.get

    interceptIO[KafkaException.BackendFailure](KafkaClient[IO].producer(settings).use(_ => IO.unit))
