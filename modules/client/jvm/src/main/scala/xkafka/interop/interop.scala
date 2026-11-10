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
package interop

import cats.effect.Sync
import cats.syntax.all.*
import fs2.Chunk
import org.apache.kafka.common.header.{Header as JavaHeader, Headers as JavaHeaders}
import org.apache.kafka.common.header.internals.{RecordHeader, RecordHeaders}
import org.apache.kafka.common.serialization.{Deserializer as JavaDeserializer, Serde as JavaSerde, Serializer as JavaSerializer}

/** A Java deserializer that read a value as null, which a value of its type cannot be. */
final class JavaNullValueException(val topic: Topic)
    extends RuntimeException(
      s"the Java deserializer read a value of topic '${topic.value}' as null; read it with `option` where values can be absent"
    )

/** Makes serializers from Java's for the effect `F`, given the value type from what is passed. */
final class SerializerFromJava[F[_]] private[interop] ():
  /** `java`, called with each record's topic and a copy of its headers, in `F`. Headers it adds to that copy are not written. */
  def apply[A](java: JavaSerializer[A])(using F: Sync[F]): Serializer[F, A] =
    Serializer.instance((topic, headers, value) => F.delay(Option(java.serialize(topic.value, headers.toJava, value)).map(Chunk.array)))

/** Makes deserializers from Java's for the effect `F`, given the value type from what is passed. */
final class DeserializerFromJava[F[_]] private[interop] ():
  /** `java`, called with each record's topic and a copy of its headers, in `F`. A record with no value fails as with any deserializer, unless read
    * with `option`; a value `java` reads as null fails with [[JavaNullValueException]].
    */
  def apply[A](java: JavaDeserializer[A])(using F: Sync[F]): Deserializer[F, A] =
    Deserializer.instance: (topic, headers, bytes) =>
      bytes match
        case None       => F.raiseError(new Deserializer.NullValueException(topic))
        case Some(read) => F.delay(java.deserialize(topic.value, headers.toJava, read.toArray))
            .flatMap(value => Option(value).liftTo[F](new JavaNullValueException(topic)))

/** Makes streams serdes from Java's for the effect `F`, given the value type from what is passed. */
final class SerdeFromJava[F[_]] private[interop] ():
  /** `java`'s serializer and deserializer, as [[Serializer.fromJava]] and [[Deserializer.fromJava]] make them. */
  def apply[A](java: JavaSerde[A])(using F: Sync[F]): xkafka.Serde[F, A] =
    xkafka.Serde(SerializerFromJava[F]()(java.serializer), DeserializerFromJava[F]()(java.deserializer))

extension (companion: Serializer.type)
  /** A serializer from a Java one, as in `Serializer.fromJava[IO](new StringSerializer)`. The Java serializer must already be configured. */
  def fromJava[F[_]]: SerializerFromJava[F] = SerializerFromJava[F]()

extension (companion: Deserializer.type)
  /** A deserializer from a Java one, as in `Deserializer.fromJava[IO](new StringDeserializer)`. The Java deserializer must already be configured. */
  def fromJava[F[_]]: DeserializerFromJava[F] = DeserializerFromJava[F]()

extension (companion: xkafka.Serde.type)
  /** A streams serde from a Java one, as in `Serde.fromJava[IO](Serdes.String())`. The Java serde must already be configured. */
  def fromJava[F[_]]: SerdeFromJava[F] = SerdeFromJava[F]()

extension (companion: Headers.type)
  /** Java's headers as headers, in their order, a header with no value kept as one. */
  def fromJava(java: JavaHeaders): Headers =
    Headers.fromVector(java.toArray.toVector.map(header => Header(header.key, Option(header.value).map(Chunk.array))))

extension (headers: Headers)
  /** The headers as Java's, in their order, a header with no value kept as one. */
  def toJava: JavaHeaders =
    new RecordHeaders(headers.values.map(header => new RecordHeader(header.key, header.value.map(_.toArray).orNull): JavaHeader).toArray)
