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

import java.nio.ByteBuffer

import cats.ApplicativeThrow
import cats.arrow.FunctionK
import fs2.Chunk

/** How a streams program writes and reads one type, since a streams application both writes and reads its topics. */
final case class Serde[F[_], A](serializer: Serializer[F, A], deserializer: Deserializer[F, A]):
  def mapK[G[_]](fk: FunctionK[F, G]): Serde[G, A] = Serde(serializer.mapK(fk), deserializer.mapK(fk))

object Serde:
  /** Bytes as they are. */
  def bytes[F[_]: ApplicativeThrow]: Serde[F, Chunk[Byte]] = Serde(Serializer.bytes[F], Deserializer.bytes[F])

  /** Text as UTF-8, as Java's `Serdes.String()` writes it. */
  def utf8[F[_]: ApplicativeThrow]: Serde[F, String] = Serde(Serializer.utf8[F], Deserializer.utf8[F])

  /** A number as eight big-endian bytes, as Java's `Serdes.Long()` writes it. */
  def long[F[_]](using F: ApplicativeThrow[F]): Serde[F, Long] =
    val serializer   = Serializer.bytes[F].contramap[Long](value => Chunk.array(ByteBuffer.allocate(8).putLong(value).array))
    val deserializer =
      Deserializer.instance[F, Long]: (topic, _, value) =>
        value match
          case Some(read) if read.size == 8 => F.pure(ByteBuffer.wrap(read.toArray).getLong)
          case Some(read)                   => F
              .raiseError(new IllegalArgumentException(s"a long is 8 bytes, but a record of topic '${topic.value}' held ${read.size}"))
          case None => F.raiseError(new Deserializer.NullValueException(topic))
    Serde(serializer, deserializer)
