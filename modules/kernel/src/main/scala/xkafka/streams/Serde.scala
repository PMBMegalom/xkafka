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

import scala.concurrent.duration.FiniteDuration

import cats.{ApplicativeThrow, MonadThrow}
import cats.arrow.FunctionK
import cats.syntax.all.*
import fs2.Chunk

/** How a streams program writes and reads one type, since a streams application both writes and reads its topics. */
final case class Serde[F[_], A](serializer: Serializer[F, A], deserializer: Deserializer[F, A]):
  def mapK[G[_]](fk: FunctionK[F, G]): Serde[G, A] = Serde(serializer.mapK(fk), deserializer.mapK(fk))

  /** Values that may be absent, written as no value at all, as a tombstone. */
  def option(using F: cats.Applicative[F]): Serde[F, Option[A]] = Serde(serializer.option, deserializer.option)

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

  /** A windowed key as Java's `TimeWindowedSerializer` writes it: the key as `key` writes it, then its window's start as eight big-endian bytes.
    * Reading it back takes the windows' `size`, from which the window's end follows, as Java's deserializer is told the size.
    */
  def timeWindowed[F[_], K](key: Serde[F, K], size: FiniteDuration)(using F: MonadThrow[F]): Serde[F, Windowed[K]] =
    val serializer =
      Serializer.instance[F, Windowed[K]]: (topic, headers, windowed) =>
        key.serializer.serialize(topic, headers, windowed.key).flatMap:
          case Some(written) => F.pure(Some(written ++ Chunk.array(ByteBuffer.allocate(8).putLong(windowed.window.start).array)))
          case None => F.raiseError(new IllegalArgumentException(s"a windowed key for topic '${topic.value}' has a key its serializer wrote as none"))
    val deserializer =
      Deserializer.instance[F, Windowed[K]]: (topic, headers, bytes) =>
        bytes match
          case Some(read) if read.size >= 8 =>
            val start = ByteBuffer.wrap(read.drop(read.size - 8).toArray).getLong
            val end   = start + size.toMillis
            // Java truncates an end past the largest timestamp to it.
            key.deserializer.deserialize(topic, headers, Some(read.take(read.size - 8)))
              .map(Windowed(_, Window(start, if end < 0 then Long.MaxValue else end)))
          case Some(read) => F.raiseError(new IllegalArgumentException(s"a windowed key is at least 8 bytes, but a record of topic '${topic
                .value}' held ${read.size}"))
          case None => F.raiseError(new IllegalArgumentException(s"a record of topic '${topic.value}' has no windowed key"))
    Serde(serializer, deserializer)

  /** A session's key as Java's `SessionWindowedSerializer` writes it: the key as `key` writes it, then the session's end and its start, each as eight
    * big-endian bytes.
    */
  def sessionWindowed[F[_], K](key: Serde[F, K])(using F: MonadThrow[F]): Serde[F, Windowed[K]] =
    val serializer =
      Serializer.instance[F, Windowed[K]]: (topic, headers, windowed) =>
        key.serializer.serialize(topic, headers, windowed.key).flatMap:
          case Some(written) => F
              .pure(Some(written ++ Chunk.array(ByteBuffer.allocate(16).putLong(windowed.window.end).putLong(windowed.window.start).array)))
          case None => F.raiseError(new IllegalArgumentException(s"a session key for topic '${topic.value}' has a key its serializer wrote as none"))
    val deserializer =
      Deserializer.instance[F, Windowed[K]]: (topic, headers, bytes) =>
        bytes match
          case Some(read) if read.size >= 16 =>
            val times = ByteBuffer.wrap(read.drop(read.size - 16).toArray)
            val end   = times.getLong
            key.deserializer.deserialize(topic, headers, Some(read.take(read.size - 16))).map(Windowed(_, Window(times.getLong, end)))
          case Some(read) => F.raiseError(new IllegalArgumentException(s"a session key is at least 16 bytes, but a record of topic '${topic
                .value}' held ${read.size}"))
          case None => F.raiseError(new IllegalArgumentException(s"a record of topic '${topic.value}' has no session key"))
    Serde(serializer, deserializer)
