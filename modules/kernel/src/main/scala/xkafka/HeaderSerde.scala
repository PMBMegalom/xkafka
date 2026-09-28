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

import java.nio.charset.StandardCharsets

import cats.{Contravariant, Functor}
import fs2.Chunk

/** Turns a value into the bytes a header carries.
  *
  * Header codecs are plain functions rather than effects, because a header value is already in memory by the time one is applied.
  */
trait HeaderSerializer[-A]:
  self =>

  def serialize(value: A): Chunk[Byte]

  final def contramap[B](f: B => A): HeaderSerializer[B] = HeaderSerializer.instance(value => self.serialize(f(value)))

object HeaderSerializer:
  given Contravariant[HeaderSerializer] with
    override def contramap[A, B](serializer: HeaderSerializer[A])(f: B => A): HeaderSerializer[B] = serializer.contramap(f)

  def apply[A](using serializer: HeaderSerializer[A]): HeaderSerializer[A] = serializer

  def instance[A](f: A => Chunk[Byte]): HeaderSerializer[A] =
    new HeaderSerializer[A]:
      override def serialize(value: A): Chunk[Byte] = f(value)

  given bytes: HeaderSerializer[Chunk[Byte]] = instance(identity)

  /** UTF-8, which is the encoding a header value is read back as on every platform. Leaving the charset to the runtime would not be. */
  given utf8: HeaderSerializer[String] = instance(value => Chunk.array(value.getBytes(StandardCharsets.UTF_8)))

  /** Four bytes, most significant first, which is the order Kafka's own clients write and other languages expect. */
  given int: HeaderSerializer[Int] = instance(value => Chunk.array(Array.tabulate(4)(index => (value >>> ((3 - index) * 8)).toByte)))

  /** Eight bytes, most significant first. */
  given long: HeaderSerializer[Long] = instance(value => Chunk.array(Array.tabulate(8)(index => (value >>> ((7 - index) * 8)).toByte)))

/** Reads the bytes a header carries. */
trait HeaderDeserializer[A]:
  self =>

  def deserialize(value: Option[Chunk[Byte]]): Either[ValidationError, A]

  final def map[B](f: A => B): HeaderDeserializer[B] = HeaderDeserializer.instance(value => self.deserialize(value).map(f))

  /** Reads and then rejects, for a value the bytes can carry but the type cannot. */
  final def emap[B](f: A => Either[ValidationError, B]): HeaderDeserializer[B] =
    HeaderDeserializer.instance(value => self.deserialize(value).flatMap(f))

object HeaderDeserializer:
  given Functor[HeaderDeserializer] with
    override def map[A, B](deserializer: HeaderDeserializer[A])(f: A => B): HeaderDeserializer[B] = deserializer.map(f)

  def apply[A](using deserializer: HeaderDeserializer[A]): HeaderDeserializer[A] = deserializer

  def instance[A](f: Option[Chunk[Byte]] => Either[ValidationError, A]): HeaderDeserializer[A] =
    new HeaderDeserializer[A]:
      override def deserialize(value: Option[Chunk[Byte]]): Either[ValidationError, A] = f(value)

  /** A header with no value at all, which Kafka allows, is a failure for every type below. */
  given bytes: HeaderDeserializer[Chunk[Byte]] = instance(_.toRight(ValidationError.MissingHeaderValue))

  given utf8: HeaderDeserializer[String] = bytes.map(value => new String(value.toArray, StandardCharsets.UTF_8))

  given int: HeaderDeserializer[Int] = fixedWidth(4).map(_.foldLeft(0)((result, byte) => (result << 8) | (byte & 0xff)))

  given long: HeaderDeserializer[Long] = fixedWidth(8).map(_.foldLeft(0L)((result, byte) => (result << 8) | (byte & 0xffL)))

  private def fixedWidth(width: Int): HeaderDeserializer[Array[Byte]] =
    bytes.emap: value =>
      val array = value.toArray
      if array.length == width then Right(array) else Left(ValidationError.InvalidHeaderLength(width, array.length))
