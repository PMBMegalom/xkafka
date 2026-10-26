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
package internal.protocol

import java.nio.charset.StandardCharsets.UTF_8

import cats.data.StateT
import cats.syntax.all.*
import scodec.bits.ByteVector

/** The primitive types of the Kafka protocol, written as bytes and read back from them.
  *
  * A read takes what it needs from the front of the bytes and leaves the rest, or fails with what was wrong.
  */
private[xkafka] object Wire:
  type Result[A] = Either[String, A]
  type Read[A]   = StateT[Result, ByteVector, A]

  def int8(value: Int): ByteVector   = ByteVector.fromByte(value.toByte)
  def int16(value: Int): ByteVector  = ByteVector.fromShort(value.toShort)
  def int32(value: Int): ByteVector  = ByteVector.fromInt(value)
  def int64(value: Long): ByteVector = ByteVector.fromLong(value)

  /** Seven bits to a byte, lowest first, with the high bit set on every byte but the last. */
  def unsignedVarint(value: Int): ByteVector =
    if (value & ~0x7f) == 0 then int8(value) else int8((value & 0x7f) | 0x80) ++ unsignedVarint(value >>> 7)

  def string(value: String): ByteVector                                  = sized(utf8(value), int16)
  def nullableString(value: Option[String]): ByteVector                  = value.fold(int16(-1))(string)
  def compactString(value: String): ByteVector                           = compact(utf8(value))
  def compactNullableString(value: Option[String]): ByteVector           = value.fold(unsignedVarint(0))(compactString)
  def compactBytes(value: ByteVector): ByteVector                        = compact(value)
  def array[A](items: List[A])(item: A => ByteVector): ByteVector        = int32(items.size) ++ ByteVector.concat(items.map(item))
  def compactArray[A](items: List[A])(item: A => ByteVector): ByteVector = unsignedVarint(items.size + 1) ++ ByteVector.concat(items.map(item))

  /** The tagged fields of a flexible structure, of which this codec writes none. */
  val noTags: ByteVector = unsignedVarint(0)

  private def utf8(value: String): ByteVector                                 = ByteVector.view(value.getBytes(UTF_8))
  private def sized(bytes: ByteVector, length: Int => ByteVector): ByteVector = length(bytes.size.toInt) ++ bytes
  private def compact(bytes: ByteVector): ByteVector                          = unsignedVarint(bytes.size.toInt + 1) ++ bytes

  def fail[A](message: String): Read[A] = StateT.liftF(Left(message))

  private def take(count: Long, what: String): Read[ByteVector] =
    StateT: bytes =>
      if count < 0 then Left(s"$what has a negative length")
      else if bytes.size < count then Left(s"$what is cut short: $count bytes wanted, ${bytes.size} left")
      else Right((bytes.drop(count), bytes.take(count)))

  val readInt8: Read[Byte]   = take(1, "an int8").map(_.toByte())
  val readInt16: Read[Short] = take(2, "an int16").map(_.toShort())
  val readInt32: Read[Int]   = take(4, "an int32").map(_.toInt())
  val readInt64: Read[Long]  = take(8, "an int64").map(_.toLong())

  /** At most five bytes, as an int holds 32 bits. */
  val readUnsignedVarint: Read[Int] =
    def from(shift: Int, value: Int): Read[Int] =
      readInt8.flatMap: byte =>
        val next = value | ((byte & 0x7f) << shift)
        if (byte & 0x80) == 0 then next.pure[Read] else if shift == 28 then fail("an unsigned varint runs past five bytes") else from(shift + 7, next)
    from(0, 0)

  private def text(bytes: ByteVector): Read[String] = StateT.liftF(bytes.decodeUtf8.leftMap(error => s"a string is not UTF-8: ${error.getMessage}"))

  val readString: Read[String] =
    readInt16.flatMap(length => if length < 0 then fail("a string is null") else take(length.toLong, "a string").flatMap(text))

  val readNullableString: Read[Option[String]] =
    readInt16.flatMap(length => if length == -1 then none[String].pure[Read] else take(length.toLong, "a string").flatMap(text).map(_.some))

  private def compactLength(what: String): Read[Long] =
    readUnsignedVarint.flatMap(length => if length == 0 then fail(s"$what is null") else (Integer.toUnsignedLong(length) - 1).pure[Read])

  val readCompactString: Read[String] = compactLength("a string").flatMap(take(_, "a string")).flatMap(text)

  val readCompactNullableString: Read[Option[String]] =
    readUnsignedVarint.flatMap: length =>
      if length == 0 then none[String].pure[Read] else take(Integer.toUnsignedLong(length) - 1, "a string").flatMap(text).map(_.some)

  val readCompactBytes: Read[ByteVector] = compactLength("a byte string").flatMap(take(_, "a byte string"))

  def readArray[A](item: Read[A]): Read[List[A]] =
    readInt32.flatMap(count => if count < 0 then fail("an array is null") else repeated(count.toLong, "array items", item))

  def readCompactArray[A](item: Read[A]): Read[List[A]] = compactLength("an array").flatMap(repeated(_, "array items", item))

  /** Tagged fields this codec does not read, each skipped by its size. */
  val skipTags: Read[Unit] =
    val field = readUnsignedVarint *> readUnsignedVarint.flatMap(size => take(Integer.toUnsignedLong(size), "a tagged field"))
    readUnsignedVarint.flatMap(count => repeated(Integer.toUnsignedLong(count), "tagged fields", field)).void

  /** Every item takes at least a byte, so a count beyond the bytes left is refused before anything is read. */
  private def repeated[A](count: Long, items: String, item: Read[A]): Read[List[A]] =
    StateT.inspect[Result, ByteVector, Long](_.size)
      .flatMap(left => if count > left then fail(s"$count $items cannot fit in $left bytes") else item.replicateA(count.toInt))

  /** Reads the whole of the bytes, failing on any left over. */
  def readAll[A](read: Read[A])(bytes: ByteVector): Result[A] =
    read.run(bytes).flatMap((rest, value) => if rest.isEmpty then Right(value) else Left(s"${rest.size} bytes are left over"))
