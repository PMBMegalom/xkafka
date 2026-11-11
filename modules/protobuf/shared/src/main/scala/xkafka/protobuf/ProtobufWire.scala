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

package xkafka.protobuf

import scala.annotation.tailrec

import cats.syntax.all.*
import fs2.Chunk

/** A field's value as the wire holds it, before its type reads it. */
private[protobuf] enum Raw:
  case Varint(value: Long)
  case Fixed64(value: Long)
  case Bytes(value: Chunk[Byte])
  case Fixed32(value: Int)

/** Protobuf's encoding: each field as its number and wire type, then its value as a varint, little-endian fixed bytes or length-prefixed bytes. */
private[protobuf] object ProtobufWire:
  val VarintType: Int  = 0
  val Fixed64Type: Int = 1
  val BytesType: Int   = 2
  val Fixed32Type: Int = 5

  /** The largest field number Protobuf allows. */
  val MaxNumber: Int = 536870911

  def wireOf(raw: Raw): Int =
    raw match
      case Raw.Varint(_)  => VarintType
      case Raw.Fixed64(_) => Fixed64Type
      case Raw.Bytes(_)   => BytesType
      case Raw.Fixed32(_) => Fixed32Type

  /** `value`'s bits in groups of seven, lowest first, each but the last with its high bit set. */
  def varint(value: Long): Chunk[Byte] =
    @tailrec
    def go(rest: Long, written: List[Byte]): List[Byte] =
      if (rest & ~0x7fL) == 0 then rest.toByte :: written else go(rest >>> 7, ((rest & 0x7f) | 0x80).toByte :: written)
    Chunk.from(go(value, Nil).reverse)

  def fixed32(value: Int): Chunk[Byte] = Chunk.from((0 until 4).map(at => (value >>> (8 * at)).toByte))

  def fixed64(value: Long): Chunk[Byte] = Chunk.from((0 until 8).map(at => (value >>> (8 * at)).toByte))

  /** A value as it is written after its tag, and as a packed repeated field holds it. */
  def payload(raw: Raw): Chunk[Byte] =
    raw match
      case Raw.Varint(value)  => varint(value)
      case Raw.Fixed64(value) => fixed64(value)
      case Raw.Bytes(value)   => varint(value.size.toLong) ++ value
      case Raw.Fixed32(value) => fixed32(value)

  /** Field `number` holding `raw`. */
  def field(number: Int, raw: Raw): Chunk[Byte] = varint((number.toLong << 3) | wireOf(raw)) ++ payload(raw)

  /** The fields of a message, in the order they were written. */
  def fields(bytes: Chunk[Byte]): Either[String, Vector[(Int, Raw)]] =
    @tailrec
    def go(at: Int, read: Vector[(Int, Raw)]): Either[String, Vector[(Int, Raw)]] =
      if at >= bytes.size then Right(read)
      else
        fieldAt(bytes, at) match
          case Right((number, raw, next)) => go(next, read :+ (number -> raw))
          case Left(reason)               => Left(reason)
    go(0, Vector.empty)

  /** The values of a packed repeated field of wire type `wire`. */
  def packed(bytes: Chunk[Byte], wire: Int): Either[String, Vector[Raw]] =
    @tailrec
    def go(at: Int, read: Vector[Raw]): Either[String, Vector[Raw]] =
      if at >= bytes.size then Right(read)
      else
        valueAt(bytes, at, wire) match
          case Right((raw, next)) => go(next, read :+ raw)
          case Left(reason)       => Left(reason)
    go(0, Vector.empty)

  private def fieldAt(bytes: Chunk[Byte], at: Int): Either[String, (Int, Raw, Int)] =
    varintAt(bytes, at).flatMap: (key, next) =>
      val number = key >>> 3
      if number < 1 || number > MaxNumber then Left(s"$number is not a field number")
      else valueAt(bytes, next, (key & 7).toInt).map((raw, end) => (number.toInt, raw, end))

  private def valueAt(bytes: Chunk[Byte], at: Int, wire: Int): Either[String, (Raw, Int)] =
    wire match
      case VarintType  => varintAt(bytes, at).map((value, next) => (Raw.Varint(value), next))
      case Fixed64Type => fixedAt(bytes, at, 8).map((value, next) => (Raw.Fixed64(value), next))
      case Fixed32Type => fixedAt(bytes, at, 4).map((value, next) => (Raw.Fixed32(value.toInt), next))
      case BytesType   => varintAt(bytes, at).flatMap: (size, start) =>
          if size < 0 || size > bytes.size - start then Left("a field's length runs past the end of the record")
          else Right((Raw.Bytes(bytes.drop(start).take(size.toInt)), start + size.toInt))
      case 3 | 4 => Left("the record holds a group, which proto3 does not have")
      case other => Left(s"$other is not one of Protobuf's wire types")

  private def varintAt(bytes: Chunk[Byte], at: Int): Either[String, (Long, Int)] =
    @tailrec
    def go(at: Int, shift: Int, value: Long): Either[String, (Long, Int)] =
      if shift >= 70 then Left("a varint is longer than ten bytes")
      else if at >= bytes.size then Left("the record ends inside a varint")
      else
        val byte = bytes(at)
        val read = value | ((byte & 0x7fL) << shift)
        if (byte & 0x80) == 0 then Right((read, at + 1)) else go(at + 1, shift + 7, read)
    go(at, 0, 0L)

  private def fixedAt(bytes: Chunk[Byte], at: Int, size: Int): Either[String, (Long, Int)] =
    if bytes.size - at < size then Left("the record ends inside a number")
    else Right(((0 until size).foldLeft(0L)((value, index) => value | ((bytes(at + index) & 0xffL) << (8 * index))), at + size))

  private def zigzag(value: Int): Long = ((value << 1) ^ (value >> 31)).toLong & 0xffffffffL

  private def unzigzag(value: Long): Int = (value.toInt >>> 1) ^ -(value.toInt & 1)

  /** Confluent's message indexes, which say which message of the schema's file a record is: the path to it through the file's messages and the
    * messages they declare, as a count and then the indexes, each a zigzag varint, where the file's first message is written as a single zero.
    */
  def indexes(path: List[Int]): Chunk[Byte] =
    if path == List(0) then Chunk(0.toByte) else (path.size :: path).foldLeft(Chunk.empty[Byte])((written, index) => written ++ varint(zigzag(index)))

  /** The message indexes a record starts with, and the message after them. */
  def readIndexes(bytes: Chunk[Byte]): Either[String, (List[Int], Chunk[Byte])] =
    @tailrec
    def go(left: Int, at: Int, read: List[Int]): Either[String, (List[Int], Chunk[Byte])] =
      if left == 0 then Right((read.reverse, bytes.drop(at)))
      else
        varintAt(bytes, at) match
          case Right((index, next)) => go(left - 1, next, unzigzag(index) :: read)
          case Left(reason)         => Left(s"the record's message indexes cannot be read: $reason")
    varintAt(bytes, 0).leftMap(reason => s"the record's message indexes cannot be read: $reason").flatMap: (count, next) =>
      unzigzag(count) match
        case 0                                            => Right((List(0), bytes.drop(next)))
        case size if size < 0 || size > bytes.size - next => Left(s"the record names $size message indexes, more than it holds")
        case size                                         => go(size, next, Nil)
