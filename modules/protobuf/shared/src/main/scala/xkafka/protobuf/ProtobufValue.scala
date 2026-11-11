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

import fs2.Chunk

/** A value as a .proto file describes it, whatever type it is read into. Numbers are as their field's type holds them: `int32`, `sint32` and
  * `sfixed32` fields hold [[ProtobufValue.Int32]], `uint32` and `fixed32` fields [[ProtobufValue.UInt32]], and likewise for 64 bits.
  */
enum ProtobufValue derives CanEqual:
  case Bool(value: scala.Boolean)
  case Int32(value: scala.Int)
  case Int64(value: scala.Long)

  /** An unsigned 32-bit number, from 0 to 4294967295. */
  case UInt32(value: scala.Long)

  /** An unsigned 64-bit number, from 0 to 18446744073709551615. */
  case UInt64(value: BigInt)

  case Float(value: scala.Float)
  case Double(value: scala.Double)
  case String(value: scala.Predef.String)
  case Bytes(value: Chunk[Byte])

  /** A value of an enum, by its name. */
  case Enum(name: scala.Predef.String)

  /** A value of an enum by its number, as one is read that the enum does not name, written by a newer version of it. */
  case EnumNumber(number: scala.Int)

  /** The message named `name`, its full name without a leading dot, and its fields that are set, by name, in the order its file declares them; and
    * the fields of numbers its file does not declare, kept as they were written.
    */
  case Message(name: scala.Predef.String, fields: Vector[(scala.Predef.String, ProtobufValue)], unknown: Vector[UnknownField] = Vector.empty)

  /** A repeated field's values. */
  case List(values: Vector[ProtobufValue])

  /** A map field's entries, in the order they were written. */
  case Map(entries: Vector[(ProtobufValue, ProtobufValue)])

  /** The value of field `name`, where this is a message in which it is set. */
  def field(name: scala.Predef.String): Option[ProtobufValue] =
    this match
      case Message(_, fields, _) => fields.collectFirst { case (`name`, value) => value }
      case _                     => None

/** A field of a number the reader's file does not declare: its number, and its bytes as they were written, its number's tag first. */
final case class UnknownField(number: Int, bytes: Chunk[Byte]) derives CanEqual
