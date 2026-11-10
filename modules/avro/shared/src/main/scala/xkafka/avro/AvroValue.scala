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

package xkafka.avro

import fs2.Chunk

/** A value as an Avro schema describes it, whatever type it is read into. A value of a logical type is its underlying value: a date is the
  * [[AvroValue.Int]] of its days, a decimal the [[AvroValue.Bytes]] of its unscaled value.
  */
enum AvroValue derives CanEqual:
  case Null
  case Boolean(value: scala.Boolean)
  case Int(value: scala.Int)
  case Long(value: scala.Long)
  case Float(value: scala.Float)
  case Double(value: scala.Double)
  case Bytes(value: Chunk[Byte])
  case String(value: scala.Predef.String)

  /** The fields of the record named `name`, in its schema's order. */
  case Record(name: FullName, fields: Vector[(scala.Predef.String, AvroValue)])

  /** A symbol of the enum named `name`. */
  case Enum(name: FullName, symbol: scala.Predef.String)

  case Array(items: Vector[AvroValue])

  /** Entries by key, in the order they are written. */
  case Map(entries: Vector[(scala.Predef.String, AvroValue)])

  /** The bytes of the fixed type named `name`. */
  case Fixed(name: FullName, bytes: Chunk[Byte])

  /** The value of field `name`, where this is a record that has one. */
  def field(name: scala.Predef.String): Option[AvroValue] =
    this match
      case Record(_, fields) => fields.collectFirst { case (`name`, value) => value }
      case _                 => None
