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

/** A named type's name and the namespace it is in, as `com.example` and `Order` make `com.example.Order`. */
final case class FullName(namespace: Option[scala.Predef.String], name: scala.Predef.String) derives CanEqual:
  /** The name with its namespace, as Avro writes a full name. */
  def value: scala.Predef.String = namespace.fold(name)(space => s"$space.$name")

object FullName:
  /** The full name `value` names, where a name without a dot is in `enclosing`, the namespace it is written in, as Avro reads names. */
  def from(value: scala.Predef.String, enclosing: Option[scala.Predef.String] = None): FullName =
    value.lastIndexOf('.') match
      case -1  => FullName(enclosing.filter(_.nonEmpty), value)
      case dot => FullName(Some(value.take(dot)).filter(_.nonEmpty), value.drop(dot + 1))

/** What a value of a schema means beyond its Avro type, as Avro's logical types name it. A schema with a logical type is written as the type it is
  * based on, so a reader that does not know the logical type reads that.
  */
enum LogicalType derives CanEqual:
  /** A decimal number of at most `precision` digits, `scale` of them after the point, written as the bytes of its unscaled value. */
  case Decimal(precision: scala.Int, scale: scala.Int)

  /** A decimal number written as the `size` bytes of the fixed type `fixedName`. */
  case FixedDecimal(fixedName: FullName, size: scala.Int, precision: scala.Int, scale: scala.Int)

  /** A UUID, written as text. */
  case Uuid

  /** A day, written as the days since 1970-01-01. */
  case Date

  /** A time of day, written as the milliseconds since midnight. */
  case TimeMillis

  /** A time of day, written as the microseconds since midnight. */
  case TimeMicros

  /** An instant, written as the milliseconds since 1970-01-01T00:00:00Z. */
  case TimestampMillis

  /** An instant, written as the microseconds since 1970-01-01T00:00:00Z. */
  case TimestampMicros

  /** An instant, written as the nanoseconds since 1970-01-01T00:00:00Z. */
  case TimestampNanos

  /** A date and time without a time zone, written as the milliseconds since 1970-01-01T00:00:00. */
  case LocalTimestampMillis

  /** A date and time without a time zone, written as the microseconds since 1970-01-01T00:00:00. */
  case LocalTimestampMicros

  /** A date and time without a time zone, written as the nanoseconds since 1970-01-01T00:00:00. */
  case LocalTimestampNanos

  /** The schema values are written as. */
  def underlying: AvroSchema =
    this match
      case Decimal(_, _)                                  => AvroSchema.Bytes
      case FixedDecimal(name, size, _, _)                 => AvroSchema.Fixed(name, size)
      case Uuid                                           => AvroSchema.String
      case Date | TimeMillis                              => AvroSchema.Int
      case TimeMicros | TimestampMillis | TimestampMicros => AvroSchema.Long
      case TimestampNanos | LocalTimestampMillis          => AvroSchema.Long
      case LocalTimestampMicros | LocalTimestampNanos     => AvroSchema.Long

  /** How Avro names it. */
  def name: scala.Predef.String =
    this match
      case Decimal(_, _) | FixedDecimal(_, _, _, _) => "decimal"
      case Uuid                                     => "uuid"
      case Date                                     => "date"
      case TimeMillis                               => "time-millis"
      case TimeMicros                               => "time-micros"
      case TimestampMillis                          => "timestamp-millis"
      case TimestampMicros                          => "timestamp-micros"
      case TimestampNanos                           => "timestamp-nanos"
      case LocalTimestampMillis                     => "local-timestamp-millis"
      case LocalTimestampMicros                     => "local-timestamp-micros"
      case LocalTimestampNanos                      => "local-timestamp-nanos"

/** A record's field: its name and schema, and the value a reader takes for it where what it reads was written without it. */
final case class AvroField(
    name: scala.Predef.String,
    schema: AvroSchema,
    default: Option[AvroValue] = None,
    doc: Option[scala.Predef.String] = None,
    aliases: List[scala.Predef.String] = Nil
) derives CanEqual

/** An Avro schema, as the Avro specification defines one. */
enum AvroSchema derives CanEqual:
  case Null, Boolean, Int, Long, Float, Double, Bytes, String

  /** Named fields, in the order they are written. */
  case Record(name: FullName, fields: List[AvroField], doc: Option[scala.Predef.String] = None, aliases: List[FullName] = Nil)

  /** One of `symbols`, where a reader takes `default` for a symbol it does not have. */
  case Enum(
      name: FullName,
      symbols: List[scala.Predef.String],
      default: Option[scala.Predef.String] = None,
      doc: Option[scala.Predef.String] = None,
      aliases: List[FullName] = Nil
  )

  case Array(items: AvroSchema)

  /** Values by text keys. */
  case Map(values: AvroSchema)

  /** A value of one of `branches`, written with the number of its branch. */
  case Union(branches: List[AvroSchema])

  /** Exactly `size` bytes. */
  case Fixed(name: FullName, size: scala.Int, aliases: List[FullName] = Nil)

  /** A type with a meaning beyond its Avro type, written as [[LogicalType.underlying]]. */
  case Logical(logicalType: LogicalType)

  /** The named type `name`, defined where the schema first names it. */
  case Named(name: FullName)

  /** The full name of a named type. */
  def fullName: Option[FullName] =
    this match
      case Record(name, _, _, _)  => Some(name)
      case Enum(name, _, _, _, _) => Some(name)
      case Fixed(name, _, _)      => Some(name)
      case Logical(logicalType)   => logicalType.underlying.fullName
      case Named(name)            => Some(name)
      case _                      => None

object AvroSchema:
  /** The named types `schema` defines, by full name, wherever in it they are defined. */
  def definitions(schema: AvroSchema): scala.collection.immutable.Map[FullName, AvroSchema] =
    def from(schema: AvroSchema, found: scala.collection.immutable.Map[FullName, AvroSchema]): scala.collection.immutable.Map[FullName, AvroSchema] =
      schema match
        case record @ Record(name, fields, _, _) if !found.contains(name) =>
          fields.foldLeft(found.updated(name, record))((sofar, field) => from(field.schema, sofar))
        case named @ (Enum(_, _, _, _, _) | Fixed(_, _, _)) => named.fullName
            .fold(found)(name => if found.contains(name) then found else found.updated(name, named))
        case Logical(logicalType) => from(logicalType.underlying, found)
        case Array(items)         => from(items, found)
        case Map(values)          => from(values, found)
        case Union(branches)      => branches.foldLeft(found)((sofar, branch) => from(branch, sofar))
        case _                    => found
    from(schema, scala.collection.immutable.Map.empty)
