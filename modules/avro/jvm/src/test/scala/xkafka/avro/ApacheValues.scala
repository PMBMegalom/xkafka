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

import java.nio.ByteBuffer

import scala.jdk.CollectionConverters.*

import fs2.Chunk
import org.apache.avro.Schema as ApacheSchema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8

/** Values as xkafka describes them, as Apache Avro's generic data, and back. */
object ApacheValues:
  /** The datum Apache Avro writes for `value` of `schema`. */
  def toApache(value: AvroValue, schema: ApacheSchema): Any =
    (value, schema.getType) match
      case (_, ApacheSchema.Type.UNION) =>
        val branch = schema.getTypes.asScala.find(branch => fits(value, branch)).getOrElse(sys.error(s"no branch of $schema fits $value"))
        toApache(value, branch)
      case (AvroValue.Null, _)            => null
      case (AvroValue.Boolean(value), _)  => value
      case (AvroValue.Int(value), _)      => value
      case (AvroValue.Long(value), _)     => value
      case (AvroValue.Float(value), _)    => value
      case (AvroValue.Double(value), _)   => value
      case (AvroValue.Bytes(bytes), _)    => ByteBuffer.wrap(bytes.toArray)
      case (AvroValue.String(value), _)   => new Utf8(value)
      case (AvroValue.Enum(_, symbol), _) => new GenericData.EnumSymbol(schema, symbol)
      case (AvroValue.Fixed(_, bytes), _) => new GenericData.Fixed(schema, bytes.toArray)
      case (AvroValue.Array(items), _)    => new GenericData.Array(schema, items.map(toApache(_, schema.getElementType)).asJava)
      case (AvroValue.Map(entries), _)    =>
        val map = new java.util.LinkedHashMap[Utf8, Any]()
        entries.foreach((key, value) => map.put(new Utf8(key), toApache(value, schema.getValueType)))
        map
      case (AvroValue.Record(_, fields), _) =>
        val record = new GenericData.Record(schema)
        fields.foreach((name, value) => record.put(name, toApache(value, schema.getField(name).schema)))
        record

  private def fits(value: AvroValue, schema: ApacheSchema): Boolean =
    (value, schema.getType) match
      case (AvroValue.Null, ApacheSchema.Type.NULL)              => true
      case (AvroValue.Boolean(_), ApacheSchema.Type.BOOLEAN)     => true
      case (AvroValue.Int(_), ApacheSchema.Type.INT)             => true
      case (AvroValue.Long(_), ApacheSchema.Type.LONG)           => true
      case (AvroValue.Float(_), ApacheSchema.Type.FLOAT)         => true
      case (AvroValue.Double(_), ApacheSchema.Type.DOUBLE)       => true
      case (AvroValue.Bytes(_), ApacheSchema.Type.BYTES)         => true
      case (AvroValue.String(_), ApacheSchema.Type.STRING)       => true
      case (AvroValue.Array(_), ApacheSchema.Type.ARRAY)         => true
      case (AvroValue.Map(_), ApacheSchema.Type.MAP)             => true
      case (AvroValue.Record(name, _), ApacheSchema.Type.RECORD) => name.value == schema.getFullName
      case (AvroValue.Enum(name, _), ApacheSchema.Type.ENUM)     => name.value == schema.getFullName
      case (AvroValue.Fixed(name, _), ApacheSchema.Type.FIXED)   => name.value == schema.getFullName
      case _                                                     => false

  /** The value a datum Apache Avro read is, as xkafka describes it. */
  def fromApache(datum: Any, schema: ApacheSchema): AvroValue =
    schema.getType match
      case ApacheSchema.Type.UNION   => fromApache(datum, schema.getTypes.get(GenericData.get.resolveUnion(schema, datum)))
      case ApacheSchema.Type.NULL    => AvroValue.Null
      case ApacheSchema.Type.BOOLEAN => AvroValue.Boolean(datum == java.lang.Boolean.TRUE)
      case ApacheSchema.Type.INT     => AvroValue.Int(datum.toString.toInt)
      case ApacheSchema.Type.LONG    => AvroValue.Long(datum.toString.toLong)
      case ApacheSchema.Type.FLOAT   => AvroValue.Float(datum.toString.toFloat)
      case ApacheSchema.Type.DOUBLE  => AvroValue.Double(datum.toString.toDouble)
      case ApacheSchema.Type.STRING  => AvroValue.String(datum.toString)
      case ApacheSchema.Type.BYTES   =>
        val buffer =
          datum match
            case buffer: ByteBuffer => buffer.duplicate;
            case other              => sys.error(s"bytes as $other")
        val array = new Array[Byte](buffer.remaining)
        buffer.get(array)
        AvroValue.Bytes(Chunk.array(array))
      case ApacheSchema.Type.ENUM  => AvroValue.Enum(FullName.from(schema.getFullName), datum.toString)
      case ApacheSchema.Type.FIXED => datum match
          case fixed: GenericData.Fixed => AvroValue.Fixed(FullName.from(schema.getFullName), Chunk.array(fixed.bytes))
          case other                    => sys.error(s"fixed as $other")
      case ApacheSchema.Type.ARRAY => datum match
          case items: java.util.Collection[?] => AvroValue.Array(items.asScala.map(fromApache(_, schema.getElementType)).toVector)
          case other                          => sys.error(s"array as $other")
      case ApacheSchema.Type.MAP => datum match
          case map: java.util.Map[?, ?] => AvroValue
              .Map(map.asScala.map((key, value) => key.toString -> fromApache(value, schema.getValueType)).toVector)
          case other => sys.error(s"map as $other")
      case ApacheSchema.Type.RECORD => datum match
          case record: GenericRecord => AvroValue.Record(
              FullName.from(schema.getFullName),
              schema.getFields.asScala.map(field => field.name -> fromApache(record.get(field.name), field.schema)).toVector
            )
          case other => sys.error(s"record as $other")

  /** The value with each map's entries in key order, since Apache Avro reads a map into a hash map, which keeps no order. */
  def sorted(value: AvroValue): AvroValue =
    value match
      case AvroValue.Map(entries)         => AvroValue.Map(entries.map((key, value) => key -> sorted(value)).sortBy(_._1))
      case AvroValue.Array(items)         => AvroValue.Array(items.map(sorted))
      case AvroValue.Record(name, fields) => AvroValue.Record(name, fields.map((key, value) => key -> sorted(value)))
      case other                          => other
