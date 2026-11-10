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
import org.scalacheck.Gen

/** Values of a schema, for checking that xkafka writes and reads them as Apache Avro does. Floats and doubles are never NaN, which equals nothing,
  * not even itself.
  */
object AvroGen:
  def value(schema: AvroSchema, defined: Map[FullName, AvroSchema], depth: Int = 0): Gen[AvroValue] =
    schema match
      case AvroSchema.Null    => Gen.const(AvroValue.Null)
      case AvroSchema.Boolean => Gen.oneOf(true, false).map(AvroValue.Boolean(_))
      case AvroSchema.Int     => Gen.oneOf(Gen.choose(Int.MinValue, Int.MaxValue), Gen.choose(-200, 200), Gen.oneOf(0, -1, 1, 63, 64, -64, -65))
          .map(AvroValue.Int(_))
      case AvroSchema.Long  => Gen.oneOf(Gen.choose(Long.MinValue, Long.MaxValue), Gen.choose(-200L, 200L)).map(AvroValue.Long(_))
      case AvroSchema.Float => Gen.oneOf(Gen.choose(-1e6f, 1e6f), Gen.oneOf(0f, -0f, Float.MaxValue, Float.MinPositiveValue, Float.PositiveInfinity))
          .map(AvroValue.Float(_))
      case AvroSchema.Double => Gen.oneOf(Gen.choose(-1e12, 1e12), Gen.oneOf(0d, -0d, Double.MaxValue, Double.NegativeInfinity))
          .map(AvroValue.Double(_))
      case AvroSchema.Bytes                        => bytes(Gen.choose(0, 20)).map(AvroValue.Bytes(_))
      case AvroSchema.String                       => Gen.oneOf(Gen.alphaNumStr, Gen.asciiPrintableStr, Gen.const("é ☃ 𝄞")).map(AvroValue.String(_))
      case AvroSchema.Fixed(name, size, _)         => bytes(Gen.const(size)).map(AvroValue.Fixed(name, _))
      case AvroSchema.Enum(name, symbols, _, _, _) => Gen.oneOf(symbols).map(AvroValue.Enum(name, _))
      case AvroSchema.Array(items) => Gen.choose(0, if depth > 2 then 0 else 4).flatMap(Gen.listOfN(_, value(items, defined, depth + 1)))
          .map(items => AvroValue.Array(items.toVector))
      case AvroSchema.Map(values) => Gen.choose(0, if depth > 2 then 0 else 4)
          .flatMap(Gen.listOfN(_, Gen.zip(Gen.alphaStr, value(values, defined, depth + 1)))).map(entries => AvroValue.Map(entries.toMap.toVector))
      case AvroSchema.Union(branches) =>
        // A record that holds itself ends where a union lets it: through its first branch, null in practice.
        if depth > 3 then value(branches.head, defined, depth + 1) else Gen.oneOf(branches).flatMap(value(_, defined, depth + 1))
      case AvroSchema.Record(name, fields, _, _) => Gen
          .sequence[List[(String, AvroValue)], (String, AvroValue)](fields.map(field => value(field.schema, defined, depth + 1).map(field.name -> _)))
          .map(values => AvroValue.Record(name, values.toVector))
      case AvroSchema.Logical(logicalType) => value(logicalType.underlying, defined, depth)
      case AvroSchema.Named(name)          => value(defined(name), defined, depth)

  private def bytes(size: Gen[Int]): Gen[Chunk[Byte]] = size.flatMap(Gen.listOfN(_, Gen.choose(Byte.MinValue, Byte.MaxValue))).map(Chunk.from)
