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
import munit.ScalaCheckSuite
import org.scalacheck.Prop.forAll

class AvroBinarySuite extends ScalaCheckSuite:
  private def parsed(text: String): AvroSchema = AvroSchemaJson.parse(text).fold(reason => fail(reason), identity)

  private def bytes(values: Int*): Chunk[Byte] = Chunk.from(values.map(_.toByte))

  test("numbers are written as zigzag varints, as the specification's examples write them"):
    val examples =
      List(0 -> bytes(0x00), -1 -> bytes(0x01), 1 -> bytes(0x02), -2 -> bytes(0x03), 2 -> bytes(0x04), -64 -> bytes(0x7f), 64 -> bytes(0x80, 0x01))
    examples.foreach((value, expected) => assertEquals(AvroBinary.encode(AvroSchema.Int, AvroValue.Int(value)), Right(expected), value))
    assertEquals(AvroBinary.encode(AvroSchema.Long, AvroValue.Long(Long.MinValue)).map(_.size), Right(10))

  test("text is its length and then its UTF-8 bytes"):
    assertEquals(AvroBinary.encode(AvroSchema.String, AvroValue.String("foo")), Right(bytes(0x06, 0x66, 0x6f, 0x6f)))

  test("a union writes its branch's number before the value, as the specification's example writes it"):
    val union = AvroSchema.Union(List(AvroSchema.Null, AvroSchema.String))
    assertEquals(AvroBinary.encode(union, AvroValue.Null), Right(bytes(0x00)))
    assertEquals(AvroBinary.encode(union, AvroValue.String("a")), Right(bytes(0x02, 0x02, 0x61)))

  test("an array is written in a block of its count, ended by an empty block, as the specification's example writes it"):
    val array = AvroSchema.Array(AvroSchema.Long)
    assertEquals(AvroBinary.encode(array, AvroValue.Array(Vector(AvroValue.Long(3), AvroValue.Long(27)))), Right(bytes(0x04, 0x06, 0x36, 0x00)))
    assertEquals(
      AvroBinary.decode(array, bytes(0x03, 0x04, 0x06, 0x36, 0x00)),
      Right(AvroValue.Array(Vector(AvroValue.Long(3), AvroValue.Long(27)))),
      "a negative count is followed by the block's size"
    )

  property("every value of the schemas reads back as it was written"):
    Schemas.all.map(parsed).map: schema =>
      forAll(AvroGen.value(schema, AvroSchema.definitions(schema))): value =>
        assertEquals(AvroBinary.encode(schema, value).flatMap(AvroBinary.decode(schema, _)), Right(value))
    .reduce(_ && _)

  test("a value that does not fit its schema is refused, and so are bytes that do not hold one"):
    assert(AvroBinary.encode(AvroSchema.Int, AvroValue.String("a")).isLeft)
    assertEquals(
      AvroBinary.encode(AvroSchema.Fixed(FullName(None, "F"), 2), AvroValue.Fixed(FullName(None, "F"), bytes(1))),
      Left("fixed 'F' is 2 bytes, but the value is 1")
    )
    assertEquals(AvroBinary.decode(AvroSchema.String, bytes(0x06, 0x66)), Left("a length of 3 does not fit the value"))
    assertEquals(AvroBinary.decode(AvroSchema.Int, bytes(0xff, 0xff, 0xff, 0xff, 0xff, 0x01)), Left("a number takes more than 5 bytes"))
    assertEquals(AvroBinary.decode(AvroSchema.Int, bytes(0x02, 0x02)), Left("1 bytes are left after the value"))
    assertEquals(AvroBinary.decode(AvroSchema.Union(List(AvroSchema.Null)), bytes(0x02)), Left("union branch 1 is not one of the writer's 1"))

  test("schemas with unknown types, a union in a union or of one type twice, or a name defined twice are refused"):
    assertEquals(AvroSchemaJson.parse("\"Missing\""), Left("'Missing' names no type defined before it"))
    assertEquals(AvroSchemaJson.parse("""["null",["int"]]"""), Left("a union cannot hold a union"))
    assertEquals(AvroSchemaJson.parse("""["int",{"type":"int","logicalType":"date"}]"""), Left("a union holds 'Int' twice"))
    assertEquals(AvroSchemaJson.parse("""[{"type":"array","items":"int"},{"type":"array","items":"string"}]"""), Left("a union holds 'Array' twice"))
    assert(
      AvroSchemaJson.parse("""[{"type":"fixed","name":"A","size":1},{"type":"fixed","name":"B","size":1}]""").isRight,
      "named types of different names"
    )
    assert(AvroSchemaJson.parse("""{"type":"record","name":"A","fields":[{"name":"b","type":{"type":"fixed","name":"A","size":1}}]}""").isLeft)
    assertEquals(
      AvroSchemaJson.parse("""{"type":"record","name":"A","fields":[{"name":"b","type":"int","default":"x"}]}"""),
      Left("field 'b' has a default that does not fit its type: expected a number, found \"x\"")
    )

  test("a logical type that is unknown or does not fit its type is read as the type it is on"):
    assertEquals(AvroSchemaJson.parse("""{"type":"string","logicalType":"date"}"""), Right(AvroSchema.String))
    assertEquals(AvroSchemaJson.parse("""{"type":"long","logicalType":"made-up"}"""), Right(AvroSchema.Long))
    assertEquals(AvroSchemaJson.parse("""{"type":"bytes","logicalType":"decimal","precision":2,"scale":3}"""), Right(AvroSchema.Bytes))
