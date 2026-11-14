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

  test("a field the reader lacks is skipped as the writer wrote it, whatever the reader's types of the same names are"):
    val writer =
      parsed("""{"type":"record","name":"Visit","fields":[{"name":"id","type":"int"},
        |{"name":"billing","type":{"type":"record","name":"Address","fields":[{"name":"street","type":"string"}]}},
        |{"name":"shipping","type":["null","Address"]}]}""".stripMargin)
    val readers =
      List(
        """{"type":"record","name":"Visit","fields":[{"name":"id","type":"int"}]}""",
        """{"type":"record","name":"Visit","fields":[{"name":"id","type":"int"},
        |{"name":"other","type":{"type":"record","name":"Address","fields":[{"name":"zip","type":"int"}]},"default":{"zip":0}}]}""".stripMargin
      ).map(parsed)
    val street  = AvroValue.Record(FullName(None, "Address"), Vector("street" -> AvroValue.String("Main")))
    val written =
      AvroBinary
        .encode(writer, AvroValue.Record(FullName(None, "Visit"), Vector("id" -> AvroValue.Int(7), "billing" -> street, "shipping" -> street)))
    readers.foreach: reader =>
      val read = written.flatMap(bytes => AvroBinary.decoder(writer, reader).flatMap(_(bytes)))
      assertEquals(
        read.map { case AvroValue.Record(_, fields) => fields.head; case other => fail(s"not a record: $other") },
        Right("id" -> AvroValue.Int(7))
      )

  test("counts of arrays and maps that the bytes left cannot hold are refused before their items are read"):
    def count(value: Long) = AvroBinary.encode(AvroSchema.Long, AvroValue.Long(value)).fold(fail(_), identity)
    val nulls              = AvroSchema.Array(AvroSchema.Null)
    assertEquals(AvroBinary.decode(nulls, count(1L << 62) ++ bytes(0x00)), Left(s"a count of ${1L << 62} items does not fit the value"))
    assertEquals(
      AvroBinary.decode(nulls, count(Long.MinValue) ++ count(0) ++ bytes(0x00)),
      Left(s"a count of ${Long.MinValue} items does not fit the value")
    )
    assertEquals(
      AvroBinary.decode(AvroSchema.Array(AvroSchema.Long), count(1000) ++ bytes(0x00)),
      Left("a count of 1000 items does not fit the value")
    )
    assertEquals(AvroBinary.decode(AvroSchema.Map(AvroSchema.Null), count(1000) ++ bytes(0x00)), Left("a count of 1000 items does not fit the value"))
    assertEquals(
      AvroBinary.decode(nulls, count(AvroBinary.MaxEmptyItems.toLong + 1) ++ bytes(0x00)).left.toOption,
      Some(s"a count of ${AvroBinary.MaxEmptyItems + 1} items does not fit the value")
    )
    assertEquals(AvroBinary.decode(nulls, bytes(0x0a, 0x00)), Right(AvroValue.Array(Vector.fill(5)(AvroValue.Null))), "a few nulls in fewer bytes")

  test("a fixed type of more bytes than are left is refused"):
    val schema =
      parsed(
        """{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"f","type":{"type":"fixed","name":"F","size":2147483647}}]}"""
      )
    assertEquals(AvroBinary.decode(schema, bytes(0x00)), Left("the value ends before its last byte"))

  test("decimals are read as decimals of another scale or a smaller precision only by refusing them"):
    def decimal(precision: Int, scale: Int) = parsed(s"""{"type":"bytes","logicalType":"decimal","precision":$precision,"scale":$scale}""")
    assert(AvroBinary.decoder(decimal(10, 2), decimal(10, 4)).isLeft, "another scale")
    assert(AvroBinary.decoder(decimal(10, 2), decimal(8, 2)).isLeft, "a smaller precision")
    assert(AvroBinary.decoder(decimal(8, 2), decimal(10, 2)).isRight, "a larger precision")
    assert(AvroBinary.decoder(AvroSchema.Bytes, decimal(10, 2)).isRight, "bytes, which carry no scale")
    def optional(schema: AvroSchema) = AvroSchema.Union(List(AvroSchema.Null, schema))
    assert(AvroBinary.decoder(decimal(10, 2), optional(decimal(10, 4))).isLeft, "another scale, the reader's in a union")
    // A writer's branch the reader cannot take fails only the values written with it: 1.23, unscaled 123, as the union's second branch.
    val written                                      = bytes(0x02, 0x02, 0x7b)
    def read(writer: AvroSchema, reader: AvroSchema) = AvroBinary.decoder(writer, reader).flatMap(_(written))
    assert(read(optional(decimal(10, 2)), optional(decimal(10, 4))).isLeft, "another scale, each in a union")
    assert(read(optional(decimal(10, 2)), optional(decimal(8, 2))).isLeft, "a smaller precision, each in a union")
    assert(read(optional(decimal(8, 2)), optional(decimal(10, 2))).isRight, "a larger precision, each in a union")

  test("a decimal held in a fixed type keeps its scale where a field names it, so another scale is refused there too"):
    val amount = """{"type":"fixed","name":"Amt","size":8,"logicalType":"decimal","precision":10,"scale":%d}"""
    val writer = parsed(s"""{"type":"record","name":"M","fields":[{"name":"price","type":${amount.format(2)}},{"name":"tax","type":"Amt"}]}""")
    val reader = parsed(s"""{"type":"record","name":"M","fields":[{"name":"tax","type":${amount.format(4)}}]}""")
    assert(AvroBinary.decoder(writer, reader).isLeft)
    assert(AvroBinary.writable(writer, reader).isLeft)

  test("a decimal held in a fixed type keeps its aliases, and is read through one"):
    val written = parsed("""{"type":"fixed","name":"Money","size":4,"logicalType":"decimal","precision":6,"scale":2}""")
    val reader  = parsed("""{"type":"fixed","name":"Money2","aliases":["Money"],"size":4,"logicalType":"decimal","precision":6,"scale":2}""")
    assertEquals(AvroSchemaJson.parse(AvroSchemaJson.render(reader)), Right(reader))
    assert(AvroSchemaJson.render(reader).contains("\"aliases\":[\"Money\"]"), AvroSchemaJson.render(reader))
    assert(AvroBinary.decoder(written, reader).isRight)

  test("a writer's record in a union is read as the reader's branch of the same full name, before one that shares only its simple name"):
    val union =
      parsed(
        """[{"type":"record","name":"E","namespace":"a","fields":[{"name":"x","type":"int"}]},""" +
          """{"type":"record","name":"E","namespace":"b","fields":[{"name":"x","type":"int"}]}]"""
      )
    val second = AvroValue.Record(FullName(Some("b"), "E"), Vector("x" -> AvroValue.Int(1)))
    assertEquals(AvroBinary.encode(union, second).flatMap(AvroBinary.decode(union, _)), Right(second))

  test("values nested deeper than the limit are refused, not read until the stack runs out"):
    val list   = parsed("""{"type":"record","name":"Node","fields":[{"name":"next","type":["null","Node"]}]}""")
    val nested = Chunk.from(Array.fill(1000000)(0x02.toByte)) ++ bytes(0x00)
    assertEquals(AvroBinary.decode(list, nested), Left(s"the value is nested more than ${AvroBinary.MaxDepth} deep"))
    val within = Chunk.from(Array.fill(AvroBinary.MaxDepth - 1)(0x02.toByte)) ++ bytes(0x00)
    assert(AvroBinary.decode(list, within).isRight)

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

  test("a record each of whose fields uses the next record is planned once for each record, however often it is used"):
    // R0 holds two R1, which hold two R2, and so on: planned once for each use, R39 would be planned 2^39 times.
    val records = (0 until 40).foldRight("""{"type":"record","name":"R40","fields":[{"name":"n","type":"null"}]}"""): (level, inner) =>
      s"""{"type":"record","name":"R$level","fields":[{"name":"a","type":$inner},{"name":"b","type":"R${level + 1}"}]}"""
    val schema = parsed(s"""{"type":"array","items":$records}""")
    assertEquals(AvroBinary.decode(schema, bytes(0x00)), Right(AvroValue.Array(Vector.empty)))
    val skipping = parsed(s"""{"type":"record","name":"Outer","fields":[{"name":"r","type":$records}]}""")
    assert(AvroBinary.decoder(skipping, parsed("""{"type":"record","name":"Outer","fields":[]}""")).isRight)

  test("a record planned through one whose plan fails is refused when the reader is made, wherever the schema uses it"):
    // While A is planned, B is planned through it; A then fails, which `first`'s union only holds against values written as A, but `second` reads B.
    def root(readerOnly: String) =
      val b = """{"type":"record","name":"B","fields":[{"name":"as","type":{"type":"array","items":"A"}}]}"""
      val a = s"""{"type":"record","name":"A","fields":[{"name":"b","type":$b}$readerOnly]}"""
      parsed(s"""{"type":"record","name":"Root","fields":[{"name":"first","type":["null",$a]},{"name":"second","type":"B"}]}""")
    assertEquals(
      AvroBinary.decoder(root(""), root(""",{"name":"y","type":"int"}""")).map(_ => ()),
      Left("record 'A' has no default for field 'y', which 'A' does not write")
    )

  test("checking that a schema whose records are each used twice by the one before can be written as itself checks each pair of records once"):
    // R0 holds two R1, which hold two R2, and so on: walked once for each use, R39 would be walked 2^39 times.
    val records = (0 until 40).foldRight("""{"type":"record","name":"R40","fields":[{"name":"n","type":"null"}]}"""): (level, inner) =>
      s"""{"type":"record","name":"R$level","fields":[{"name":"a","type":$inner},{"name":"b","type":"R${level + 1}"}]}"""
    val schema = parsed(records)
    assertEquals(AvroBinary.writable(schema, schema), Right(()))

  test("a writer's record in a union is read as the reader's branch whose alias is its full name, before one sharing only its simple name"):
    val writer = parsed("""{"type":"record","name":"R","namespace":"a","fields":[{"name":"x","type":"int"}]}""")
    val reader =
      parsed(
        """[{"type":"record","name":"R","namespace":"x","fields":[{"name":"x","type":"int"}]},""" +
          """{"type":"record","name":"Q","namespace":"b","aliases":["a.R"],"fields":[{"name":"x","type":"int"}]}]"""
      )
    val value = AvroValue.Record(FullName(Some("a"), "R"), Vector("x" -> AvroValue.Int(1)))
    val read  = AvroBinary.encode(writer, value).flatMap(bytes => AvroBinary.decoder(writer, reader).flatMap(_(bytes)))
    assertEquals(read.map { case AvroValue.Record(name, _) => Some(name); case _ => None }, Right(Some(FullName(Some("b"), "Q"))))

  test("a record of many fields is written and read back, each field found once, not by searching the others"):
    val count  = 20000
    val schema =
      parsed((0 until count).map(n => s"""{"name":"f$n","type":"int"}""").mkString("""{"type":"record","name":"Wide","fields":[""", ",", "]}"))
    val reader =
      parsed(
        (0 until count).reverse.map(n => s"""{"name":"f$n","type":"int"}""").mkString("""{"type":"record","name":"Wide","fields":[""", ",", "]}")
      )
    val value = AvroValue.Record(FullName(None, "Wide"), (0 until count).toVector.map(n => s"f$n" -> AvroValue.Int(n)))
    val read  = AvroBinary.encode(schema, value).flatMap(bytes => AvroBinary.decoder(schema, reader).flatMap(_(bytes)))
    assertEquals(read.map { case AvroValue.Record(_, fields) => fields.headOption; case _ => None }, Right(Some("f19999" -> AvroValue.Int(19999))))

  test("a number whose tenth byte holds bits past 64 is refused, not read without them"):
    val overlong = bytes(0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f)
    assertEquals(AvroBinary.decode(AvroSchema.Long, overlong), Left("a number does not fit 64 bits"))
    assertEquals(
      AvroBinary.decode(AvroSchema.Long, bytes(0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01)),
      Right(AvroValue.Long(Long.MaxValue))
    )
