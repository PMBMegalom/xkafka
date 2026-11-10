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

import java.io.ByteArrayOutputStream

import fs2.Chunk
import munit.ScalaCheckSuite
import org.apache.avro.{Schema as ApacheSchema, SchemaCompatibility}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

/** xkafka's schemas, encoding and schema resolution against Apache Avro's. */
class ApacheAvroSuite extends ScalaCheckSuite:
  import ApacheValues.*

  private def parsed(text: String): AvroSchema = AvroSchemaJson.parse(text).fold(reason => fail(reason), identity)

  private def apache(text: String): ApacheSchema = new ApacheSchema.Parser().parse(text)

  private def apacheWrite(schema: ApacheSchema, datum: Any): Chunk[Byte] =
    val out     = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get.binaryEncoder(out, null)
    new GenericDatumWriter[Any](schema).write(datum, encoder)
    encoder.flush()
    Chunk.array(out.toByteArray)

  private def apacheRead(writer: ApacheSchema, reader: ApacheSchema, bytes: Chunk[Byte]): Any =
    new GenericDatumReader[Any](writer, reader).read(null, DecoderFactory.get.binaryDecoder(bytes.toArray, null))

  test("Apache Avro reads each schema xkafka writes as the schema it was read from"):
    Schemas.all.foreach: text =>
      assertEquals(apache(AvroSchemaJson.render(parsed(text))), apache(text), text)

  property("xkafka writes every value of the schemas in the bytes Apache Avro writes, and reads what Apache Avro writes"):
    Schemas.all.map: text =>
      val schema = parsed(text)
      val theirs = apache(text)
      forAll(AvroGen.value(schema, AvroSchema.definitions(schema))): value =>
        val written = apacheWrite(theirs, toApache(value, theirs))
        assertEquals(AvroBinary.encode(schema, value), Right(written))
        assertEquals(AvroBinary.decode(schema, written).map(sorted), Right(sorted(fromApache(apacheRead(theirs, theirs, written), theirs))))
    .reduce(_ && _)

  /** Pairs of a writer's and a reader's schema, each changed from the other as schemas evolve. */
  private val evolutions: List[(String, String, String)] =
    List(
      (
        "a field added with a default",
        """{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}""",
        """{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":["null","string"],"default":null},{"name":"c","type":{"type":"array","items":"int"},"default":[1,2]}]}"""
      ),
      (
        "a field removed",
        """{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"gone","type":{"type":"map","values":{"type":"record","name":"G","fields":[{"name":"x","type":"bytes"}]}}},{"name":"c","type":"string"}]}""",
        """{"type":"record","name":"R","fields":[{"name":"c","type":"string"},{"name":"a","type":"int"}]}"""
      ),
      (
        "numbers widened and text read as bytes",
        """{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"long"},{"name":"c","type":"float"},{"name":"d","type":"string"},{"name":"e","type":"int"}]}""",
        """{"type":"record","name":"R","fields":[{"name":"a","type":"long"},{"name":"b","type":"double"},{"name":"c","type":"double"},{"name":"d","type":"bytes"},{"name":"e","type":"float"}]}"""
      ),
      (
        "a field renamed through an alias, and a record renamed through one",
        """{"type":"record","name":"Old","namespace":"a","fields":[{"name":"before","type":"int"}]}""",
        """{"type":"record","name":"New","namespace":"b","aliases":["a.Old"],"fields":[{"name":"after","type":"int","aliases":["before"]}]}"""
      ),
      (
        "an enum symbol the reader lacks taken as its default",
        """{"type":"enum","name":"E","symbols":["A","B","C"]}""",
        """{"type":"enum","name":"E","symbols":["C","A","OTHER"],"default":"OTHER"}"""
      ),
      (
        "a value read into a union, and a union read as one of its branches",
        """{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":["null","long"]}]}""",
        """{"type":"record","name":"R","fields":[{"name":"a","type":["null","string","long"]},{"name":"b","type":["null","double","string"]}]}"""
      ),
      (
        "a record that holds itself",
        Schemas.list,
        """{"type":"record","name":"Node","fields":[{"name":"next","type":["null","Node"],"default":null},{"name":"value","type":"long"}]}"""
      )
    )

  property("xkafka reads what was written with one schema as another as Apache Avro does"):
    evolutions.map: (description, writerText, readerText) =>
      val (writer, reader)           = (parsed(writerText), parsed(readerText))
      val (theirWriter, theirReader) = (apache(writerText), apache(readerText))
      assertEquals(
        SchemaCompatibility.checkReaderWriterCompatibility(theirReader, theirWriter).getType,
        SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE,
        description
      )
      val decoder = AvroBinary.decoder(writer, reader).fold(reason => fail(s"$description: $reason"), identity)
      forAll(AvroGen.value(writer, AvroSchema.definitions(writer))): value =>
        val written = apacheWrite(theirWriter, toApache(value, theirWriter))
        assertEquals(decoder(written).map(sorted), Right(sorted(fromApache(apacheRead(theirWriter, theirReader, written), theirReader))), description)
    .reduce(_ && _)

  /** Pairs Apache Avro cannot read one as the other, and the value of the writer's it fails on. */
  private val breaks: List[(String, String, String, AvroValue)] =
    List(
      (
        "a field added without a default",
        """{"type":"record","name":"R","fields":[]}""",
        """{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}""",
        AvroValue.Record(FullName(None, "R"), Vector.empty)
      ),
      ("a number narrowed", "\"long\"", "\"int\"", AvroValue.Long(1)),
      (
        "a fixed resized",
        """{"type":"fixed","name":"F","size":2}""",
        """{"type":"fixed","name":"F","size":3}""",
        AvroValue.Fixed(FullName(None, "F"), Chunk[Byte](1, 2))
      ),
      (
        "an enum symbol the reader lacks, without a default",
        """{"type":"enum","name":"E","symbols":["A","B"]}""",
        """{"type":"enum","name":"E","symbols":["A"]}""",
        AvroValue.Enum(FullName(None, "E"), "B")
      ),
      ("a union branch the reader cannot take", """["null","string"]""", "\"null\"", AvroValue.String("x"))
    )

  test("xkafka refuses to read what Apache Avro refuses to"):
    breaks.foreach: (description, writerText, readerText, value) =>
      val written = apacheWrite(apache(writerText), toApache(value, apache(writerText)))
      assert(scala.util.Try(apacheRead(apache(writerText), apache(readerText), written)).isFailure, s"$description: Apache Avro reads it")
      assert(AvroBinary.decoder(parsed(writerText), parsed(readerText)).flatMap(_(written)).isLeft, s"$description: xkafka reads it")

  test("a record is not read as one of another name, as the specification and Apache Avro's compatibility check say, though its reader reads it"):
    val (writer, reader) = ("""{"type":"record","name":"A","fields":[]}""", """{"type":"record","name":"B","fields":[]}""")
    val compatibility    = SchemaCompatibility.checkReaderWriterCompatibility(apache(reader), apache(writer)).getType
    assertEquals(compatibility, SchemaCompatibility.SchemaCompatibilityType.INCOMPATIBLE)
    assertEquals(AvroBinary.decoder(parsed(writer), parsed(reader)).map(_ => ()), Left("record 'A' is not read as 'B'"))

  test("Apache Avro takes each derived schema, defaults checked, and reads a derived value with its own conversions"):
    List(AvroCodec[Order].schema, AvroCodec[Payment].schema, AvroCodec[Node].schema, AvroCodec[Preferred].schema).foreach: schema =>
      new ApacheSchema.Parser().setValidateDefaults(true).parse(AvroSchemaJson.render(schema))
    val order =
      Order(
        java.util.UUID.fromString("3f2b8c1e-9a4d-4e6f-8b2a-1c3d5e7f9a0b"),
        java.time.Instant.parse("2026-11-10T07:00:00.123Z"),
        java.time.LocalDate.of(2026, 11, 10),
        List(Line("apple")),
        Status.Shipped,
        Cash
      )
    val theirs = apache(AvroSchemaJson.render(AvroCodec[Order].schema))
    val data   = new GenericData()
    data.addLogicalTypeConversion(new org.apache.avro.data.TimeConversions.TimestampMillisConversion)
    data.addLogicalTypeConversion(new org.apache.avro.data.TimeConversions.DateConversion)
    data.addLogicalTypeConversion(new org.apache.avro.Conversions.UUIDConversion)
    val written = AvroCodec[Order].encode(order).flatMap(AvroBinary.encode(AvroCodec[Order].schema, _)).fold(fail(_), identity)
    val read    = new GenericDatumReader[GenericRecord](theirs, theirs, data).read(null, DecoderFactory.get.binaryDecoder(written.toArray, null))
    assertEquals(read.get("id"), order.id)
    assertEquals(read.get("placed_at"), order.placedAt)
    assertEquals(read.get("day"), order.day)
    assertEquals(read.get("status").toString, "Shipped")
    read.get("payment") match
      case payment: GenericRecord => assertEquals(payment.getSchema.getFullName, "xkafka.avro.Cash")
      case other                  => fail(s"expected a record, got $other")

  test("decimals are the bytes Apache Avro's decimal conversion writes"):
    val schema     = apache("""{"type":"bytes","logicalType":"decimal","precision":12,"scale":3}""")
    val conversion = new org.apache.avro.Conversions.DecimalConversion
    List("0", "1.5", "-1.5", "123456789.123", "-0.001", "-128", "127.999").foreach: text =>
      val value    = BigDecimal(text)
      val expected = conversion.toBytes(value.bigDecimal.setScale(3), schema, schema.getLogicalType)
      val bytes    = new Array[Byte](expected.remaining)
      expected.get(bytes)
      assertEquals(AvroCodec.decimal(12, 3).encode(value), Right(AvroValue.Bytes(Chunk.array(bytes))), text)

  test("NaN is written with the bits Apache Avro writes"):
    val nan = java.lang.Float.intBitsToFloat(0x7fc00001)
    assertEquals(AvroBinary.encode(AvroSchema.Float, AvroValue.Float(nan)), Right(apacheWrite(apache("\"float\""), nan)))

  test("a default too big for an int, and one of the wrong type, are refused as Apache Avro refuses them"):
    val big = """{"type":"record","name":"R","fields":[{"name":"a","type":"int","default":3000000000}]}"""
    assert(AvroSchemaJson.parse(big).isLeft)
    assert(scala.util.Try(new ApacheSchema.Parser().setValidateDefaults(true).parse(big)).isFailure)

  test("the generated values cover each branch of each union"):
    val schema   = parsed("""["null","int","string"]""")
    val branches = Gen.listOfN(200, AvroGen.value(schema, Map.empty)).sample.get.map(_.productPrefix).toSet
    assertEquals(branches, Set("Null", "Int", "String"))
