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

package example.avrohugger

import com.example.clicks.Click
import com.example.shop.{Blob, Graded, Hashed, Priced}
import com.example.web.{Ad, Agent, Kind, Link, Visit}
import io.circe.Json
import munit.FunSuite
import xkafka.avro.{AvroBinary, AvroCodec, AvroSchema, AvroSchemaJson, AvroValue}

/** Codecs derived for the classes avrohugger generates, set up as other-libraries.md says, against the files they were generated from. */
class AvrohuggerSuite extends FunSuite:
  given AvroCodec[Ad | Link] = AvroCodec.union[(Ad, Link)]

  /** The file's schema without its `doc`s, which avrohugger turns into comments. */
  private def withoutDocs(file: String): AvroSchema =
    def strip(json: Json): Json =
      json.arrayOrObject(json, values => Json.fromValues(values.map(strip)), fields => Json.fromJsonObject(fields.remove("doc").mapValues(strip)))
    io.circe.jawn.parse(AvroFiles.texts(file)).flatMap(json => AvroSchemaJson.parse(strip(json).noSpaces).left.map(new Exception(_)))
      .fold(throw _, identity)

  private def roundTrip[A](value: A)(using codec: AvroCodec[A]): Either[String, A] =
    codec.encode(value).flatMap(AvroBinary.encode(codec.schema, _)).flatMap(AvroBinary.decode(codec.schema, _)).flatMap(codec.decode)

  test(
    "a record's derived schema is its file's without the docs: records, enums, primitives, bytes, unions, arrays, maps, defaults and logical types"
  ):
    val derived = AvroCodec.derived[Visit].schema
    assertEquals(AvroSchemaJson.render(derived), AvroSchemaJson.render(withoutDocs("visit.avsc")))
    assert(AvroSchemaJson.render(derived).contains("\"doc\"") == false, AvroSchemaJson.render(derived))

  test("its values are written and read back, a union's by the branch it is of"):
    given AvroCodec[Visit] = AvroCodec.derived
    val visit              =
      Visit(
        java.util.UUID.fromString("123e4567-e89b-12d3-a456-426614174000"),
        java.time.Instant.ofEpochMilli(1700000000000L),
        java.time.LocalDate.of(2026, 11, 14),
        "/",
        bytesRead = 7L,
        ratio = 0.5f,
        payload = Array[Byte](1, 2),
        counts = Map("a" -> 1L),
        kind = Kind.LINK,
        agent = Agent("curl"),
        source = Link("mail")
      )
    val back = roundTrip(visit)
    // Arrays are equal only as themselves, so the bytes are compared apart.
    assertEquals(back.map(_.copy(payload = null)), Right(visit.copy(payload = null)))
    assertEquals(back.map(_.payload.toList), Right(List[Byte](1, 2)))

  test("a record of an .avdl file read from the shared directory's canonical path is derived and read back"):
    given AvroCodec[Click] = AvroCodec.derived
    assertEquals(roundTrip(Click("/", 3)), Right(Click("/", 3)))

  test("a decimal is a BigDecimal without its precision and scale, so it has no codec until one is given"):
    assert(compileErrors("AvroCodec.derived[Priced]").contains("BigDecimal"), compileErrors("AvroCodec.derived[Priced]"))

  test("a decimal codec given for BigDecimal makes the file's decimal, and serves every BigDecimal"):
    given AvroCodec[BigDecimal] = AvroCodec.decimal(10, 2)
    assertEquals(AvroSchemaJson.render(AvroCodec.derived[Priced].schema), AvroSchemaJson.render(withoutDocs("priced.avsc")))
    assertEquals(
      AvroCodec[BigDecimal].schema,
      AvroCodec.derived[Priced].schema match
        case AvroSchema.Record(_, fields, _, _) => fields.head.schema;
        case other                              => other
    )

  test("a fixed type is a case class of its bytes, derived as a record, not as the file's fixed"):
    val derived = AvroCodec.derived[Hashed].schema
    assertNotEquals(AvroSchemaJson.render(derived), AvroSchemaJson.render(withoutDocs("hashed.avsc")))
    assert(AvroSchemaJson.render(derived).contains("\"type\":\"record\",\"name\":\"Digest\""), AvroSchemaJson.render(derived))

  test("an enum's default is not carried by avrohugger's enum, so the derived enum has none"):
    val derived = AvroSchemaJson.render(AvroCodec.derived[Graded].schema)
    assertEquals(derived, AvroSchemaJson.render(withoutDocs("graded.avsc")).replace(",\"default\":\"A\"", ""))
    assert(AvroSchemaJson.render(withoutDocs("graded.avsc")).contains(",\"default\":\"A\""))

  test("a bytes default with a character past \\u007f is generated as other bytes than the file's, and the derived default is those"):
    val derived =
      AvroCodec.derived[Blob].schema match
        case AvroSchema.Record(_, fields, _, _) => fields.map(field => field.name -> field.default)
        case other                              => fail(s"expected a record, got $other")
    val file =
      withoutDocs("blob.avsc") match
        case AvroSchema.Record(_, fields, _, _) => fields.map(field => field.name -> field.default)
        case other                              => fail(s"expected a record, got $other")
    assertEquals(derived.head, file.head)
    assertNotEquals(derived(1), file(1))
    assertEquals(derived(1)._2, Some(AvroValue.Bytes(fs2.Chunk[Byte](-61, -65))))
