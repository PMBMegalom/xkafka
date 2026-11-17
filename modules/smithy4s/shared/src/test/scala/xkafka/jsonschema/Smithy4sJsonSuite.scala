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

package xkafka.jsonschema

import java.util.UUID

import scala.concurrent.duration.*

import example.shop.*
import io.circe.{jawn, Json}
import munit.FunSuite
import smithy4s.{Blob, Document, Nullable, Schema as SmithySchema}
import smithy4s.time.{LocalDate, LocalTime, OffsetDateTime, Timestamp}

/** Values of the types smithy4s generates from src/test/smithy, as the tests write them. */
object Shop:
  val least: Order = Order(UUID.fromString("123e4567-e89b-12d3-a456-426614174000"), Timestamp(1700000000L, 0), List(Line("a")))

  val most: Order =
    least.copy(
      status = Status.PAID,
      flag = true,
      shipped = Some(Timestamp(1700000001L, 500000000)),
      billed = Some(Timestamp(1700000002L, 0)),
      discount = Some(5),
      firstName = Some("Ada"),
      payment = Some(Payment.card(Card(10.5, "4242"))),
      tags = Some(Set("x", "y")),
      counts = Some(Map("y" -> 2L)),
      blob = Some(Blob("hi")),
      currency = Some("EUR"),
      meta = Some(Document.obj("a" -> Document.fromBoolean(true))),
      priority = Some(Priority.HIGH),
      ratio = Some(0.25),
      big = Some(BigDecimal("12345678901234567.89")),
      huge = Some(BigInt("123456789012345678901234567890")),
      small = Some(1.toByte),
      short = Some(2.toShort),
      day = Some(LocalDate.parseUnsafe("2024-01-02")),
      opens = Some(LocalTime.parseUnsafe("09:30:00")),
      _wait = Some(90.seconds),
      at = Some(OffsetDateTime.parseUnsafe("2024-01-02T03:04:05+02:00")),
      code = Some(Code.number(7)),
      sparse = Some(List(Nullable.Null, Nullable.Value("a"))),
      previous = Some(least),
      note = Some(Nullable.Null)
    )

  val events: List[Event] = List(Event.created(Created("o-1")), Event.cancelled(Cancelled("o-1", Some("late"))))

  val forest: Forest = Forest(List(Node(1, Some(List(Node(2), Node(3, Some(List(Node(4)))))))))

/** Types smithy4s generates, written and read with the JSON Schema of the JSON smithy4s writes. */
class Smithy4sJsonSuite extends FunSuite:
  import Shop.*

  private def schema[A](using smithy: SmithySchema[A]): Json = Smithy4sJsonSchema.of(smithy)

  private def at(json: Json, path: String*): Json =
    path.foldLeft(json)((inside, key) => inside.hcursor.downField(key).focus.getOrElse(fail(s"no $key in ${inside.noSpaces}")))

  private def roundTrip[A: SmithySchema](value: A): Either[String, A] =
    val format = Smithy4sJsonFormat[A]
    for
      write <- format.writer(format.schema)
      bytes <- write(value)
      read  <- format.reader(format.schema)
      back  <- read(bytes)
    yield back

  test("a record nested deeper than xkafka reads JSON is refused before smithy4s reads it, which counts no depth and would run the stack out"):
    val reader = Smithy4sJsonFormat[Order].reader(Smithy4sJsonFormat[Order].schema).fold(fail(_), identity)
    val text   = s"""{"meta":${"[" * 100000}${"]" * 100000}}"""
    // The JSON parser refuses depths far past the limit itself, and xkafka the rest.
    val refused = reader(fs2.Chunk.array(text.getBytes("UTF-8")))
    assert(refused.left.exists(reason => reason.contains("nested more than 100 deep") || reason.contains("max depth")), refused)
    val past = s"""{"meta":${"[" * 150}${"]" * 150}}"""
    assertEquals(reader(fs2.Chunk.array(past.getBytes("UTF-8"))).left.toOption, Some("the record is nested more than 100 deep"))

  test("nothing nested deeper than reading takes is written"):
    val writer                       = Smithy4sJsonFormat[Order].writer(Smithy4sJsonFormat[Order].schema).fold(fail(_), identity)
    def lists(levels: Int): Document = (0 until levels).foldLeft(Document.nullDoc)((inner, _) => Document.array(inner))
    assertEquals(writer(least.copy(meta = Some(lists(150)))).left.toOption, Some("the record is nested more than 100 deep"))
    assert(writer(least.copy(meta = Some(lists(50)))).isRight)

  test("the format registers the schema of smithy4s's JSON, titled by the shape's full name"):
    assertEquals(Smithy4sJsonFormat[Order].schema.text, schema[Order].noSpaces)
    assertEquals(Smithy4sJsonFormat[Order].recordName, Some("example.shop.Order"))
    assertEquals(at(schema[Order], "title"), Json.fromString("example.shop.Order"))
    assertEquals(at(schema[Order], "description"), Json.fromString("An order, as the shop writes it."))

  test("values are written as smithy4s writes them, and read back"):
    val written = Smithy4sJsonFormat[Order].writer(Smithy4sJsonFormat[Order].schema).flatMap(_(most)).map(_.toList)
    assertEquals(written, Right(smithy4s.json.Json.writeBlob(most).toArray.toList))
    assertEquals(roundTrip(most), Right(most))
    assertEquals(roundTrip(least), Right(least))
    events.foreach(event => assertEquals(roundTrip(event), Right(event)))
    assertEquals(roundTrip(forest), Right(forest))

  test("properties left out take their defaults, or none, and others are ignored"):
    val text   = """{"id":"123e4567-e89b-12d3-a456-426614174000","placed":1700000000,"lines":[{"sku":"a"}],"unknown":1}"""
    val format = Smithy4sJsonFormat[Order]
    assertEquals(format.reader(format.schema).flatMap(_(fs2.Chunk.array(text.getBytes("UTF-8")))), Right(least))

  test("a struct's required members are required, and others may be left out or null, with their defaults"):
    val order = schema[Order]
    assertEquals(at(order, "required"), Json.arr(Json.fromString("id"), Json.fromString("placed"), Json.fromString("lines")))
    assertEquals(
      at(order, "properties", "id"),
      jawn.parse("""{"description":"Who knows the order by it.","type":"string","format":"uuid"}""").toOption.get
    )
    assertEquals(at(order, "properties", "status", "default"), Json.fromString("new"))
    assertEquals(at(order, "properties", "status", "anyOf"), jawn.parse("""[{"type":"null"},{"type":"string","enum":["new","paid"]}]""").toOption.get)
    assertEquals(at(order, "properties", "first-name", "anyOf"), jawn.parse("""[{"type":"null"},{"type":"string"}]""").toOption.get)
    assertEquals(at(order, "properties", "priority", "anyOf"), jawn.parse("""[{"type":"null"},{"type":"integer","enum":[1,2]}]""").toOption.get)

  test("a required member with a default is required only where smithy4s writes it, so a value equal to its default fits the schema"):
    val written  = jawn.parse(new String(smithy4s.json.Json.writeBlob(Counter(0)).toArray, "UTF-8")).toOption.get
    val required = at(schema[Counter], "required").asArray.toList.flatten.flatMap(_.asString)
    assertEquals(required.filterNot(name => written.hcursor.downField(name).succeeded), Nil, written.noSpaces)
    assertEquals(required, List("count"))
    assertEquals(written, Json.obj("count" -> Json.fromInt(0)))

  test("timestamps are numbers of seconds, or text, as their format has them, and smithy4s's other times as it writes them"):
    def kind(property: String) = at(schema[Order], "properties", property, "anyOf").asArray.flatMap(_.lastOption).getOrElse(fail(property))
    assertEquals(at(schema[Order], "properties", "placed"), Json.obj("type" -> Json.fromString("number")))
    assertEquals(kind("shipped"), Json.obj("type" -> Json.fromString("string"), "format" -> Json.fromString("date-time")))
    assertEquals(kind("billed"), Json.obj("type" -> Json.fromString("string")))
    assertEquals(kind("day"), Json.obj("type" -> Json.fromString("string"), "format" -> Json.fromString("date")))
    assertEquals(kind("opens"), Json.obj("type" -> Json.fromString("string")))
    assertEquals(kind("wait"), Json.obj("type" -> Json.fromString("number")))
    assertEquals(kind("at"), Json.obj("type" -> Json.fromString("string"), "format" -> Json.fromString("date-time")))
    assertEquals(kind("blob"), Json.obj("type" -> Json.fromString("string"), "contentEncoding" -> Json.fromString("base64")))

  test("@length, @range and @pattern are JSON Schema's keywords of the value they constrain"):
    def kind(property: String) = at(schema[Order], "properties", property, "anyOf").asArray.flatMap(_.lastOption).getOrElse(fail(property))
    assertEquals(
      (at(schema[Order], "properties", "lines", "minItems"), at(schema[Order], "properties", "lines", "maxItems")),
      (Json.fromInt(1), Json.fromInt(50))
    )
    assertEquals(kind("discount"), jawn.parse("""{"type":"integer","minimum":0,"maximum":100}""").toOption.get)
    assertEquals(kind("currency"), jawn.parse("""{"type":"string","pattern":"^[A-Z]{3}$","minLength":3,"maxLength":3}""").toOption.get)
    assertEquals(kind("counts"), jawn.parse("""{"type":"object","additionalProperties":{"type":"integer"},"maxProperties":10}""").toOption.get)
    assertEquals(kind("tags"), jawn.parse("""{"type":"array","items":{"type":"string"},"uniqueItems":true}""").toOption.get)

  test("unions are objects of one member, or carry their member's name in their discriminator, or are any of their members untagged"):
    assertEquals(
      at(schema[Order], "properties", "payment", "anyOf").asArray.flatMap(_.lastOption).map(at(_, "oneOf")).flatMap(_.asArray).flatMap(_.headOption),
      jawn.parse(
        """{"type":"object","properties":{"cash":{"type":"object","properties":{"amount":{"type":"number"}},"required":["amount"]}},"required":["cash"],"additionalProperties":false}"""
      ).toOption
    )
    assertEquals(
      at(schema[Event], "oneOf").asArray.flatMap(_.headOption).map(at(_, "allOf")).flatMap(_.asArray).flatMap(_.lastOption),
      jawn.parse("""{"type":"object","properties":{"kind":{"const":"created"}},"required":["kind"]}""").toOption
    )
    assertEquals(
      at(schema[Order], "properties", "code", "anyOf").asArray.flatMap(_.lastOption),
      jawn.parse("""{"anyOf":[{"type":"string"},{"type":"integer"}]}""").toOption
    )

  test("a shape that holds itself refers to the root, or to its definition"):
    assertEquals(
      at(schema[Node], "properties", "children", "anyOf").asArray.flatMap(_.lastOption),
      jawn.parse("""{"type":"array","items":{"$ref":"#"}}""").toOption
    )
    val forest = schema[Forest]
    assertEquals(
      at(forest, "properties", "trees", "items"),
      Json.obj("type" -> Json.fromString("object")).deepMerge(at(forest, "properties", "trees", "items"))
    )
    assertEquals(
      at(forest, "definitions", "example.shop.Node", "properties", "children", "anyOf").asArray.flatMap(_.lastOption),
      jawn.parse("""{"type":"array","items":{"$ref":"#/definitions/example.shop.Node"}}""").toOption
    )
