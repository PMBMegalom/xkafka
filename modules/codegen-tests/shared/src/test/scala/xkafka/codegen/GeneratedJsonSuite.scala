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

package xkafka.codegen

import java.time.{Instant, LocalDate, OffsetTime, ZoneOffset}
import java.util.UUID

import example.json.{Order, Payment}
import example.json.decimal.Price
import example.json.untitled.{Level, Tree}
import fs2.Chunk
import io.circe.{jawn, Json}
import munit.FunSuite
import xkafka.jsonschema.{JsonSchemaCodec, JsonSchemaFormat}

/** Values of the classes xkafka-codegen generated from the JSON Schema files in src/main/json-schema, written and read with the schemas they were
  * generated from.
  */
class GeneratedJsonSuite extends FunSuite:
  private def json(text: String): Json = jawn.parse(text).fold(error => fail(error.message), identity)

  private def written[A](value: A)(using codec: JsonSchemaCodec[A]): Json = codec.encode(value).fold(fail(_), identity)

  private def read[A](text: String)(using codec: JsonSchemaCodec[A]): Either[String, A] = codec.decode(json(text))

  private def roundTrip[A](value: A)(using JsonSchemaCodec[A]): Either[String, A] =
    val format = JsonSchemaFormat[A]
    for
      write <- format.writer(format.schema)
      bytes <- write(value)
      read  <- format.reader(format.schema)
      back  <- read(bytes)
    yield back

  private val id     = UUID.fromString("123e4567-e89b-12d3-a456-426614174000")
  private val placed = Instant.parse("2024-01-02T01:04:05Z")

  private val order =
    Order(
      id = id,
      placed = placed,
      day = LocalDate.of(2025, 3, 4),
      opens = OffsetTime.of(10, 0, 0, 0, ZoneOffset.ofHours(-5)),
      status = Order.Status.InProgress,
      lines = List(Order.Line("apple", 3), Order.Line("pear")),
      payment = Order.Card(10.5, "4242"),
      refund = Some(Order.Cash(2)),
      note = Some("ring twice"),
      tags = List("x"),
      counts = Map("y" -> 2L),
      metadata = Some(Json.obj("source" -> Json.fromString("web"))),
      ratio = 0.25,
      blob = Chunk[Byte](1, 2, 3),
      firstName = "Ada",
      `type` = "kind",
      copy_ = 9,
      code = Some(7L),
      measure = Some(5.5),
      size = Some(Order.Large(1, Some(2))),
      shipping = Order.Shipping("Via Po 2", "Turin", Some(3.25))
    )

  test("each file's type registers the file's schema, named by its title"):
    assertEquals(JsonSchemaFormat[Order].schema.text, Order.jsonSchema.noSpaces)
    assertEquals(JsonSchemaFormat[Payment].schema.text, Payment.jsonSchema.noSpaces)
    assertEquals(JsonSchemaFormat[Order].recordName, Some("example.json.Order"))
    assertEquals(JsonSchemaFormat[Tree].recordName, Some("Tree"))
    // A file that names no draft is registered as draft 7, the draft Confluent's serializers take by default.
    val draft7 = "$schema" -> Json.fromString("http://json-schema.org/draft-07/schema#")
    assertEquals(JsonSchemaFormat[Level].schema.text, Json.fromFields(draft7 :: Level.jsonSchema.asObject.toList.flatMap(_.toList)).noSpaces)
    assertEquals(JsonSchemaFormat[Level].recordName, None)

  test("fields take the file's defaults, and optional ones none"):
    val defaults = Order(id = id, placed = placed, lines = Nil, payment = Order.Cash(1), note = None)
    assertEquals((defaults.day, defaults.opens), (LocalDate.of(2024, 1, 2), OffsetTime.of(9, 0, 0, 0, ZoneOffset.ofHours(1))))
    assertEquals((defaults.status, defaults.tags, defaults.counts), (Order.Status.`new`, List("a", "b"), Map("x" -> 1L)))
    assertEquals((defaults.ratio, defaults.blob, defaults.firstName), (0.5, Chunk[Byte](0, -1), "say \"hi\"\n"))
    assertEquals((defaults.`type`, defaults.copy_, defaults.shipping), ("keyword", 2L, Order.Shipping("Via Roma 1", "Rome")))
    assertEquals((defaults.refund, defaults.metadata, defaults.code, defaults.size), (None, None, None, None))
    assertEquals(Order.Line("x").quantity, 1L)
    assertEquals(Tree(1).children, Nil)

  test("values read back as they were written"):
    assertEquals(roundTrip(order), Right(order))
    assertEquals(roundTrip[Payment](Payment.Card(1, Some("1111"))), Right(Payment.Card(1, Some("1111"))))
    assertEquals(roundTrip[Payment](Payment.Cash(3)), Right(Payment.Cash(3)))
    assertEquals(roundTrip(Tree(1, List(Tree(2), Tree(3, List(Tree(4)))))), Right(Tree(1, List(Tree(2), Tree(3, List(Tree(4)))))))
    Level.values.foreach(level => assertEquals(roundTrip(level), Right(level)))

  test("properties are written under the file's names, optional ones left out where they have no value, and nullable ones written as null"):
    val written = this.written(Order(id = id, placed = placed, lines = Nil, payment = Order.Cash(1), note = None)).asObject.get
    assertEquals(written("kind"), Some(Json.fromString("order")))
    assertEquals(written("note"), Some(Json.Null))
    assertEquals((written("refund"), written("metadata"), written("code")), (None, None, None))
    assertEquals(
      (written("first-name"), written("type"), written("copy")),
      (Some(Json.fromString("say \"hi\"\n")), Some(Json.fromString("keyword")), Some(Json.fromLong(2)))
    )
    assertEquals(
      (written("status"), written("blob"), written("opens")),
      (Some(Json.fromString("new")), Some(Json.fromString("AP8=")), Some(Json.fromString("09:00:00+01:00")))
    )
    assertEquals(written("placed"), Some(Json.fromString("2024-01-02T01:04:05Z")))
    assertEquals(this.written[Payment](Payment.Cash(3)), json("""{"kind":"cash","amount":3.0}"""))

  private val least =
    s"""{"kind":"order","id":"$id","placed":"2024-01-02T03:04:05+02:00","lines":[{"sku":"a"}],"payment":{"amount":1},"note":null,"shipping":{"street":"s"}}"""

  test("properties left out take their defaults, or none, and others than the file's are ignored"):
    assertEquals(
      read[Order](least),
      Right(Order(id = id, placed = placed, lines = List(Order.Line("a")), payment = Order.Cash(1), note = None, shipping = Order.Shipping("s")))
    )
    assertEquals(
      read[Order](least.replace("\"note\":null", "\"note\":\"n\",\"refund\":null,\"unknown\":1")).map(found => (found.note, found.refund)),
      Right((Some("n"), None))
    )
    assert(read[Order](least.replace("\"note\":null,", "")).left.exists(_.contains("no property 'note'")))
    assert(read[Order](least.replace("\"kind\":\"order\"", "\"kind\":\"other\"")).isLeft)
    assertEquals(read[Order](least.replace("03:04:05+02:00", "01:04:05z").replace("T", "t")).map(_.placed), Right(placed))

  test("a union's value is read by the one branch that reads it without properties its schema lacks"):
    def payment(text: String) = read[Order](least.replace("\"payment\":{\"amount\":1}", s"\"payment\":$text")).map(_.payment)
    assertEquals(payment("""{"amount":1,"last4":"4242"}"""), Right(Order.Card(1, "4242")))
    assertEquals(payment("""{"amount":1}"""), Right(Order.Cash(1)))
    def size(text: String) = read[Order](least.replace("\"note\":null", s"\"note\":null,\"size\":$text")).map(_.size)
    assertEquals(size("""{"n":1,"m":2}"""), Right(Some(Order.Large(1, Some(2)))))

  test("a oneOf's value that several branches read fails, and an anyOf's takes the first"):
    def payment(text: String) = read[Order](least.replace("\"payment\":{\"amount\":1}", s"\"payment\":$text")).map(_.payment)
    // Cash is open, so a card with a property neither branch names is read by both.
    assert(
      payment("""{"amount":1,"last4":"4242","extra":true}""").left.exists(_.contains("exactly one branch")),
      payment("""{"amount":1,"last4":"4242","extra":true}""")
    )
    def with_(property: String, text: String) = read[Order](least.replace("\"note\":null", s"\"note\":null,\"$property\":$text"))
    assert(with_("size", """{"n":1}""").left.exists(_.contains("Order.Small, Order.Large")), with_("size", """{"n":1}"""))
    assertEquals(with_("measure", "5").map(_.measure), Right(Some(5L)))
    assertEquals(with_("measure", "5.5").map(_.measure), Right(Some(5.5)))
    def exact(text: String) = read[Order](least.replace("\"street\":\"s\"", s"\"street\":\"s\",\"exact\":$text")).map(_.shipping.exact)
    assert(exact("5").left.exists(_.contains("exactly one branch")), exact("5"))
    assertEquals(exact("5.5"), Right(Some(5.5)))
    assertEquals((with_("code", "\"x\"").map(_.code), with_("code", "7").map(_.code)), (Right(Some("x")), Right(Some(7L))))

  test("a union's value is written as its branch writes it"):
    val written = this.written(order.copy(measure = Some(5L), code = Some("x"))).asObject.get
    assertEquals((written("measure"), written("code")), (Some(Json.fromLong(5)), Some(Json.fromString("x"))))
    assertEquals(roundTrip(order.copy(measure = Some(5L))).map(_.measure), Right(Some(5L)))
    assertEquals(written("payment"), Some(json("""{"amount":10.5,"last4":"4242"}""")))

  test("a file that is a oneOf of objects is a sealed trait, told apart by its constants"):
    assertEquals(read[Payment]("""{"kind":"card","amount":1}"""), Right(Payment.Card(1, None)))
    assertEquals(read[Payment]("""{"kind":"cash","amount":1}"""), Right(Payment.Cash(1)))
    assert(read[Payment]("""{"amount":1}""").isLeft)

  test("an enum's values are named in Scala, and written as the file's"):
    assertEquals(Order.Status.values.toList, List(Order.Status.`new`, Order.Status.InProgress, Order.Status.paid, Order.Status.values_))
    val statuses = Order.Status.values.toList.map(status => written(order.copy(status = status)).asObject.flatMap(_("status")))
    assertEquals(statuses, List("new", "in-progress", "paid", "values").map(text => Some(Json.fromString(text))))
    assertEquals(
      Order.Status.values.toList.map(status => roundTrip(order.copy(status = status)).map(_.status)),
      Order.Status.values.toList.map(Right(_))
    )
    assert(read[Level]("\"medium\"").isLeft)

  test("numbers are BigDecimal where the generator is told so, and keep every digit"):
    val price = Price(BigDecimal("12345678901234567.89"))
    assertEquals(price.discount, BigDecimal("0.10"))
    assertEquals(roundTrip(price), Right(price))
    assertEquals(written(price), json("""{"amount":12345678901234567.89,"discount":0.10}"""))
