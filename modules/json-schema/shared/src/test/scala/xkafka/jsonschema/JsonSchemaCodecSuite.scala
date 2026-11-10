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

import java.time.{Instant, LocalDate}
import java.util.UUID

import cats.data.NonEmptyList
import cats.effect.IO
import fs2.Chunk
import io.circe.Json
import io.circe.parser.parse
import munit.CatsEffectSuite
import xkafka.{Headers, Topic}
import xkafka.schemaregistry.*

final case class Line(sku: String, quantity: Int = 1) derives JsonSchemaCodec

@SchemaName("Line")
final case class LineBefore(sku: String) derives JsonSchemaCodec

enum Status derives JsonSchemaCodec:
  case Open, Paid

sealed trait Payment derives JsonSchemaCodec
final case class Card(last4: String) extends Payment
case object Cash                     extends Payment
final case class Transfer(
    iban: String,
    @SchemaDoc("When it cleared")
    cleared: Option[Instant] = None
) extends Payment

@SchemaNamespace("com.example") @SchemaDoc("An order")
final case class Order(
    id: UUID,
    @SchemaName("placed_at")
    placedAt: Instant,
    day: LocalDate,
    lines: List[Line],
    status: Status,
    payment: Payment,
    note: Option[String] = None,
    tags: Map[String, Long] = Map.empty,
    @SchemaAlias("attempts")
    retries: Int = 0,
    raw: Chunk[Byte] = Chunk.empty
) derives JsonSchemaCodec

final case class Node(value: Int, next: Option[Node] = None) derives JsonSchemaCodec

final case class Maybe(note: Option[String]) derives JsonSchemaCodec

final case class Clash(`type`: String) extends Shape
sealed trait Shape derives JsonSchemaCodec

class JsonSchemaCodecSuite extends CatsEffectSuite:
  private def json(text: String): Json = parse(text).fold(error => fail(error.message), identity)

  private val order =
    Order(
      UUID.fromString("3f2b8c1e-9a4d-4e6f-8b2a-1c3d5e7f9a0b"),
      Instant.parse("2026-11-10T07:00:00.123Z"),
      LocalDate.of(2026, 11, 10),
      List(Line("apple", 3), Line("pear")),
      Status.Paid,
      Transfer("DE00", Some(Instant.parse("2026-11-11T00:00:00Z"))),
      Some("ring twice"),
      Map("priority" -> 2L),
      1,
      Chunk[Byte](1, 2, 3)
    )

  test("a case class is a closed object of its fields, those without a default required"):
    assertEquals(
      JsonSchemaCodec[Line].schema,
      json(
        """{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","title":"xkafka.jsonschema.Line",
             |"properties":{"sku":{"type":"string"},"quantity":{"type":"integer","default":1}},"required":["sku"],"additionalProperties":false}"""
          .stripMargin
      )
    )

  test("an optional field is oneOf null and its type, and may be left out"):
    assertEquals(
      JsonSchemaCodec[Maybe].schema,
      json("""{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","title":"xkafka.jsonschema.Maybe",
             |"properties":{"note":{"oneOf":[{"type":"null"},{"type":"string"}]}},"required":[],"additionalProperties":false}""".stripMargin)
    )
    assertEquals(JsonSchemaCodec[Maybe].decode(json("{}")), Right(Maybe(None)))
    assertEquals(JsonSchemaCodec[Maybe].encode(Maybe(None)), Right(json("""{"note":null}""")))

  test("an enum of objects is text, one of its cases' names"):
    assertEquals(
      JsonSchemaCodec[Status].schema,
      json("""{"$schema":"http://json-schema.org/draft-07/schema#","type":"string","title":"xkafka.jsonschema.Status","enum":["Open","Paid"]}""")
    )
    assertEquals(JsonSchemaCodec[Status].encode(Status.Paid), Right(Json.fromString("Paid")))

  test("a sealed trait is oneOf its cases' objects, each telling its case in the property type"):
    val schema = JsonSchemaCodec[Payment].schema
    assertEquals(
      schema.hcursor.downField("oneOf").downN(1).focus,
      Some(json(
        """{"type":"object","title":"xkafka.jsonschema.Cash","properties":{"type":{"const":"Cash"}},"required":["type"],"additionalProperties":false}"""
      ))
    )
    assertEquals(JsonSchemaCodec[Payment].encode(Card("4242")), Right(json("""{"type":"Card","last4":"4242"}""")))
    assertEquals(JsonSchemaCodec[Payment].decode(json("""{"last4":"1","type":"Card"}""")), Right(Card("1")))
    assertEquals(JsonSchemaCodec[Payment].decode(json("""{"type":"Cash"}""")), Right(Cash))

  test("a case with a field named type, which tells the cases apart, is refused"):
    assertEquals(
      JsonSchemaCodec[Shape].encode(Clash("x")),
      Left("case Clash of xkafka.jsonschema.Shape has a field named 'type', which tells the cases apart")
    )

  test("values of every kind read back as they were written, times in their ISO-8601 forms"):
    val written = JsonSchemaCodec[Order].encode(order)
    assertEquals(written.flatMap(JsonSchemaCodec[Order].decode), Right(order))
    assertEquals(written.map(_.hcursor.get[String]("placed_at")), Right(Right("2026-11-10T07:00:00.123Z")))
    assertEquals(written.map(_.hcursor.get[String]("day")), Right(Right("2026-11-10")))
    assertEquals(written.map(_.hcursor.get[String]("raw")), Right(Right("AQID")))
    val plain = order.copy(payment = Cash, note = None)
    assertEquals(JsonSchemaCodec[Order].encode(plain).flatMap(JsonSchemaCodec[Order].decode), Right(plain))

  test("a record lacking fields reads them as their defaults or none, reads a field under an alias, and ignores properties it does not know"):
    val read =
      JsonSchemaCodec[Order]
        .decode(json("""{"id":"3f2b8c1e-9a4d-4e6f-8b2a-1c3d5e7f9a0b","placed_at":"2026-11-10T07:00:00Z","day":"2026-11-10","lines":[{"sku":"fig"}],
        |"status":"Open","payment":{"type":"Cash"},"attempts":4,"unknown":{"anything":true}}""".stripMargin))
    assertEquals(read.map(value => (value.lines, value.note, value.tags, value.retries)), Right((List(Line("fig")), None, Map.empty, 4)))

  test("a record lacking a required field is refused, naming it"):
    assertEquals(JsonSchemaCodec[Line].decode(json("""{"quantity":2}""")), Left("xkafka.jsonschema.Line: no field 'sku'"))

  test("a type that holds itself is a reference to its definition"):
    val schema = JsonSchemaCodec[Node].schema
    assertEquals(
      schema.hcursor.downField("properties").downField("next").downField("oneOf").downN(1).focus,
      Some(json("""{"$ref":"#/definitions/xkafka.jsonschema.Node"}"""))
    )
    assert(schema.hcursor.downField("definitions").downField("xkafka.jsonschema.Node").focus.isDefined, schema.spaces2)
    assertEquals(JsonSchemaCodec[Node].encode(Node(1, Some(Node(2)))).flatMap(JsonSchemaCodec[Node].decode), Right(Node(1, Some(Node(2)))))

  test("numbers JSON cannot hold are refused"):
    assertEquals(JsonSchemaCodec[Double].encode(Double.NaN), Left("NaN is not a number JSON can hold"))

  test("serdes write the schema's id and then the JSON, and read records written before a field was added"):
    for
      fake   <- FakeRegistry()
      client <-
        SchemaRegistry.of(RegistrySettings.from(NonEmptyList.one("http://registry:8081")).toOption.get, fake.transport("http://registry:8081"))
      topic   <- IO.fromEither(Topic.from("lines").left.map(error => new IllegalArgumentException(error.message)))
      before  <- JsonSchemaSerde.of[IO, LineBefore](client, Part.Value).flatMap(_.serializer.serialize(topic, Headers.empty, LineBefore("apple")))
      read    <- JsonSchemaSerde.of[IO, Line](client, Part.Value).flatMap(_.deserializer.deserialize(topic, Headers.empty, before))
      written <- JsonSchemaSerde.asWritten[IO](client).flatMap(_.deserialize(topic, Headers.empty, before))
      subject <- client.latest(Subject.from("lines-value").toOption.get)
    yield
      assertEquals(before.map(_.drop(5).toArray.toList), Some("""{"sku":"apple"}""".getBytes.toList))
      assertEquals(read, Line("apple", 1))
      assertEquals(written, JsonSchemaWritten(JsonSchemaCodec[LineBefore].schema, json("""{"sku":"apple"}""")))
      assertEquals(subject.map(_.schema.schemaType), Some(SchemaType.Json))
