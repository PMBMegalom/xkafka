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

import java.nio.file.{Files, Path}
import java.time.Instant
import java.util.UUID

import com.fasterxml.jackson.databind.ObjectMapper
import example.json.{Order, Payment}
import example.json.untitled.{Level, Tree}
import io.circe.{jawn, Json}
import io.confluent.kafka.schemaregistry.json.JsonSchema
import munit.FunSuite
import xkafka.jsonschema.JsonSchemaCodec

/** The generated classes' values, as Confluent's JSON Schema support validates them against the files they were generated from. */
class ConfluentJsonCodegenSuite extends FunSuite:
  private val sources = Path.of("modules/codegen-tests/shared/src/main/json-schema")
  private val mapper  = new ObjectMapper()

  private def file(name: String): String = Files.readString(sources.resolve(name))

  private def valid[A](name: String, values: A*)(using codec: JsonSchemaCodec[A]): Unit =
    val schema = new JsonSchema(file(name))
    schema.validate(true)
    values.foreach: value =>
      val text = codec.encode(value).fold(fail(_), _.noSpaces)
      try schema.validate(mapper.readTree(text))
      catch case error: org.everit.json.schema.ValidationException => fail(s"$text: ${error.getAllMessages}")

  test("the schema each generated type keeps is its file's"):
    assertEquals(Some(Order.jsonSchema), jawn.parse(file("order.json")).toOption)
    assertEquals(Some(Payment.jsonSchema), jawn.parse(file("payment.json")).toOption)
    assertEquals(Some(Tree.jsonSchema), jawn.parse(file("tree.json")).toOption)
    assertEquals(Some(Level.jsonSchema), jawn.parse(file("level.schema.json")).toOption)

  test("the values the generated types write are valid against their files, as Confluent validates them"):
    val id     = UUID.fromString("123e4567-e89b-12d3-a456-426614174000")
    val placed = Instant.parse("2024-01-02T01:04:00Z")
    val least  = Order(id = id, placed = placed, lines = Nil, payment = Order.Cash(1), note = None)
    val most   =
      least.copy(
        lines = List(Order.Line("a", 2)),
        refund = Some(Order.Cash(2)),
        note = Some("n"),
        metadata = Some(Json.obj("a" -> Json.True)),
        code = Some(7L),
        measure = Some(5.5),
        shipping = Order.Shipping("s", "c", Some(3.5))
      )
    valid("order.json", least, most, most.copy(code = Some("x"), measure = Some(5L)))
    valid[Payment]("payment.json", Payment.Cash(1), Payment.Card(2, Some("1111")), Payment.Card(3, None))
    valid("tree.json", Tree(1, List(Tree(2))))
    valid("level.schema.json", Level.values.toList*)

  test("Confluent refuses what the files refuse, so the validation above can fail"):
    val schema = new JsonSchema(file("order.json"))
    val text   = Order(id = UUID.randomUUID, placed = Instant.EPOCH, lines = Nil, payment = Order.Cash(1), note = None)
    val json   = summon[JsonSchemaCodec[Order]].encode(text).fold(fail(_), identity)
    val broken = json.mapObject(_.add("opens", Json.fromString("10:00+01:00")))
    assert(scala.util.Try(schema.validate(mapper.readTree(broken.noSpaces))).isFailure, "a time without its seconds")
    assert(scala.util.Try(schema.validate(mapper.readTree(json.mapObject(_.remove("kind")).noSpaces))).isFailure, "no kind")
    // A oneOf's value must be of exactly one branch, and a card is also a value of the open Cash.
    val card = summon[JsonSchemaCodec[Order]].encode(text.copy(payment = Order.Card(1, "4242"))).fold(fail(_), identity)
    assert(scala.util.Try(schema.validate(mapper.readTree(card.noSpaces))).isFailure, "a card")
