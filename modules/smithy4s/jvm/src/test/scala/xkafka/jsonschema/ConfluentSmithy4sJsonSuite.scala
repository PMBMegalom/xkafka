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

import scala.jdk.CollectionConverters.*

import cats.effect.IO
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import example.shop.*
import fs2.Chunk
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.schemaregistry.json.{JsonSchema, JsonSchemaProvider, JsonSchemaUtils}
import io.confluent.kafka.serializers.json.{KafkaJsonSchemaDeserializer, KafkaJsonSchemaSerializer}
import munit.CatsEffectSuite
import smithy4s.Schema as SmithySchema
import xkafka.{Headers, Topic}
import xkafka.schemaregistry.Part

/** The schemas of smithy4s's JSON, and the JSON smithy4s writes, against Confluent's JSON Schema support. */
class ConfluentSmithy4sJsonSuite extends CatsEffectSuite:
  import Shop.*

  private val mapper = new ObjectMapper()
  private val topic  = Topic.from("orders").toOption.get

  private def confluent[A](using smithy: SmithySchema[A]): JsonSchema = new JsonSchema(Smithy4sJsonSchema.of(smithy).noSpaces)

  private def written[A: SmithySchema](value: A): JsonNode = mapper.readTree(smithy4s.json.Json.writeBlob(value).toArray)

  private def valid[A: SmithySchema](values: A*): Unit =
    val schema = confluent[A]
    schema.validate(true)
    values.foreach: value =>
      try schema.validate(written(value))
      catch case error: org.everit.json.schema.ValidationException => fail(s"${written(value)}: ${error.getAllMessages}")

  private def refused[A: SmithySchema](value: A)(change: JsonNode => JsonNode): Unit =
    val node = change(written(value))
    assert(scala.util.Try(confluent[A].validate(node)).isFailure, s"$node")

  test("the JSON smithy4s writes is valid against its schema, as Confluent validates"):
    valid(least, most, most.copy(payment = Some(Payment.cash(Cash(1))), code = Some(Code.text("x")), note = Some(smithy4s.Nullable.Value("n"))))
    valid(events*)
    valid(forest)
    valid(Node(1, Some(List(Node(2)))))

  test("Confluent refuses what the schema refuses: values out of their constraints, and unions of other shapes"):
    def set(path: String, value: String)(node: JsonNode): JsonNode =
      node.asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode].set[JsonNode](path, mapper.readTree(value))
    refused(most)(set("discount", "101"))
    refused(most)(set("currency", "\"eur\""))
    refused(most)(set("lines", "[]"))
    refused(most)(set("payment", """{"cash":{"amount":1},"card":{"amount":1,"last4":"1"}}"""))
    refused(most)(set("status", "\"lost\""))
    refused(most)(set("counts", (1 to 11).map(n => s""""k$n":$n""").mkString("{", ",", "}")))
    refused(least)(node => node.asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode].without[JsonNode]("id"))
    refused(events.head)(node => node.asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode].without[JsonNode]("kind"))

  test("Confluent's deserializer reads what the serde writes, and the serde reads what Confluent's serializer writes, checking it"):
    val mock         = new MockSchemaRegistryClient(List(new JsonSchemaProvider()).asJava)
    val serializer   = new KafkaJsonSchemaSerializer[JsonNode](mock)
    val deserializer = new KafkaJsonSchemaDeserializer[Object](mock)
    serializer
      .configure(Map("schema.registry.url" -> "mock://xkafka", "auto.register.schemas" -> "true", "json.fail.invalid.schema" -> "true").asJava, false)
    deserializer.configure(Map("schema.registry.url" -> "mock://xkafka", "json.fail.invalid.schema" -> "true").asJava, false)
    for
      serde  <- Smithy4sJsonSerde.of[IO, Order](ConfluentJsonRegistry.bridged(mock), Part.Value)
      ours   <- serde.serializer.serialize(topic, Headers.empty, most)
      read   <- IO.blocking(deserializer.deserialize("orders", ours.get.toArray))
      theirs <- IO.blocking(serializer.serialize("orders", JsonSchemaUtils.envelope(confluent[Order], written(least))))
      back   <- serde.deserializer.deserialize(topic, Headers.empty, Some(Chunk.array(theirs)))
    yield
      // Confluent's deserializer fills in the schema's defaults, which smithy4s reads back as the same order.
      assertEquals(smithy4s.json.Json.read[Order](smithy4s.Blob(mapper.writeValueAsBytes(read))), Right(most))
      assertEquals(back, least)
      assertEquals(mock.getAllSubjects.asScala.toList, List("orders-value"))
