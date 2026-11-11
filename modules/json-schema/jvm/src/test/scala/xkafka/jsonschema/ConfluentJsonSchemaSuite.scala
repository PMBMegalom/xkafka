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

import scala.jdk.CollectionConverters.*

import cats.effect.IO
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import fs2.Chunk
import io.circe.Json
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import io.confluent.kafka.schemaregistry.json.{JsonSchema, JsonSchemaUtils}
import io.confluent.kafka.serializers.json.{KafkaJsonSchemaDeserializer, KafkaJsonSchemaSerializer}
import munit.CatsEffectSuite
import xkafka.{Headers, Topic}
import xkafka.schemaregistry.{Compatibility, Part, Registered, Schema, SchemaId, SchemaRegistry, SchemaType, Subject}

final case class Grown(sku: String, quantity: Int = 1, colour: Option[String] = None) derives JsonSchemaCodec

/** xkafka's JSON Schema schemas, values and serdes against Confluent's JSON Schema support. */
class ConfluentJsonSchemaSuite extends CatsEffectSuite:
  private val mapper = new ObjectMapper()
  private val topic  = Topic.from("orders").toOption.get

  private def node(json: Json): JsonNode = mapper.readTree(json.noSpaces)

  private def confluent[A](codec: JsonSchemaCodec[A]): JsonSchema = new JsonSchema(codec.schema.noSpaces)

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

  test("Confluent takes every derived schema, and finds every value xkafka writes valid against it"):
    def valid[A](codec: JsonSchemaCodec[A], values: A*): Unit =
      val schema = confluent(codec)
      schema.validate(true)
      values.foreach(value => schema.validate(node(codec.encode(value).fold(fail(_), identity))))
    valid(JsonSchemaCodec[Order], order, order.copy(payment = Cash, note = None), order.copy(payment = Card("1"), lines = Nil))
    valid(JsonSchemaCodec[Payment], Card("1"), Cash, Transfer("x"))
    valid(JsonSchemaCodec[Status], Status.Open)
    valid(JsonSchemaCodec[Node], Node(1, Some(Node(2, Some(Node(3))))))

  test("Confluent finds a value that breaks a derived schema invalid, so its validation reads the schema as xkafka means it"):
    val schema = confluent(JsonSchemaCodec[Line])
    List("""{"quantity":1}""", """{"sku":"a","colour":"red"}""", """{"sku":1}""").foreach: text =>
      assert(scala.util.Try(schema.validate(mapper.readTree(text))).isFailure, text)
    val payments = confluent(JsonSchemaCodec[Payment])
    assert(scala.util.Try(payments.validate(mapper.readTree("""{"type":"Card"}"""))).isFailure, "a case missing its field")

  test("an optional field added with a default is a compatible change under Confluent's default rules, and a field removed is not"):
    val older = confluent(JsonSchemaCodec[Line])
    assertEquals(confluent(JsonSchemaCodec[Grown]).isBackwardCompatible(older).asScala.toList, Nil)
    assert(confluent(JsonSchemaCodec[LineBefore]).isBackwardCompatible(older).asScala.nonEmpty, "removing a field from a closed object")

  /** xkafka's registry operations on Confluent's registry in memory. */
  private def bridged(mock: MockSchemaRegistryClient): SchemaRegistry[IO] =
    def missing[A](run: => A): IO[Option[A]] = IO.blocking(Option(run)).recover { case error: RestClientException if error.getStatus == 404 => None }
    new SchemaRegistry[IO]:
      override def schema(id: SchemaId): IO[Schema] = IO.blocking(Schema(mock.getSchemaById(id.value).canonicalString, SchemaType.Json))
      override def register(subject: Subject, schema: Schema): IO[SchemaId] =
        IO.blocking(SchemaId(mock.register(subject.value, new JsonSchema(schema.text))))
      override def lookup(subject: Subject, schema: Schema): IO[Option[Registered]] =
        missing(mock.getId(subject.value, new JsonSchema(schema.text))).map(_.map(id => Registered(SchemaId(id), subject, 1, schema)))
      override def latest(subject: Subject): IO[Option[Registered]] =
        missing(mock.getLatestSchemaMetadata(subject.value))
          .map(_.map(meta => Registered(SchemaId(meta.getId), subject, meta.getVersion, Schema(meta.getSchema, SchemaType.Json))))
      override def version(subject: Subject, version: Int): IO[Option[Registered]]    = IO.pure(None)
      override def compatibility(subject: Subject, schema: Schema): IO[Compatibility] = IO.pure(Compatibility.Compatible)

  test("Confluent's deserializer reads what xkafka writes, and xkafka reads what Confluent's serializer writes"):
    val mock         = new MockSchemaRegistryClient(List(new io.confluent.kafka.schemaregistry.json.JsonSchemaProvider()).asJava)
    val serializer   = new KafkaJsonSchemaSerializer[JsonNode](mock)
    val deserializer = new KafkaJsonSchemaDeserializer[Object](mock)
    serializer.configure(Map("schema.registry.url" -> "mock://xkafka", "auto.register.schemas" -> "true").asJava, false)
    deserializer.configure(Map("schema.registry.url" -> "mock://xkafka").asJava, false)
    val theirs = order.copy(payment = Card("9"), note = None)
    for
      serde   <- JsonSchemaSerde.of[IO, Order](bridged(mock), Part.Value)
      ours    <- serde.serializer.serialize(topic, Headers.empty, order)
      read    <- IO.blocking(deserializer.deserialize("orders", ours.get.toArray))
      written <-
        IO.blocking(serializer.serialize(
          "orders",
          JsonSchemaUtils.envelope(confluent(JsonSchemaCodec[Order]), node(JsonSchemaCodec[Order].encode(theirs).fold(fail(_), identity)))
        ))
      back <- serde.deserializer.deserialize(topic, Headers.empty, Some(Chunk.array(written)))
    yield
      assertEquals(mapper.valueToTree[JsonNode](read), node(JsonSchemaCodec[Order].encode(order).fold(fail(_), identity)))
      assertEquals(back, theirs)
      assertEquals(mock.getAllSubjects.asScala.toList, List("orders-value"))
