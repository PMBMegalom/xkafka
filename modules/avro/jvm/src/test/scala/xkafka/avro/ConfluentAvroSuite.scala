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

import java.time.{Instant, LocalDate}
import java.util.UUID

import scala.jdk.CollectionConverters.*

import cats.effect.IO
import cats.syntax.all.*
import fs2.Chunk
import io.confluent.kafka.schemaregistry.avro.AvroSchema as ConfluentAvroSchema
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroSerializer}
import munit.CatsEffectSuite
import org.apache.avro.generic.GenericRecord
import xkafka.{Headers, Topic}
import xkafka.schemaregistry.*

/** xkafka's Avro serdes against Confluent's `KafkaAvroSerializer` and `KafkaAvroDeserializer`, sharing Confluent's registry in memory. */
class ConfluentAvroSuite extends CatsEffectSuite:
  private val topic = Topic.from("orders").toOption.get

  private val order =
    Order(
      UUID.fromString("3f2b8c1e-9a4d-4e6f-8b2a-1c3d5e7f9a0b"),
      Instant.ofEpochMilli(1762761600123L),
      LocalDate.of(2026, 11, 10),
      List(Line("apple", 3), Line("pear")),
      Status.Paid,
      Transfer("DE00", Some(Instant.ofEpochMilli(5))),
      Some("ring twice"),
      Map("priority" -> 2L),
      1
    )

  /** xkafka's registry operations on Confluent's registry in memory. */
  private def bridged(mock: MockSchemaRegistryClient): SchemaRegistry[IO] =
    def missing[A](run: => A): IO[Option[A]] = IO.blocking(Option(run)).recover { case error: RestClientException if error.getStatus == 404 => None }
    def registered(subject: Subject, id: Int, version: Int, text: String) = Registered(SchemaId(id), subject, version, Schema(text, SchemaType.Avro))
    new SchemaRegistry[IO]:
      override def schema(id: SchemaId): IO[Schema] = IO.blocking(Schema(mock.getSchemaById(id.value).canonicalString, SchemaType.Avro))
      override def register(subject: Subject, schema: Schema): IO[SchemaId] =
        IO.blocking(SchemaId(mock.register(subject.value, new ConfluentAvroSchema(schema.text))))
      override def lookup(subject: Subject, schema: Schema): IO[Option[Registered]] =
        missing(mock.getId(subject.value, new ConfluentAvroSchema(schema.text))).flatMap(
          _.traverse: id =>
            IO.blocking(registered(subject, id, mock.getVersion(subject.value, new ConfluentAvroSchema(schema.text)), schema.text))
        )
      override def latest(subject: Subject): IO[Option[Registered]] =
        missing(mock.getLatestSchemaMetadata(subject.value)).map(_.map(meta => registered(subject, meta.getId, meta.getVersion, meta.getSchema)))
      override def version(subject: Subject, version: Int): IO[Option[Registered]] =
        missing(mock.getSchemaMetadata(subject.value, version)).map(_.map(meta => registered(subject, meta.getId, meta.getVersion, meta.getSchema)))
      override def compatibility(subject: Subject, schema: Schema): IO[Compatibility] =
        IO.blocking(mock.testCompatibility(subject.value, new ConfluentAvroSchema(schema.text)))
          .map(ok => if ok then Compatibility.Compatible else Compatibility.Incompatible(Nil))

  private def confluentSerializer(mock: MockSchemaRegistryClient, isKey: Boolean = false, strategy: Option[String] = None): KafkaAvroSerializer =
    val serializer = new KafkaAvroSerializer(mock)
    val naming     =
      strategy.map(name =>
        (if isKey then "key.subject.name.strategy" else "value.subject.name.strategy") -> s"io.confluent.kafka.serializers.subject.$name"
      )
    serializer.configure((Map("schema.registry.url" -> "mock://xkafka", "auto.register.schemas" -> "true") ++ naming).asJava, isKey)
    serializer

  private def confluentDeserializer(mock: MockSchemaRegistryClient): KafkaAvroDeserializer =
    val deserializer = new KafkaAvroDeserializer(mock)
    deserializer.configure(Map("schema.registry.url" -> "mock://xkafka").asJava, false)
    deserializer

  /** The order as the generic record Confluent's serializer writes. */
  private def generic(value: Order): Any =
    val schema = new org.apache.avro.Schema.Parser().parse(AvroSchemaJson.render(AvroCodec[Order].schema))
    ApacheValues.toApache(AvroCodec[Order].encode(value).fold(fail(_), identity), schema)

  test("xkafka writes an order in the bytes Confluent's serializer writes, with the same schema id"):
    val mock = new MockSchemaRegistryClient()
    for
      serde  <- AvroSerde.of[IO, Order](bridged(mock), Part.Value)
      ours   <- serde.serializer.serialize(topic, Headers.empty, order).map(_.get)
      theirs <- IO.blocking(Chunk.array(confluentSerializer(mock).serialize("orders", generic(order))))
    yield
      assertEquals(ours, theirs)
      assertEquals(mock.getAllSubjects.asScala.toList, List("orders-value"))

  test("Confluent's deserializer reads what xkafka writes, and xkafka reads what Confluent's serializer writes"):
    val mock = new MockSchemaRegistryClient()
    for
      serde  <- AvroSerde.of[IO, Order](bridged(mock), Part.Value)
      ours   <- serde.serializer.serialize(topic, Headers.empty, order).map(_.get)
      read   <- IO.blocking(confluentDeserializer(mock).deserialize("orders", ours.toArray))
      theirs <- IO.blocking(Chunk.array(confluentSerializer(mock).serialize("orders", generic(order.copy(note = None, payment = Cash)))))
      back   <- AvroSerde.of[IO, Order](bridged(mock), Part.Value).flatMap(_.deserializer.deserialize(topic, Headers.empty, Some(theirs)))
    yield
      read match
        case record: GenericRecord => assertEquals(
            ApacheValues.sorted(ApacheValues.fromApache(record, record.getSchema)),
            ApacheValues.sorted(AvroCodec[Order].encode(order).fold(fail(_), identity))
          )
        case other => fail(s"expected a record, got $other")
      assertEquals(back, order.copy(note = None, payment = Cash))

  test("subjects are named as Confluent's strategies name them, for keys and values"):
    val strategies =
      List(
        (SubjectNaming.TopicName, "TopicNameStrategy"),
        (SubjectNaming.RecordName, "RecordNameStrategy"),
        (SubjectNaming.TopicRecordName, "TopicRecordNameStrategy")
      )
    strategies.flatMap((naming, strategy) => List((naming, strategy, Part.Key), (naming, strategy, Part.Value))).traverse_ :
      (naming, strategy, part) =>
        val (ours, theirs) = (new MockSchemaRegistryClient(), new MockSchemaRegistryClient())
        for
          serde <- AvroSerde.of[IO, Line](bridged(ours), part, naming)
          _     <- serde.serializer.serialize(topic, Headers.empty, Line("apple"))
          _     <-
            IO.blocking(confluentSerializer(theirs, part == Part.Key, Some(strategy)).serialize(
              "orders",
              ApacheValues.toApache(
                AvroCodec[Line].encode(Line("apple")).fold(fail(_), identity),
                new org.apache.avro.Schema.Parser().parse(AvroSchemaJson.render(AvroCodec[Line].schema))
              )
            ))
        yield assertEquals(ours.getAllSubjects.asScala.toList, theirs.getAllSubjects.asScala.toList, s"$strategy, $part")

  test("a record written with an older schema registered by Confluent's serializer is read with the newer codec's defaults"):
    val mock  = new MockSchemaRegistryClient()
    val older = new org.apache.avro.Schema.Parser().parse(AvroSchemaJson.render(AvroCodec[LineBefore].schema))
    val datum = new org.apache.avro.generic.GenericData.Record(older)
    datum.put("sku", "apple")
    for
      theirs <- IO.blocking(Chunk.array(confluentSerializer(mock).serialize("orders", datum)))
      back   <- AvroSerde.of[IO, Line](bridged(mock), Part.Value).flatMap(_.deserializer.deserialize(topic, Headers.empty, Some(theirs)))
    yield assertEquals(back, Line("apple", 1))

  test("records Confluent's serializer writes are read as written, and Confluent's deserializer reads generic values xkafka writes"):
    val mock   = new MockSchemaRegistryClient()
    val schema = new org.apache.avro.Schema.Parser().parse(AvroSchemaJson.render(AvroCodec[Line].schema))
    val datum  = new org.apache.avro.generic.GenericData.Record(schema)
    datum.put("sku", "apple")
    datum.put("quantity", 4)
    val value = AvroValue.Record(FullName(Some("xkafka.avro"), "Line"), Vector("sku" -> AvroValue.String("pear"), "quantity" -> AvroValue.Int(2)))
    for
      theirs  <- IO.blocking(Chunk.array(confluentSerializer(mock).serialize("orders", datum)))
      written <- AvroSerde.asWritten[IO](bridged(mock)).flatMap(_.deserialize(topic, Headers.empty, Some(theirs)))
      ours    <- AvroSerde.generic[IO](bridged(mock), AvroCodec[Line].schema, Part.Value).flatMap(_.serializer.serialize(topic, Headers.empty, value))
      read    <- IO.blocking(confluentDeserializer(mock).deserialize("orders", ours.get.toArray))
    yield
      assertEquals(written.value, ApacheValues.fromApache(datum, schema))
      assertEquals(new org.apache.avro.Schema.Parser().parse(AvroSchemaJson.render(written.schema)), schema)
      read match
        case record: GenericRecord => assertEquals(ApacheValues.fromApache(record, record.getSchema), value)
        case other                 => fail(s"expected a record, got $other")
