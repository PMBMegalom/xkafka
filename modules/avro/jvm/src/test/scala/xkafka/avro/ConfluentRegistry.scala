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

import scala.jdk.CollectionConverters.*

import cats.effect.IO
import cats.syntax.all.*
import io.confluent.kafka.schemaregistry.avro.AvroSchema as ConfluentAvroSchema
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroSerializer}
import xkafka.schemaregistry.*

/** Confluent's registry in memory, as xkafka's registry, and Confluent's Avro serializers on it, for the tests of xkafka's Avro serdes and of those
  * of other libraries' codecs.
  */
object ConfluentRegistry:
  /** xkafka's registry operations on Confluent's registry in memory. */
  def bridged(mock: MockSchemaRegistryClient): SchemaRegistry[IO] =
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

  def serializer(mock: MockSchemaRegistryClient, isKey: Boolean = false, strategy: Option[String] = None): KafkaAvroSerializer =
    val serializer = new KafkaAvroSerializer(mock)
    val naming     =
      strategy.map(name =>
        (if isKey then "key.subject.name.strategy" else "value.subject.name.strategy") -> s"io.confluent.kafka.serializers.subject.$name"
      )
    serializer.configure((Map("schema.registry.url" -> "mock://xkafka", "auto.register.schemas" -> "true") ++ naming).asJava, isKey)
    serializer

  def deserializer(mock: MockSchemaRegistryClient): KafkaAvroDeserializer =
    val deserializer = new KafkaAvroDeserializer(mock)
    deserializer.configure(Map("schema.registry.url" -> "mock://xkafka").asJava, false)
    deserializer
