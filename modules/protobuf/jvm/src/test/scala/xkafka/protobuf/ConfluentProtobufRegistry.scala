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

package xkafka.protobuf

import cats.effect.IO
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema
import xkafka.schemaregistry.*

/** Confluent's registry in memory, as xkafka's registry, for the tests of xkafka's Protobuf serdes and of those of other libraries' types. */
object ConfluentProtobufRegistry:
  /** xkafka's registry operations on Confluent's registry in memory. */
  def bridged(mock: MockSchemaRegistryClient): SchemaRegistry[IO] =
    def missing[A](run: => A): IO[Option[A]] = IO.blocking(Option(run)).recover { case error: RestClientException if error.getStatus == 404 => None }
    new SchemaRegistry[IO]:
      override def schema(id: SchemaId): IO[Schema] = IO.blocking(Schema(mock.getSchemaById(id.value).canonicalString, SchemaType.Protobuf))
      override def register(subject: Subject, schema: Schema): IO[SchemaId] =
        IO.blocking(SchemaId(mock.register(subject.value, new ProtobufSchema(schema.text))))
      override def lookup(subject: Subject, schema: Schema): IO[Option[Registered]] =
        missing(mock.getId(subject.value, new ProtobufSchema(schema.text))).map(_.map(id => Registered(SchemaId(id), subject, 1, schema)))
      override def latest(subject: Subject): IO[Option[Registered]] =
        missing(mock.getLatestSchemaMetadata(subject.value))
          .map(_.map(meta => Registered(SchemaId(meta.getId), subject, meta.getVersion, Schema(meta.getSchema, SchemaType.Protobuf))))
      override def version(subject: Subject, version: Int): IO[Option[Registered]]    = IO.pure(None)
      override def compatibility(subject: Subject, schema: Schema): IO[Compatibility] = IO.pure(Compatibility.Compatible)
