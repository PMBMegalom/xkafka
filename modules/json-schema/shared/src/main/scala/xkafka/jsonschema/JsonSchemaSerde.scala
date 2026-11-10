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

import java.nio.charset.StandardCharsets

import cats.effect.Concurrent
import cats.syntax.all.*
import fs2.Chunk
import io.circe.{parser, Json}
import xkafka.{Deserializer, Serde}
import xkafka.schemaregistry.{Part, RegistrySerde, Schema, SchemaFormat, SchemaRegistry, SchemaType, SubjectNaming, Writing}

/** A record as it was written: the schema its writer registered, and its JSON. */
final case class JsonSchemaWritten(schema: Json, value: Json)

/** JSON Schema as one of the registry's schema languages. JSON names its fields, so a record is read the same whatever schema it was written with: a
  * field it lacks takes its default, and properties the codec does not know are ignored.
  */
object JsonSchemaFormat:
  def apply[A](using codec: JsonSchemaCodec[A]): SchemaFormat[A] =
    new SchemaFormat[A]:
      override val schema: Schema             = Schema(codec.schema.noSpaces, SchemaType.Json)
      override val recordName: Option[String] = codec.name

      override def writer(target: Schema): Either[String, A => Either[String, Chunk[Byte]]] =
        json(target).map(_ => value => codec.encode(value).map(written => Chunk.array(written.noSpaces.getBytes(StandardCharsets.UTF_8))))

      override def reader(writer: Schema): Either[String, Chunk[Byte] => Either[String, A]] =
        json(writer).map(_ => bytes => read(bytes).flatMap(codec.decode))

  private[jsonschema] def json(schema: Schema): Either[String, Json] =
    if schema.schemaType != SchemaType.Json then Left(s"the schema is ${schema.schemaType.name}, not JSON Schema")
    else if schema.references.nonEmpty then Left("the schema names schemas registered under other subjects, which xkafka does not read yet")
    else parser.parse(schema.text).leftMap(error => s"the schema is not JSON: ${error.message}")

  private[jsonschema] def read(bytes: Chunk[Byte]): Either[String, Json] =
    parser.parse(new String(bytes.toArray, StandardCharsets.UTF_8)).leftMap(error => s"the record is not JSON: ${error.message}")

/** Serdes writing and reading JSON with the schemas the registry keeps, in Confluent's wire format, as Confluent's `KafkaJsonSchemaSerializer` does:
  * the schema's id, then the JSON as UTF-8 text.
  */
object JsonSchemaSerde:
  def of[F[_]: Concurrent, A: JsonSchemaCodec](
      registry: SchemaRegistry[F],
      part: Part,
      naming: SubjectNaming = SubjectNaming.TopicName,
      writing: Writing = Writing.Register
  ): F[Serde[F, A]] = RegistrySerde.of(registry, JsonSchemaFormat[A], part, naming, writing)

  /** Serdes of JSON values of `schema`, a schema known only as the program runs. Values are written as they are, without checking them against it. */
  def generic[F[_]: Concurrent](
      registry: SchemaRegistry[F],
      schema: Json,
      part: Part,
      naming: SubjectNaming = SubjectNaming.TopicName,
      writing: Writing = Writing.Register
  ): F[Serde[F, Json]] = RegistrySerde.of(registry, JsonSchemaFormat(using JsonSchemaCodec.generic(schema)), part, naming, writing)

  /** Reads each record's JSON as it was written, with the schema it names: for tools that show or copy any record. */
  def asWritten[F[_]: Concurrent](registry: SchemaRegistry[F]): F[Deserializer[F, JsonSchemaWritten]] =
    RegistrySerde.deserializer(
      registry,
      schema => JsonSchemaFormat.json(schema).map(written => bytes => JsonSchemaFormat.read(bytes).map(JsonSchemaWritten(written, _)))
    )
