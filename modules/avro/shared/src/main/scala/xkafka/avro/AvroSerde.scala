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

import cats.effect.Concurrent
import fs2.Chunk
import xkafka.{Deserializer, Serde}
import xkafka.schemaregistry.{Part, RegistrySerde, Schema, SchemaFormat, SchemaRegistry, SchemaType, SubjectNaming, Writing}

/** A record as it was written: the schema its writer used, and its value in that schema. */
final case class AvroWritten(schema: AvroSchema, value: AvroValue)

/** Avro as one of the registry's schema languages: values are written with their codec's schema, and read with the schema they were written with,
  * resolved to the codec's. A schema that names schemas registered under other subjects is not read.
  */
object AvroFormat:
  def apply[A](using codec: AvroCodec[A]): SchemaFormat[A] =
    new SchemaFormat[A]:
      private val own = codec.schema

      override val schema: Schema             = Schema(AvroSchemaJson.render(own), SchemaType.Avro)
      override val recordName: Option[String] = own.fullName.map(_.value)

      override def writer(target: Schema): Either[String, A => Either[String, Chunk[Byte]]] =
        avro(target).map: written =>
          // A value is written as its codec gives it; where the schema is another, the fields it lacks are left out and those it adds take their
          // defaults.
          value => codec.encode(value).flatMap(AvroBinary.encode(written, _))

      override def reader(writer: Schema): Either[String, Chunk[Byte] => Either[String, A]] =
        avro(writer).flatMap(AvroBinary.decoder(_, own)).map(decode => bytes => decode(bytes).flatMap(codec.decode))

  private[avro] def avro(schema: Schema): Either[String, AvroSchema] =
    if schema.schemaType != SchemaType.Avro then Left(s"the schema is ${schema.schemaType.name}, not Avro")
    else if schema.references.nonEmpty then Left("the schema names schemas registered under other subjects, which xkafka does not read yet")
    else AvroSchemaJson.parse(schema.text)

/** Serdes writing and reading Avro with the schemas the registry keeps, in Confluent's wire format, as Confluent's `KafkaAvroSerializer` does. */
object AvroSerde:
  def of[F[_]: Concurrent, A: AvroCodec](
      registry: SchemaRegistry[F],
      part: Part,
      naming: SubjectNaming = SubjectNaming.TopicName,
      writing: Writing = Writing.Register
  ): F[Serde[F, A]] = RegistrySerde.of(registry, AvroFormat[A], part, naming, writing)

  /** Serdes of values of `schema`, a schema known only as the program runs: values are written with it, and records read as it describes them. */
  def generic[F[_]: Concurrent](
      registry: SchemaRegistry[F],
      schema: AvroSchema,
      part: Part,
      naming: SubjectNaming = SubjectNaming.TopicName,
      writing: Writing = Writing.Register
  ): F[Serde[F, AvroValue]] = RegistrySerde.of(registry, AvroFormat(using AvroCodec.generic(schema)), part, naming, writing)

  /** Reads each record as it was written, with the schema it names, whatever that schema is: for tools that show or copy any record. */
  def asWritten[F[_]: Concurrent](registry: SchemaRegistry[F]): F[Deserializer[F, AvroWritten]] =
    RegistrySerde.deserializer(
      registry,
      schema =>
        AvroFormat.avro(schema).flatMap(writer => AvroBinary.decoder(writer, writer).map(read => bytes => read(bytes).map(AvroWritten(writer, _))))
    )
