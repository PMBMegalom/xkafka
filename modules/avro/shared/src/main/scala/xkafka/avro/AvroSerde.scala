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
import xkafka.Serde
import xkafka.schemaregistry.{Part, RegistrySerde, Schema, SchemaFormat, SchemaRegistry, SchemaType, SubjectNaming, Writing}

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

  private def avro(schema: Schema): Either[String, AvroSchema] =
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
