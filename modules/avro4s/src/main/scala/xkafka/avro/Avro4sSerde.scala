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

import java.io.ByteArrayOutputStream

import cats.effect.Concurrent
import cats.syntax.all.*
import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import fs2.Chunk
import org.apache.avro.Schema as ApacheSchema
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import xkafka.Serde
import xkafka.schemaregistry.{Part, RegistryException, RegistrySerde, Schema, SchemaFormat, SchemaRegistry, SchemaType, SubjectNaming, Writing}

/** Values that avro4s's encoders and decoders write and read, as one of the registry's schema languages. A value is written with avro4s's schema for
  * it, or where another is written with, as Apache Avro resolves avro4s's schema to it, so the fields avro4s's lacks take their defaults; a record is
  * read as avro4s reads one, with the schema it was written with resolved to avro4s's.
  */
object Avro4sFormat:
  /** The format of `A`, or why avro4s gives it no schema. */
  def apply[A](using schemaFor: SchemaFor[A], encoder: Encoder[A], decoder: Decoder[A]): Either[String, SchemaFormat[A]] =
    Either.catchNonFatal(schemaFor.schema).leftMap(error => s"avro4s gives no schema: ${error.getMessage}").map: own =>
      val text   = own.toString
      val encode = encoder.encode(own)
      val decode = decoder.decode(own)
      new SchemaFormat[A]:
        override val schema: Schema             = Schema(text, SchemaType.Avro)
        override val recordName: Option[String] = Avro4sBinary.recordName(own)

        override def writer(target: Schema): Either[String, A => Either[String, Chunk[Byte]]] =
          AvroBinary.writableText(text, target.text) *> Avro4sBinary.parse(target).map: written =>
            value =>
              Either.catchNonFatal(encode(value)).leftMap(error => s"the value cannot be encoded: ${error.getMessage}")
                .flatMap(Avro4sBinary.write(own, _)).flatMap(bytes =>
                  if target.text == text then Right(bytes) else Avro4sBinary.read(own, written, bytes).flatMap(Avro4sBinary.write(written, _))
                )

        override def reader(writer: Schema): Either[String, Chunk[Byte] => Either[String, A]] =
          Avro4sBinary.parse(writer).map: written =>
            bytes =>
              Avro4sBinary.read(written, own, bytes)
                .flatMap(value => Either.catchNonFatal(decode(value)).leftMap(error => s"the record cannot be decoded: ${error.getMessage}"))

/** Serdes of values that avro4s's encoders and decoders write and read, with the schemas the registry keeps, in Confluent's wire format, as
  * Confluent's `KafkaAvroSerializer` writes them.
  */
object Avro4sSerde:
  /** Fails with `RegistryException.NoSchema` where avro4s gives `A` no schema. */
  def of[F[_], A: SchemaFor: Encoder: Decoder](
      registry: SchemaRegistry[F],
      part: Part,
      naming: SubjectNaming = SubjectNaming.TopicName,
      writing: Writing = Writing.Register
  )(using F: Concurrent[F]): F[Serde[F, A]] =
    F.fromEither(Avro4sFormat[A].leftMap(new RegistryException.NoSchema(_))).flatMap(RegistrySerde.of(registry, _, part, naming, writing))

/** Apache Avro's schemas, and its binary encoding of the generic values avro4s's encoders and decoders give. */
private[avro] object Avro4sBinary:
  def parse(schema: Schema): Either[String, ApacheSchema] =
    if schema.schemaType != SchemaType.Avro then Left(s"the schema is ${schema.schemaType.name}, not Avro")
    else if schema.references.nonEmpty then Left("the schema names schemas registered under other subjects, which xkafka does not read yet")
    else Either.catchNonFatal(new ApacheSchema.Parser().parse(schema.text)).leftMap(error => s"the schema cannot be read: ${error.getMessage}")

  /** The full name of `schema`, where it is a named type. */
  def recordName(schema: ApacheSchema): Option[String] =
    schema.getType match
      case ApacheSchema.Type.RECORD | ApacheSchema.Type.ENUM | ApacheSchema.Type.FIXED => Some(schema.getFullName)
      case _                                                                           => None

  def write(schema: ApacheSchema, value: Any): Either[String, Chunk[Byte]] =
    Either.catchNonFatal:
      val out     = new ByteArrayOutputStream
      val encoder = EncoderFactory.get.binaryEncoder(out, null)
      new GenericDatumWriter[Any](schema).write(value, encoder)
      encoder.flush()
      Chunk.array(out.toByteArray)
    .leftMap(error => s"the value cannot be written: ${error.getMessage}")

  /** The value `bytes` hold, written with `writer`, as `reader` describes it. */
  def read(writer: ApacheSchema, reader: ApacheSchema, bytes: Chunk[Byte]): Either[String, Any] =
    val decoder = DecoderFactory.get.binaryDecoder(bytes.toArray, null)
    Either.catchNonFatal((new GenericDatumReader[Any](writer, reader).read(null, decoder), decoder.inputStream.available))
      .leftMap(error => s"the record cannot be read: ${error.getMessage}")
      .flatMap((value, left) => Either.cond(left == 0, value, s"$left bytes are left after the value"))
