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
import fs2.Chunk
import org.apache.avro.Schema as ApacheSchema
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import xkafka.Serde
import xkafka.schemaregistry.{Part, RegistryException, RegistrySerde, Schema, SchemaFormat, SchemaRegistry, SchemaType, SubjectNaming, Writing}

/** Values that vulcan's codecs write and read, as one of the registry's schema languages. A value is written with the codec's schema, or where
  * another is written with, as Apache Avro resolves the codec's schema to it, so the fields the codec lacks take their defaults; a record is read as
  * vulcan reads one, with the schema it was written with.
  */
object VulcanFormat:
  /** The format of `A`, or why its codec has no schema. */
  def apply[A](using codec: vulcan.Codec[A]): Either[String, SchemaFormat[A]] =
    codec.schema.leftMap(_.message).map: own =>
      val text = own.toString
      new SchemaFormat[A]:
        override val schema: Schema             = Schema(text, SchemaType.Avro)
        override val recordName: Option[String] = VulcanBinary.recordName(own)

        override def writer(target: Schema): Either[String, A => Either[String, Chunk[Byte]]] =
          AvroBinary.writableText(text, target.text) *> VulcanBinary.parse(target).map: written =>
            value =>
              codec.encode(value).leftMap(_.message).flatMap(VulcanBinary.write(own, _)).flatMap: bytes =>
                if target.text == text then Right(bytes) else VulcanBinary.read(own, written, bytes).flatMap(VulcanBinary.write(written, _))

        override def reader(writer: Schema): Either[String, Chunk[Byte] => Either[String, A]] =
          VulcanBinary.parse(writer)
            .map(written => bytes => VulcanBinary.read(written, written, bytes).flatMap(codec.decode(_, written).leftMap(_.message)))

/** Serdes of values that vulcan's codecs write and read, with the schemas the registry keeps, in Confluent's wire format, as Confluent's
  * `KafkaAvroSerializer` writes them.
  */
object VulcanSerde:
  /** Fails with `RegistryException.NoSchema` where `A`'s codec has no schema. */
  def of[F[_], A: vulcan.Codec](
      registry: SchemaRegistry[F],
      part: Part,
      naming: SubjectNaming = SubjectNaming.TopicName,
      writing: Writing = Writing.Register
  )(using F: Concurrent[F]): F[Serde[F, A]] =
    F.fromEither(VulcanFormat[A].leftMap(new RegistryException.NoSchema(_))).flatMap(RegistrySerde.of(registry, _, part, naming, writing))

/** Apache Avro's schemas, and its binary encoding of the generic values vulcan's codecs give. */
private[avro] object VulcanBinary:
  def parse(schema: Schema): Either[String, ApacheSchema] =
    if schema.schemaType != SchemaType.Avro then Left(s"the schema is ${schema.schemaType.name}, not Avro")
    else if schema.references.nonEmpty then Left("the schema names schemas registered under other subjects, which xkafka does not read yet")
    else Either.catchNonFatal(new ApacheSchema.Parser().parse(schema.text)).leftMap(error => s"the schema cannot be read: ${error.getMessage}")

  /** The full name of `schema`, where it is a record: Confluent's strategies that name subjects by record take no other type. */
  def recordName(schema: ApacheSchema): Option[String] = Option.when(schema.getType == ApacheSchema.Type.RECORD)(schema.getFullName)

  /** Bytes on their own are written after the header as they are, with no length before them, as Confluent's serializers write them. */
  def write(schema: ApacheSchema, value: Any): Either[String, Chunk[Byte]] =
    Either.catchNonFatal:
      val out     = new ByteArrayOutputStream
      val encoder = EncoderFactory.get.binaryEncoder(out, null)
      new GenericDatumWriter[Any](schema).write(value, encoder)
      encoder.flush()
      if schema.getType == ApacheSchema.Type.BYTES then
        val read = DecoderFactory.get.binaryDecoder(out.toByteArray, null).readBytes(null)
        Chunk.array(java.util.Arrays.copyOfRange(read.array, read.arrayOffset + read.position, read.arrayOffset + read.limit))
      else Chunk.array(out.toByteArray)
    .leftMap(error => s"the value cannot be written: ${error.getMessage}")

  /** The Avro bytes of a payload written with `writer`: bytes on their own come with no length before them, which Avro reads them by. */
  private def framed(writer: ApacheSchema, bytes: Chunk[Byte]): Array[Byte] =
    if writer.getType == ApacheSchema.Type.BYTES then
      val out     = new ByteArrayOutputStream
      val encoder = EncoderFactory.get.binaryEncoder(out, null)
      encoder.writeBytes(bytes.toArray)
      encoder.flush()
      out.toByteArray
    else bytes.toArray

  /** The value `bytes` hold, written with `writer`, as `reader` describes it. */
  def read(writer: ApacheSchema, reader: ApacheSchema, bytes: Chunk[Byte]): Either[String, Any] =
    val decoder = DecoderFactory.get.binaryDecoder(framed(writer, bytes), null)
    Either.catchNonFatal((new GenericDatumReader[Any](writer, reader).read(null, decoder), decoder.inputStream.available))
      .leftMap(error => s"the record cannot be read: ${error.getMessage}")
      .flatMap((value, left) => Either.cond(left == 0, value, s"$left bytes are left after the value"))
