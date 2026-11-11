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

import cats.effect.Concurrent
import cats.syntax.all.*
import fs2.Chunk
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}
import xkafka.Serde
import xkafka.schemaregistry.{Part, RegistryException, RegistrySerde, Schema, SchemaFormat, SchemaRegistry, SchemaType, SubjectNaming, Writing}

/** Protobuf messages that ScalaPB generated, as one of the registry's schema languages. They are written and read by ScalaPB, with the .proto file
  * ScalaPB generated them from as their schema, so a record is read the same whatever version of the file it was written with.
  */
object ScalaPBFormat:
  /** The format of `M`, or why its file cannot be registered: where it imports files other than the well-known types, or declares what xkafka does
    * not read, as groups or extensions.
    */
  def apply[M <: GeneratedMessage](using companion: GeneratedMessageCompanion[M]): Either[String, SchemaFormat[M]] =
    val message = companion.scalaDescriptor.fullName
    for
      file <- ScalaPBDescriptors.file(companion.scalaDescriptor.file.asProto)
      _ <- Pool.of(file, Map.empty).leftMap(reason => s"$reason; a ScalaPB message is registered with a file that imports only the well-known types")
      path <- file.message(message).map(_._2).toRight(s"the file declares no message $message")
    yield
      val text = ProtobufSchemaText.render(file)
      new SchemaFormat[M]:
        override val schema: Schema             = Schema(text, SchemaType.Protobuf)
        override val recordName: Option[String] = Some(message)

        // A record names its message by its path through the file's messages, which another version of the file may have moved.
        override def writer(target: Schema): Either[String, M => Either[String, Chunk[Byte]]] =
          ProtobufFormat.protobuf(target).flatMap: _ =>
            val at =
              if target.text == text then Right(path)
              else ProtobufSchemaText.parse(target.text).flatMap(_.message(message).map(_._2).toRight(s"the schema declares no message $message"))
            at.map(indexes => value => Right(ProtobufWire.indexes(indexes) ++ Chunk.array(value.toByteArray)))

        override def reader(writer: Schema): Either[String, Chunk[Byte] => Either[String, M]] =
          ProtobufFormat.protobuf(writer).map: _ =>
            bytes =>
              ProtobufWire.readIndexes(bytes).flatMap: (_, written) =>
                Either.catchNonFatal(companion.parseFrom(written.toArray)).leftMap(failure => s"$message could not be read: ${failure.getMessage}")

/** Serdes of Protobuf messages that ScalaPB generated, with the schemas the registry keeps, in Confluent's wire format, as Confluent's
  * `KafkaProtobufSerializer` writes them.
  */
object ScalaPBSerde:
  /** Fails with `RegistryException.NoSchema` where `M`'s file cannot be registered, as [[ScalaPBFormat]] says. */
  def of[F[_], M <: GeneratedMessage: GeneratedMessageCompanion](
      registry: SchemaRegistry[F],
      part: Part,
      naming: SubjectNaming = SubjectNaming.TopicName,
      writing: Writing = Writing.Register
  )(using F: Concurrent[F]): F[Serde[F, M]] =
    F.fromEither(ScalaPBFormat[M].leftMap(new RegistryException.NoSchema(_))).flatMap(RegistrySerde.of(registry, _, part, naming, writing))
