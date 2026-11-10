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
import xkafka.Serde
import xkafka.schemaregistry.{Part, RegistryException, RegistrySerde, Schema, SchemaFormat, SchemaRegistry, SchemaType, SubjectNaming, Writing}

/** Protobuf as one of the registry's schema languages. Protobuf numbers its fields, so a record is read the same whatever schema it was written with:
  * a field it lacks takes its default or its zero, and fields the codec does not know are skipped.
  */
object ProtobufFormat:
  /** The format of `A`'s codec, or why `A` has no .proto file. */
  def apply[A](using codec: ProtobufCodec[A]): Either[String, SchemaFormat[A]] =
    codec.schema.map: text =>
      new SchemaFormat[A]:
        override val schema: Schema             = Schema(text, SchemaType.Protobuf)
        override val recordName: Option[String] = codec.name

        // A record names its message by its place among the file's messages, the first where the file is the codec's own.
        override def writer(target: Schema): Either[String, A => Either[String, Chunk[Byte]]] =
          protobuf(target).flatMap: _ =>
            val message = codec.describe.name
            val index   = if target.text == text then 0 else messages(target.text).indexOf(message)
            if index < 0 then Left(s"the schema declares no message $message")
            else Right(value => codec.encode(value).map(ProtobufWire.indexes(List(index)) ++ _))

        override def reader(writer: Schema): Either[String, Chunk[Byte] => Either[String, A]] =
          protobuf(writer).map(_ => bytes => ProtobufWire.readIndexes(bytes).flatMap((_, message) => codec.decode(message)))

  private def protobuf(schema: Schema): Either[String, Unit] =
    Either.cond(schema.schemaType == SchemaType.Protobuf, (), s"the schema is ${schema.schemaType.name}, not Protobuf")

  /** The names of the messages a .proto file declares outside any other, in order. */
  private[protobuf] def messages(text: String): List[String] =
    // Comments and quoted text may hold braces or the word message, so they are left out.
    val code   = text.replaceAll("""(?s)/\*.*?\*/|//[^\n]*|"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'""", " ")
    val tokens = """[A-Za-z_][A-Za-z0-9_.]*|[{}]""".r.findAllIn(code).toList
    tokens.foldLeft((0, false, List.empty[String])):
      case ((depth, named, found), "{") => (depth + 1, named, found)
      case ((depth, named, found), "}") => (depth - 1, named, found)
      case ((0, _, found), "message")   => (0, true, found)
      case ((depth, true, found), name) => (depth, false, name :: found)
      case (state, _)                   => state
    ._3.reverse

/** Serdes writing and reading Protobuf with the schemas the registry keeps, in Confluent's wire format, as Confluent's `KafkaProtobufSerializer`
  * does: the schema's id, then which of the schema's messages the record is, then the message.
  */
object ProtobufSerde:
  /** Fails with [[RegistryException.NoSchema]] where `A` is not a message, or holds what Protobuf cannot, as a list of lists. */
  def of[F[_], A: ProtobufCodec](
      registry: SchemaRegistry[F],
      part: Part,
      naming: SubjectNaming = SubjectNaming.TopicName,
      writing: Writing = Writing.Register
  )(using F: Concurrent[F]): F[Serde[F, A]] =
    F.fromEither(ProtobufFormat[A].leftMap(new RegistryException.NoSchema(_))).flatMap(RegistrySerde.of(registry, _, part, naming, writing))
