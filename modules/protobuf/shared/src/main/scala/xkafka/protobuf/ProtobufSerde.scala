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
import xkafka.{Deserializer, Serde}
import xkafka.schemaregistry.{Part, RegistryException, RegistrySerde, Schema, SchemaFormat, SchemaRegistry, SchemaType, SubjectNaming, Writing}

/** A record as it was written: the file of the schema its writer registered, and its message, whose name says which of the file's messages it is. */
final case class ProtobufWritten(schema: ProtobufSchema.File, value: ProtobufValue.Message)

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

  /** The format of messages named `message`, its full name, of `schema`, a file that imports only the well-known types. */
  def generic(schema: ProtobufSchema.File, message: String): Either[String, SchemaFormat[ProtobufValue]] =
    for
      pool <- Pool.of(schema, Map.empty).leftMap(reason => s"$reason; a generic serde registers files that import only the well-known types")
      _    <- schema.message(message).toRight(s"the schema declares no message $message")
    yield
      val dynamic = new Dynamic(pool)
      val text    = ProtobufSchemaText.render(schema)
      new SchemaFormat[ProtobufValue]:
        override val schema: Schema             = Schema(text, SchemaType.Protobuf)
        override val recordName: Option[String] = Some(message)

        override def writer(target: Schema): Either[String, ProtobufValue => Either[String, Chunk[Byte]]] =
          protobuf(target).flatMap(_ => ProtobufSchemaText.parse(target.text)).flatMap: file =>
            file.message(message).toRight(s"the schema declares no message $message").map: (_, path) =>
              value => dynamic.encode(message, value).map(ProtobufWire.indexes(path) ++ _)

        override def reader(writer: Schema): Either[String, Chunk[Byte] => Either[String, ProtobufValue]] =
          protobuf(writer).map(_ => bytes => ProtobufWire.readIndexes(bytes).flatMap((_, written) => dynamic.decode(message, written)))

  private[protobuf] def protobuf(schema: Schema): Either[String, Unit] =
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

  /** Serdes of messages named `message`, its full name, of `schema`, a file known only as the program runs, which may import the well-known types.
    * Values are [[ProtobufValue.Message]]s, and one that does not fit the message fails to write. Reading takes a record's fields by number, whatever
    * version of the schema it was written with. Fails with [[RegistryException.NoSchema]] where `schema` declares no such message or imports other
    * files.
    */
  def generic[F[_]](
      registry: SchemaRegistry[F],
      schema: ProtobufSchema.File,
      message: String,
      part: Part,
      naming: SubjectNaming = SubjectNaming.TopicName,
      writing: Writing = Writing.Register
  )(using F: Concurrent[F]): F[Serde[F, ProtobufValue]] =
    F.fromEither(ProtobufFormat.generic(schema, message).leftMap(new RegistryException.NoSchema(_)))
      .flatMap(RegistrySerde.of(registry, _, part, naming, writing))

  /** Reads each record as it was written, with the schema and message it names: for tools that show or copy any record. The schemas a schema names
    * under other subjects are fetched from the registry, and the well-known types are known without them.
    */
  def asWritten[F[_]](registry: SchemaRegistry[F])(using F: Concurrent[F]): F[Deserializer[F, ProtobufWritten]] =
    RegistrySerde.resolving(
      registry,
      schema =>
        files(registry, schema, Set.empty).map(
          _.flatMap: (root, imported) =>
            Pool.of(root, imported).map: pool =>
              val dynamic = new Dynamic(pool)
              (bytes: Chunk[Byte]) =>
                ProtobufWire.readIndexes(bytes).flatMap: (path, message) =>
                  root.messageAt(path).toRight(s"the schema has no message at ${path.mkString("[", ", ", "]")}").flatMap(dynamic.decode(_, message))
                    .map(ProtobufWritten(root, _))
        )
    )

  /** `schema`'s file and the files its references name, by import name, fetched from the registry, those under `fetching` being fetched already. */
  private def files[F[_]](registry: SchemaRegistry[F], schema: Schema, fetching: Set[String])(using
      F: Concurrent[F]
  ): F[Either[String, (ProtobufSchema.File, Map[String, ProtobufSchema.File])]] =
    ProtobufFormat.protobuf(schema).flatMap(_ => ProtobufSchemaText.parse(schema.text)) match
      case Left(reason) => F.pure(Left(reason))
      case Right(root)  => schema.references.filterNot(reference => fetching.contains(reference.name)).traverse: reference =>
          registry.version(reference.subject, reference.version).flatMap:
            case None => F
                .pure(Left(s"the schema names version ${reference.version} of subject '${reference.subject.value}', which the registry lacks"))
            case Some(registered) => files(registry, registered.schema, fetching ++ schema.references.map(_.name))
                .map(_.map((file, imported) => imported.updated(reference.name, file)))
        .map(_.sequence.map(found => (root, found.foldLeft(Map.empty[String, ProtobufSchema.File])(_ ++ _))))
