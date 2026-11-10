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

package xkafka.schemaregistry

import cats.effect.{Concurrent, Ref}
import cats.syntax.all.*
import fs2.Chunk
import xkafka.{Deserializer, Serde, Serializer, Topic}

/** Which part of a record a serde writes, which names the subject its schema is kept under. */
enum Part derives CanEqual:
  case Key, Value

/** How the subject a record's schema is kept under is named, as Confluent's subject name strategies name it. */
enum SubjectNaming derives CanEqual:
  /** The topic and the part, as `orders-value`: one schema per topic, and Confluent's default. */
  case TopicName

  /** The full name of the record, as `com.example.Order`, wherever it is written. */
  case RecordName

  /** The topic and the record's full name, as `orders-com.example.Order`. */
  case TopicRecordName

/** Which schema a value is written with. */
enum Writing derives CanEqual:
  /** The format's own, registered under the subject on the first write where it is not already, as Confluent's `auto.register.schemas`. */
  case Register

  /** The format's own, which must already be registered under the subject, as Confluent's serializers write with `auto.register.schemas` off. */
  case AlreadyRegistered

  /** The subject's latest version, as Confluent's `use.latest.version`, which the format must be able to write its values with. */
  case Latest

  /** The schema registered as `id`, which the format must be able to write its values with. */
  case Pinned(id: SchemaId)

/** One schema language's way of writing and reading values of `A`: its schema for them, and how to write and read them with a schema. */
trait SchemaFormat[A]:
  /** The schema values of `A` are written with by default. */
  def schema: Schema

  /** The full name of the record the schema describes, which [[SubjectNaming.RecordName]] and [[SubjectNaming.TopicRecordName]] need. */
  def recordName: Option[String]

  /** How values are written with `target`, each of which may be refused, or why none can be. */
  def writer(target: Schema): Either[String, A => Either[String, Chunk[Byte]]]

  /** How what was written with `writer` is read, or why it cannot be. */
  def reader(writer: Schema): Either[String, Chunk[Byte] => Either[String, A]]

object RegistrySerde:
  /** Writes and reads `part` of records with the schemas the registry keeps, in Confluent's wire format: a zero byte, the schema's id as four
    * big-endian bytes, and then the value as `format` writes it. Reading takes the schema a record names, so records written with an older or newer
    * schema are read as `format` reads them.
    */
  def of[F[_], A](
      registry: SchemaRegistry[F],
      format: SchemaFormat[A],
      part: Part,
      naming: SubjectNaming = SubjectNaming.TopicName,
      writing: Writing = Writing.Register
  )(using F: Concurrent[F]): F[Serde[F, A]] =
    (Ref.of[F, Map[SchemaId, A => Either[String, Chunk[Byte]]]](Map.empty), Ref.of[F, Map[SchemaId, Chunk[Byte] => Either[String, A]]](Map.empty))
      .mapN: (writers, readers) =>
        def unserializable(topic: Topic, reason: String): Throwable = new RegistryException.Unserializable(topic, reason)

        def subject(topic: Topic): F[Subject] =
          val name =
            naming match
              case SubjectNaming.TopicName       => Right(s"${topic.value}-${suffix(part)}")
              case SubjectNaming.RecordName      => format.recordName.toRight("the format names no record, which naming subjects by record needs")
              case SubjectNaming.TopicRecordName => format.recordName.map(record => s"${topic.value}-$record")
                  .toRight("the format names no record, which naming subjects by record needs")
          F.fromEither(name.flatMap(Subject.from(_).leftMap(_.message)).leftMap(unserializable(topic, _)))

        /** The id of the schema a value of `topic` is written with, and the schema where it is not the format's own. */
        def target(topic: Topic): F[(SchemaId, Option[Schema])] =
          writing match
            case Writing.Register          => subject(topic).flatMap(registry.register(_, format.schema)).map((_, None))
            case Writing.AlreadyRegistered => subject(topic).flatMap: named =>
                registry.lookup(named, format.schema).flatMap:
                  case Some(registered) => F.pure((registered.id, None))
                  case None => F.raiseError(unserializable(topic, s"the format's schema is not registered under subject '${named.value}'"))
            case Writing.Latest => subject(topic).flatMap: named =>
                registry.latest(named).flatMap:
                  case Some(registered) => F.pure((registered.id, Some(registered.schema)))
                  case None             => F.raiseError(unserializable(topic, s"subject '${named.value}' has no versions"))
            case Writing.Pinned(id) => registry.schema(id).map(schema => (id, Some(schema)))

        def writerFor(topic: Topic, id: SchemaId, schema: Option[Schema]): F[A => Either[String, Chunk[Byte]]] =
          writers.get.map(_.get(id)).flatMap:
            case Some(write) => F.pure(write)
            case None        => F.fromEither(format.writer(schema.getOrElse(format.schema)).leftMap(unserializable(topic, _)))
                .flatTap(write => writers.update(_.updated(id, write)))

        def readerFor(topic: Topic, id: SchemaId): F[Chunk[Byte] => Either[String, A]] =
          readers.get.map(_.get(id)).flatMap:
            case Some(read) => F.pure(read)
            case None       => registry.schema(id).flatMap(schema => F.fromEither(format.reader(schema).leftMap(unserializable(topic, _))))
                .flatTap(read => readers.update(_.updated(id, read)))

        val serializer =
          Serializer.instance[F, A]: (topic, _, value) =>
            target(topic).flatMap: (id, schema) =>
              writerFor(topic, id, schema)
                .flatMap(write => F.fromEither(write(value).bimap(unserializable(topic, _), written => Some(Wire.write(id, written)))))

        val deserializer =
          Deserializer.instance[F, A]: (topic, _, bytes) =>
            bytes match
              case None          => F.raiseError(new Deserializer.NullValueException(topic))
              case Some(written) => F.fromEither(Wire.read(written).leftMap(unserializable(topic, _))).flatMap: (id, payload) =>
                  readerFor(topic, id).flatMap(read => F.fromEither(read(payload).leftMap(unserializable(topic, _))))

        Serde(serializer, deserializer)

  private def suffix(part: Part): String =
    part match
      case Part.Key   => "key"
      case Part.Value => "value"

/** Confluent's wire format: a zero byte, the schema's id as four big-endian bytes, and then the value. */
private[schemaregistry] object Wire:
  private val Magic: Byte = 0

  def write(id: SchemaId, payload: Chunk[Byte]): Chunk[Byte] =
    val value = id.value
    Chunk(Magic, (value >>> 24).toByte, (value >>> 16).toByte, (value >>> 8).toByte, value.toByte) ++ payload

  def read(bytes: Chunk[Byte]): Either[String, (SchemaId, Chunk[Byte])] =
    if bytes.isEmpty then Left("the record is empty, so it names no schema")
    else if bytes(0) != Magic then Left(s"the record starts with byte ${bytes(0)}, not the zero byte a record naming its schema starts with")
    else if bytes.size < 5 then Left(s"the record is ${bytes.size} bytes, too short to name its schema")
    else
      val id = (1 to 4).foldLeft(0)((sofar, at) => (sofar << 8) | (bytes(at) & 0xff))
      Right((SchemaId(id), bytes.drop(5)))
