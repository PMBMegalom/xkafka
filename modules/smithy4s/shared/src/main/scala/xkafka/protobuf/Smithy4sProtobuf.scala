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

import alloy.proto.{
  ProtoCompactLocalDate, ProtoCompactLocalTime, ProtoCompactUUID, ProtoIndex, ProtoInlinedOneOf, ProtoNumType, ProtoOffsetDateTimeFormat,
  ProtoTimestampFormat, ProtoWrapped
}
import cats.effect.Concurrent
import cats.syntax.all.*
import fs2.Chunk
import smithy4s.{Blob, Hints, ShapeId}
import smithy4s.schema.{EnumTag, Primitive, Schema as SmithySchema}
import smithy4s.schema.Schema.*
import xkafka.Serde
import xkafka.protobuf.ProtobufSchema.{Enum, Field, FieldType, File, Label, Message, Syntax}
import xkafka.schemaregistry.{Part, RegistryException, RegistrySerde, Schema, SchemaFormat, SchemaRegistry, SchemaType, SubjectNaming, Writing}

/** Types smithy4s generates, as Protobuf values of the registry: written and read by smithy4s-protobuf, so records are the messages smithy4s's
  * services write and read, with the .proto file that declares those messages, made from the type's smithy4s schema.
  */
object Smithy4sProtobufFormat:
  /** The format of `A`, or why its messages cannot be declared in a .proto file. */
  def apply[A](using schema: SmithySchema[A]): Either[String, SchemaFormat[A]] =
    Smithy4sProtobufSchema.of(schema).flatMap(file => Pool.of(file, Map.empty).map(file -> _)).map: (file, pool) =>
      val codec = smithy4s.protobuf.Protobuf.codecs.fromSchema(schema)
      // smithy4s-protobuf reads a message inside another by recursion, counting no depth, so a record too deep is refused before it is read.
      val depth   = new Dynamic(pool)
      val text    = ProtobufSchemaText.render(file)
      val message = Smithy4sProtobufSchema.name(schema.shapeId, file)
      new SchemaFormat[A]:
        override val schema: Schema             = Schema(text, SchemaType.Protobuf)
        override val recordName: Option[String] = Some(message)

        // A record names its message by its path through the file's messages, which another version of the file may have moved.
        override def writer(target: Schema): Either[String, A => Either[String, Chunk[Byte]]] =
          ProtobufFormat.protobuf(target).flatMap: _ =>
            val written = if target.text == text then Right(file) else ProtobufSchemaText.parse(target.text)
            written.flatMap(_.message(message).map(_._2).toRight(s"the schema declares no message $message")).map: path =>
              value =>
                // What is written is held to the depth reading takes, so that nothing is written that reading refuses.
                Either.catchNonFatal(Chunk.array(codec.writeBlob(value).toArray))
                  .leftMap(error => s"the value cannot be written: ${error.getMessage}")
                  .flatMap(written => depth.shallow(message, written).as(ProtobufWire.indexes(path) ++ written))

        override def reader(writer: Schema): Either[String, Chunk[Byte] => Either[String, A]] =
          ProtobufFormat.protobuf(writer).map: _ =>
            bytes =>
              ProtobufWire.readIndexes(bytes).flatMap((_, written) =>
                depth.shallow(message, written)
                  .flatMap(_ => codec.readBlob(Blob(written.toArray)).leftMap(error => s"the record cannot be read: ${error.getMessage}"))
              )

/** Serdes of types smithy4s generates, written and read as smithy4s-protobuf writes and reads them, with the registry's schemas, in Confluent's wire
  * format, as Confluent's `KafkaProtobufSerializer` writes them.
  */
object Smithy4sProtobufSerde:
  /** Fails with `RegistryException.NoSchema` where `A`'s messages cannot be declared in a .proto file, as [[Smithy4sProtobufFormat]] says. */
  def of[F[_], A: SmithySchema](
      registry: SchemaRegistry[F],
      part: Part,
      naming: SubjectNaming = SubjectNaming.TopicName,
      writing: Writing = Writing.Register
  )(using F: Concurrent[F]): F[Serde[F, A]] =
    F.fromEither(Smithy4sProtobufFormat[A].leftMap(new RegistryException.NoSchema(_))).flatMap(RegistrySerde.of(registry, _, part, naming, writing))

/** The .proto files of the messages smithy4s-protobuf writes and reads, as smithy-translate declares them: a structure is a message, a union a
  * message of one `oneof`, or the `oneof` itself in the structure holding it where it has `@protoInlinedOneOf`, an enum an enum, a list a repeated
  * field, a map a map, and timestamps, durations and documents the well-known types they are written as.
  */
object Smithy4sProtobufSchema:
  /** The file of the messages of `schema`, which must be a structure or a union, in the package of its shape's namespace. */
  def of[A](schema: SmithySchema[A]): Either[String, File] =
    val root = schema.shapeId
    for
      _ <-
        unwrapped(schema) match
          case _: StructSchema[?] | _: UnionSchema[?] => Right(())
          case _                                      => Left(s"${show(root)} is neither a structure nor a union, which a message is made of")
      shapes <- Right(collect(schema, Map.empty))
      _      <-
        shapes.values.toList.groupBy(_.shapeId.name).collectFirst {
          case (name, same) if same.map(_.shapeId).distinct.size > 1 => s"two shapes are named $name, which one file cannot declare"
        }.toLeft(())
      rootMessage <- declaration(unwrapped(schema)).flatMap(_.left.toOption.toRight(s"${show(root)} is not a message"))
      others      <- shapes.values.toList.filter(_.shapeId != root).sortBy(_.shapeId.name).traverse(declaration)
      enums = others.flatMap(_.toOption)
      _ <-
        enums.flatMap(_.values.map(_._1)).groupBy(identity).collectFirst {
          case (value, copies) if copies.size > 1 => s"two enums have the value $value, which one package cannot declare twice"
        }.toLeft(())
    yield
      val messages = rootMessage :: others.flatMap(_.left.toOption)
      val used     =
        (messages.flatMap(_.fields.map(_.fieldType))).flatMap:
          case FieldType.Map(key, value) => List(key, value)
          case other                     => List(other)
      val imports = used.collect { case FieldType.Named(name) if wellKnown.contains(name) => wellKnown(name) }.distinct.sorted
      File(Syntax.Proto3, Some(root.namespace), imports, messages, enums)

  /** The full name of the message of `id` in `file`. */
  def name(id: ShapeId, file: File): String = file.packageName.fold(id.name)(space => s"$space.${id.name}")

  private val wellKnown: Map[String, String] =
    Map(
      ".google.protobuf.Timestamp"   -> "google/protobuf/timestamp.proto",
      ".google.protobuf.Duration"    -> "google/protobuf/duration.proto",
      ".google.protobuf.Value"       -> "google/protobuf/struct.proto",
      ".google.protobuf.Int64Value"  -> "google/protobuf/wrappers.proto",
      ".google.protobuf.Int32Value"  -> "google/protobuf/wrappers.proto",
      ".google.protobuf.UInt64Value" -> "google/protobuf/wrappers.proto",
      ".google.protobuf.UInt32Value" -> "google/protobuf/wrappers.proto",
      ".google.protobuf.DoubleValue" -> "google/protobuf/wrappers.proto",
      ".google.protobuf.FloatValue"  -> "google/protobuf/wrappers.proto",
      ".google.protobuf.BoolValue"   -> "google/protobuf/wrappers.proto",
      ".google.protobuf.StringValue" -> "google/protobuf/wrappers.proto",
      ".google.protobuf.BytesValue"  -> "google/protobuf/wrappers.proto"
    )

  private def show(id: ShapeId): String = s"${id.namespace}#${id.name}"

  /** `schema` without the layers that do not change how it is written. */
  private def unwrapped(schema: SmithySchema[?]): SmithySchema[?] =
    schema match
      case BijectionSchema(underlying, _)  => unwrapped(underlying)
      case RefinementSchema(underlying, _) => unwrapped(underlying)
      case LazySchema(suspend)             => unwrapped(suspend.value)
      case other                           => other

  /** The structures, unions and closed enums `schema` holds, with those already in `found`, by their shapes. */
  private def collect(schema: SmithySchema[?], found: Map[ShapeId, SmithySchema[?]]): Map[ShapeId, SmithySchema[?]] =
    unwrapped(schema) match
      case struct: StructSchema[?] if !found.contains(struct.shapeId) =>
        struct.fields.foldLeft(found.updated(struct.shapeId, struct))((all, field) => collect(field.schema, all))
      case union: UnionSchema[?] if !found.contains(union.shapeId) =>
        val inlined = union.hints.has[ProtoInlinedOneOf]
        union.alternatives.foldLeft(if inlined then found else found.updated(union.shapeId, union))((all, alt) => collect(alt.schema, all))
      case enumeration @ EnumerationSchema(_, _, EnumTag.StringEnum(_, None) | EnumTag.IntEnum(_, None), _) => found
          .updated(enumeration.shapeId, enumeration)
      case CollectionSchema(_, _, _, member) => collect(member, found)
      case MapSchema(_, _, _, key, value)    => collect(value, collect(key, found))
      case OptionSchema(_, underlying)       => collect(underlying, found)
      case _                                 => found

  /** How a value is held in a message: as one field, a repeated field, a map, or the fields of a `oneof`. */
  private enum Kind:
    case Single(fieldType: FieldType)
    case Repeated(fieldType: FieldType)
    case Keyed(key: FieldType, value: FieldType)
    case Inlined(fields: List[(String, Int, FieldType)])

  /** A message or enum declaring a structure, union or enum. */
  private def declaration(schema: SmithySchema[?]): Either[String, Either[Message, Enum]] =
    schema match
      case struct: StructSchema[?] => numbered(show(struct.shapeId), struct.fields.toList.map(field => (field.label, field.hints, field.schema)))
          .flatMap: fields =>
            fields.flatTraverse: (label, number, schema) =>
              kind(schema, s"${show(struct.shapeId)}$$$label").map:
                case Kind.Single(fieldType)   => List(Field(label, number, fieldType, Label.Implicit))
                case Kind.Repeated(fieldType) => List(Field(label, number, fieldType, Label.Repeated))
                case Kind.Keyed(key, value)   => List(Field(label, number, FieldType.Map(key, value), Label.Repeated))
                case Kind.Inlined(members)    => members.map((name, at, fieldType) => Field(name, at, fieldType, Label.Optional, Some(label)))
          .flatMap: fields =>
            fields.groupBy(_.number).collectFirst { case (number, same) if same.size > 1 => number } match
              case Some(number) =>
                Left(s"${show(struct.shapeId)} numbers two fields $number, as where an inlined union's members are not numbered apart from its own")
              case None => Right(Left(Message(struct.shapeId.name, fields)))
      case union: UnionSchema[?] => alternatives(union).map(members =>
          Left(Message(union.shapeId.name, members.map((name, at, fieldType) => Field(name, at, fieldType, Label.Optional, Some("definition")))))
        )
      case enumeration: EnumerationSchema[?] =>
        val indexed = enumeration.values.map(_.hints.get[ProtoIndex].map(_.value))
        val values  =
          if indexed.forall(_.isDefined) then enumeration.values.zip(indexed.flatten).map((value, number) => (value.name, number))
          else enumeration.values.zipWithIndex.map((value, number) => (value.name, number))
        // proto3 takes an enum's first value as its zero, so the value numbered 0 is declared first.
        Either.cond(
          values.exists(_._2 == 0),
          Right(Enum(enumeration.shapeId.name, values.filter(_._2 == 0) ++ values.filterNot(_._2 == 0))),
          s"${show(enumeration.shapeId)} has no value numbered 0, which Protobuf's enums start with"
        )
      case other => Left(s"${show(other.shapeId)} is not a message")

  /** Members numbered by their `@protoIndex`es, or else by their places from 1. */
  private def numbered(where: String, members: List[(String, Hints, SmithySchema[?])]): Either[String, List[(String, Int, SmithySchema[?])]] =
    val indexes = members.map(_._2.get(ProtoIndex).map(_.value))
    if indexes.forall(_.isDefined) then Right(members.zip(indexes.flatten).map((member, number) => (member._1, number, member._3)))
    else if indexes.forall(_.isEmpty) then Right(members.zipWithIndex.map((member, index) => (member._1, index + 1, member._3)))
    else Left(s"$where numbers some of its members with @protoIndex, and not others")

  private def alternatives(union: UnionSchema[?]): Either[String, List[(String, Int, FieldType)]] =
    numbered(show(union.shapeId), union.alternatives.toList.map(alt => (alt.label, alt.hints, alt.schema))).flatMap: alts =>
      alts.traverse: (label, number, schema) =>
        kind(schema, s"${show(union.shapeId)}$$$label").flatMap:
          case Kind.Single(fieldType) => Right((label, number, fieldType))
          case _                      => Left(s"${show(union.shapeId)}$$$label is a list, a map or a union inlined, which a oneof cannot hold")

  private def kind(schema: SmithySchema[?], where: String): Either[String, Kind] =
    schema match
      case PrimitiveSchema(_, hints, tag)        => primitive(tag, hints, where).map(Kind.Single(_))
      case CollectionSchema(_, hints, _, member) =>
        if hints.has[ProtoWrapped] then Left(s"$where is a list with @protoWrapped, which xkafka does not declare yet")
        else
          kind(member, where).flatMap:
            case Kind.Single(fieldType) if !member.isOption => Right(Kind.Repeated(fieldType))
            case _ => Left(s"$where is a list of lists, maps, inlined unions or nulls, which a repeated field cannot hold")
      case MapSchema(_, hints, _, key, value) =>
        if hints.has[ProtoWrapped] then Left(s"$where is a map with @protoWrapped, which xkafka does not declare yet")
        else
          (kind(key, where), kind(value, where)).tupled.flatMap:
            case (Kind.Single(keyType), Kind.Single(valueType)) if mapKeys(keyType) => Right(Kind.Keyed(keyType, valueType))
            case _ => Left(s"$where is a map whose keys or values a Protobuf map cannot hold")
      case EnumerationSchema(id, _, EnumTag.StringEnum(_, None) | EnumTag.IntEnum(_, None), _) => Right(Kind.Single(FieldType.Named(id.name)))
      case EnumerationSchema(_, _, EnumTag.StringEnum(_, Some(_)), _)                          => Right(Kind.Single(FieldType.String))
      case EnumerationSchema(_, _, _, _)                                                       => Right(Kind.Single(FieldType.Int32))
      case struct: StructSchema[?] => Right(Kind.Single(FieldType.Named(struct.shapeId.name)))
      case union: UnionSchema[?]   =>
        if union.hints.has[ProtoInlinedOneOf] then alternatives(union).map(Kind.Inlined(_))
        else Right(Kind.Single(FieldType.Named(union.shapeId.name)))
      case OptionSchema(_, underlying)     => kind(underlying, where)
      case BijectionSchema(underlying, _)  => kind(underlying, where)
      case RefinementSchema(underlying, _) => kind(underlying, where)
      case LazySchema(suspend)             => kind(suspend.value, where)

  private val mapKeys: FieldType => Boolean =
    Set(FieldType.String, FieldType.Bool, FieldType.Int32, FieldType.Int64, FieldType.UInt32, FieldType.UInt64, FieldType.SInt32, FieldType.SInt64)
      .union(Set(FieldType.Fixed32, FieldType.Fixed64, FieldType.SFixed32, FieldType.SFixed64))

  private def primitive(tag: Primitive[?], hints: Hints, where: String): Either[String, FieldType] =
    val numbers = hints.get[ProtoNumType]
    val plain   =
      tag match
        case Primitive.PLong => Right(
            numbers match
              case Some(ProtoNumType.SIGNED)       => FieldType.SInt64
              case Some(ProtoNumType.UNSIGNED)     => FieldType.UInt64
              case Some(ProtoNumType.FIXED)        => FieldType.Fixed64
              case Some(ProtoNumType.FIXED_SIGNED) => FieldType.SFixed64
              case _                               => FieldType.Int64
          )
        case Primitive.PInt => Right(
            numbers match
              case Some(ProtoNumType.SIGNED)       => FieldType.SInt32
              case Some(ProtoNumType.UNSIGNED)     => FieldType.UInt32
              case Some(ProtoNumType.FIXED)        => FieldType.Fixed32
              case Some(ProtoNumType.FIXED_SIGNED) => FieldType.SFixed32
              case _                               => FieldType.Int32
          )
        case Primitive.PShort | Primitive.PByte                            => Right(FieldType.Int32)
        case Primitive.PBoolean                                            => Right(FieldType.Bool)
        case Primitive.PDouble                                             => Right(FieldType.Double)
        case Primitive.PFloat                                              => Right(FieldType.Float)
        case Primitive.PString | Primitive.PBigDecimal | Primitive.PBigInt => Right(FieldType.String)
        case Primitive.PBlob                                               => Right(FieldType.Bytes)
        case Primitive.PDocument                                           => Right(FieldType.Named(".google.protobuf.Value"))
        case Primitive.PDuration                                           => Right(FieldType.Named(".google.protobuf.Duration"))
        case Primitive.PUUID if hints.has[ProtoCompactUUID] => Left(s"$where is a UUID with @protoCompactUUID, which xkafka does not declare yet")
        case Primitive.PLocalDate if hints.has[ProtoCompactLocalDate] =>
          Left(s"$where is a date with @protoCompactLocalDate, which xkafka does not declare yet")
        case Primitive.PLocalTime if hints.has[ProtoCompactLocalTime] =>
          Left(s"$where is a time with @protoCompactLocalTime, which xkafka does not declare yet")
        case Primitive.POffsetDateTime if hints.get[ProtoOffsetDateTimeFormat].contains(ProtoOffsetDateTimeFormat.PROTOBUF) =>
          Left(s"$where is a date and time in the Protobuf format, which xkafka does not declare yet")
        case Primitive.PUUID | Primitive.PLocalDate | Primitive.PLocalTime | Primitive.POffsetDateTime           => Right(FieldType.String)
        case Primitive.PTimestamp if hints.get[ProtoTimestampFormat].contains(ProtoTimestampFormat.EPOCH_MILLIS) =>
          Left(s"$where is a timestamp in epoch milliseconds, which xkafka does not declare yet")
        case Primitive.PTimestamp => Right(FieldType.Named(".google.protobuf.Timestamp"))
    if !hints.has[ProtoWrapped] then plain
    else
      plain.flatMap:
        case FieldType.Int64  => Right(FieldType.Named(".google.protobuf.Int64Value"))
        case FieldType.Int32  => Right(FieldType.Named(".google.protobuf.Int32Value"))
        case FieldType.UInt64 => Right(FieldType.Named(".google.protobuf.UInt64Value"))
        case FieldType.UInt32 => Right(FieldType.Named(".google.protobuf.UInt32Value"))
        case FieldType.Double => Right(FieldType.Named(".google.protobuf.DoubleValue"))
        case FieldType.Float  => Right(FieldType.Named(".google.protobuf.FloatValue"))
        case FieldType.Bool   => Right(FieldType.Named(".google.protobuf.BoolValue"))
        case FieldType.String => Right(FieldType.Named(".google.protobuf.StringValue"))
        case FieldType.Bytes  => Right(FieldType.Named(".google.protobuf.BytesValue"))
        case _                => Left(s"$where has @protoWrapped on a type without a well-known wrapper, which xkafka does not declare yet")
