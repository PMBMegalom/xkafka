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

import java.nio.charset.StandardCharsets

import cats.syntax.all.*
import fs2.Chunk
import xkafka.protobuf.ProtobufSchema.{Enum, Field, FieldType, File, Label, Message, Syntax}

/** The messages and enums of a file and the files it imports, by full name without a leading dot. */
private[protobuf] final class Pool(messages: Map[String, (Message, Syntax)], enums: Map[String, Enum]):
  def message(full: String): Option[(Message, Syntax)] = messages.get(full)

  /** The full name a type named `name` in message `scope` refers to, looking in the scope and then each one enclosing it, as protoc does. */
  def resolve(scope: String, name: String): Either[String, Either[(String, Enum), String]] =
    val candidates =
      if name.startsWith(".") then List(name.drop(1))
      else
        val parts = scope.split('.').toList.filter(_.nonEmpty)
        (parts.length to 0 by -1).toList.map(size => (parts.take(size) :+ name).mkString("."))
    candidates.collectFirstSome(full => messages.get(full).map(_ => Right(full)).orElse(enums.get(full).map(found => Left(full -> found))))
      .toRight(s"type $name in $scope is declared in none of the files")

private[protobuf] object Pool:
  /** The pool of `root` and the files it imports, transitively, from `files` and the well-known types. */
  def of(root: File, files: Map[String, File]): Either[String, Pool] =
    def gather(pending: List[String], seen: Map[String, File]): Either[String, List[File]] =
      pending match
        case Nil                                 => Right(seen.values.toList)
        case name :: rest if seen.contains(name) => gather(rest, seen)
        case name :: rest                        => files.get(name).orElse(WellKnown.files.get(name))
            .toRight(s"the schema imports $name, which neither its references nor the well-known types hold")
            .flatMap(file => gather(rest ++ file.imports, seen.updated(name, file)))
    gather(root.imports, Map.empty).map: imported =>
      val all                                                                                              = root :: imported
      def walk(prefix: String, syntax: Syntax, declared: List[Message]): List[(String, (Message, Syntax))] =
        declared.flatMap: message =>
          val full = if prefix.isEmpty then message.name else s"$prefix.${message.name}"
          (full -> (message, syntax)) :: walk(full, syntax, message.messages)
      def enumsOf(prefix: String, declared: List[Message], own: List[Enum]): List[(String, Enum)] =
        own.map(found => (if prefix.isEmpty then found.name else s"${prefix}.${found.name}") -> found) ++ declared.flatMap: message =>
          val full = if prefix.isEmpty then message.name else s"$prefix.${message.name}"
          enumsOf(full, message.messages, message.enums)
      new Pool(
        all.flatMap(file => walk(file.packageName.getOrElse(""), file.syntax, file.messages)).toMap,
        all.flatMap(file => enumsOf(file.packageName.getOrElse(""), file.messages, file.enums)).toMap
      )

/** Writes and reads [[ProtobufValue]]s as the messages of a [[Pool]] describe them. */
private[protobuf] final class Dynamic(pool: Pool):
  private def described(full: String): Either[String, (Message, Syntax)] = pool.message(full).toRight(s"no message is named $full")

  def encode(full: String, value: ProtobufValue): Either[String, Chunk[Byte]] =
    value match
      case ProtobufValue.Message(name, fields, unknown) => described(full).flatMap: (message, syntax) =>
          val declared = message.fields.map(_.name).toSet
          val named    = fields.map(_._1)
          if name != full then Left(s"a message $name is given where $full is written")
          else
            named.find(!declared.contains(_)).map(extra => Left(s"$full has no field $extra"))
              .orElse(named.diff(named.distinct).headOption.map(twice => Left(s"field $twice of $full is given twice")))
              .orElse(message.fields.groupBy(_.oneof).collectFirst {
                case (Some(oneof), members) if members.count(member => named.contains(member.name)) > 1 =>
                  Left(s"more than one field of oneof $oneof of $full is given")
              }).getOrElse:
                message.fields.sortBy(_.number).traverse: field =>
                  fields.collectFirst { case (field.name, set) => set } match
                    case None if field.label == Label.Required => Left(s"required field ${field.name} of $full is not given")
                    case None                                  => Right(Chunk.empty[Byte])
                    case Some(set) => writeField(full, syntax, field, set).leftMap(reason => s"field ${field.name} of $full: $reason")
                .map(written => (written ++ unknown.map(_.bytes)).foldLeft(Chunk.empty[Byte])(_ ++ _))
      case other => Left(s"expected a message $full, found $other")

  private def writeField(scope: String, syntax: Syntax, field: Field, value: ProtobufValue): Either[String, Chunk[Byte]] =
    (field.fieldType, value) match
      case (FieldType.Map(key, item), ProtobufValue.Map(entries)) => entries.traverse: (entryKey, entryValue) =>
          (single(scope, key, entryKey), single(scope, item, entryValue)).mapN((written, valueWritten) =>
            ProtobufWire.field(field.number, Raw.Bytes(ProtobufWire.field(1, written) ++ ProtobufWire.field(2, valueWritten)))
          )
        .map(_.foldLeft(Chunk.empty[Byte])(_ ++ _))
      case (_, ProtobufValue.List(values)) if field.label == Label.Repeated =>
        values.traverse(single(scope, field.fieldType, _)).map: raws =>
          val packable = raws.headOption.exists(raw => ProtobufWire.wireOf(raw) != ProtobufWire.BytesType)
          if raws.isEmpty then Chunk.empty
          else if packable && field.packed.getOrElse(syntax == Syntax.Proto3) then
            ProtobufWire.field(field.number, Raw.Bytes(raws.foldLeft(Chunk.empty[Byte])(_ ++ ProtobufWire.payload(_))))
          else raws.foldLeft(Chunk.empty[Byte])(_ ++ ProtobufWire.field(field.number, _))
      case (FieldType.Map(_, _), other)                => Left(s"expected a map, found $other")
      case (_, other) if field.label == Label.Repeated => Left(s"expected a list, found $other")
      case (fieldType, set)                            => single(scope, fieldType, set).map: raw =>
          // A field without presence is left out where it is zero.
          if field.label == Label.Implicit && zero(raw, fieldType) then Chunk.empty else ProtobufWire.field(field.number, raw)

  private def zero(raw: Raw, fieldType: FieldType): Boolean =
    raw match
      case Raw.Varint(value)  => value == 0
      case Raw.Fixed64(value) => value == 0
      case Raw.Fixed32(value) => value == 0
      case Raw.Bytes(bytes)   => bytes.isEmpty && (fieldType == FieldType.String || fieldType == FieldType.Bytes)

  private def zigzag32(value: Int): Long  = ((value << 1) ^ (value >> 31)).toLong & 0xffffffffL
  private def zigzag64(value: Long): Long = (value << 1) ^ (value >> 63)

  private val maxUInt64 = (BigInt(1) << 64) - 1

  private def single(scope: String, fieldType: FieldType, value: ProtobufValue): Either[String, Raw] =
    (fieldType, value) match
      case (FieldType.Double, ProtobufValue.Double(number))  => Right(Raw.Fixed64(java.lang.Double.doubleToRawLongBits(number)))
      case (FieldType.Float, ProtobufValue.Float(number))    => Right(Raw.Fixed32(java.lang.Float.floatToRawIntBits(number)))
      case (FieldType.Int32, ProtobufValue.Int32(number))    => Right(Raw.Varint(number.toLong))
      case (FieldType.SInt32, ProtobufValue.Int32(number))   => Right(Raw.Varint(zigzag32(number)))
      case (FieldType.SFixed32, ProtobufValue.Int32(number)) => Right(Raw.Fixed32(number))
      case (FieldType.Int64, ProtobufValue.Int64(number))    => Right(Raw.Varint(number))
      case (FieldType.SInt64, ProtobufValue.Int64(number))   => Right(Raw.Varint(zigzag64(number)))
      case (FieldType.SFixed64, ProtobufValue.Int64(number)) => Right(Raw.Fixed64(number))
      case (FieldType.UInt32 | FieldType.Fixed32, ProtobufValue.UInt32(number)) if number < 0 || number > 0xffffffffL =>
        Left(s"$number is not an unsigned 32-bit number")
      case (FieldType.UInt32, ProtobufValue.UInt32(number))  => Right(Raw.Varint(number))
      case (FieldType.Fixed32, ProtobufValue.UInt32(number)) => Right(Raw.Fixed32(number.toInt))
      case (FieldType.UInt64 | FieldType.Fixed64, ProtobufValue.UInt64(number)) if number < 0 || number > maxUInt64 =>
        Left(s"$number is not an unsigned 64-bit number")
      case (FieldType.UInt64, ProtobufValue.UInt64(number))  => Right(Raw.Varint(number.toLong))
      case (FieldType.Fixed64, ProtobufValue.UInt64(number)) => Right(Raw.Fixed64(number.toLong))
      case (FieldType.Bool, ProtobufValue.Bool(flag))        => Right(Raw.Varint(if flag then 1 else 0))
      case (FieldType.String, ProtobufValue.String(text))    => Right(Raw.Bytes(Chunk.array(text.getBytes(StandardCharsets.UTF_8))))
      case (FieldType.Bytes, ProtobufValue.Bytes(bytes))     => Right(Raw.Bytes(bytes))
      case (FieldType.Named(name), set)                      => pool.resolve(scope, name).flatMap:
          case Right(full)              => encode(full, set).map(Raw.Bytes(_))
          case Left((full, enumerated)) => set match
              case ProtobufValue.Enum(value) => enumerated.values.collectFirst { case (`value`, number) => Raw.Varint(number.toLong) }
                  .toRight(s"$value is not a value of $full")
              case ProtobufValue.EnumNumber(number) => Right(Raw.Varint(number.toLong))
              case other                            => Left(s"expected a value of $full, found $other")
      case (expected, other) => Left(s"expected a value of ${expected.toString.toLowerCase}, found $other")

  def decode(full: String, bytes: Chunk[Byte]): Either[String, ProtobufValue.Message] =
    described(full).flatMap: (message, _) =>
      ProtobufWire.fields(bytes).flatMap: read =>
        val byNumber = read.groupMap(_._1)(_._2)
        val numbers  = message.fields.map(_.number).toSet
        val unknown  = read.collect { case (number, raw) if !numbers.contains(number) => UnknownField(number, ProtobufWire.field(number, raw)) }
        message.fields.flatTraverse: field =>
          readField(full, field, byNumber.getOrElse(field.number, Vector.empty))
            .bimap(reason => s"field ${field.name} of $full: $reason", _.map(field.name -> _).toList)
        .map(fields => ProtobufValue.Message(full, fields.toVector, unknown))

  /** A field's value from what was written under its number; none where it is absent and records presence. */
  private def readField(scope: String, field: Field, found: Vector[Raw]): Either[String, Option[ProtobufValue]] =
    field.fieldType match
      case FieldType.Map(key, item) => found.traverse:
          case Raw.Bytes(entry) => ProtobufWire.fields(entry).flatMap: fields =>
              def at(number: Int): Option[Raw] = fields.collect { case (`number`, raw) => raw }.lastOption
              (at(1).fold(zeroOf(scope, key))(take(scope, key, _)), at(2).fold(zeroOf(scope, item))(take(scope, item, _))).tupled
          case _ => Left("expected a map's entry")
        .map(entries =>
          Some(
            ProtobufValue
              .Map(entries.foldLeft(Vector.empty[(ProtobufValue, ProtobufValue)])((kept, entry) => kept.filterNot(_._1 == entry._1) :+ entry))
          )
        )
      case fieldType if field.label == Label.Repeated =>
        found.flatTraverse:
          case Raw.Bytes(packed) if packable(scope, fieldType) =>
            ProtobufWire.packed(packed, wireOf(fieldType)).flatMap(_.traverse(take(scope, fieldType, _)))
          case raw => take(scope, fieldType, raw).map(Vector(_))
        .map(values => Some(ProtobufValue.List(values)))
      case fieldType => found.lastOption match
          case Some(raw) => take(scope, fieldType, raw).map(Some(_))
          // A message has presence of its own, even in a field without a label.
          case None if field.label == Label.Implicit && !message(scope, fieldType) => zeroOf(scope, fieldType).map(Some(_))
          case None                                                                => Right(None)

  private def message(scope: String, fieldType: FieldType): Boolean =
    fieldType match
      case FieldType.Named(name) => pool.resolve(scope, name).exists(_.isRight)
      case _                     => false

  /** Whether values of the type are numbers, which a repeated field may pack: an enum's are, a message's are not. */
  private def packable(scope: String, fieldType: FieldType): Boolean =
    fieldType match
      case FieldType.String | FieldType.Bytes => false
      case FieldType.Named(name)              => pool.resolve(scope, name).exists(_.isLeft)
      case _                                  => true

  private def wireOf(fieldType: FieldType): Int =
    fieldType match
      case FieldType.Double | FieldType.Fixed64 | FieldType.SFixed64 => ProtobufWire.Fixed64Type
      case FieldType.Float | FieldType.Fixed32 | FieldType.SFixed32  => ProtobufWire.Fixed32Type
      case _                                                         => ProtobufWire.VarintType

  /** A field's value where a record lacks it and it records no presence. */
  private def zeroOf(scope: String, fieldType: FieldType): Either[String, ProtobufValue] =
    fieldType match
      case FieldType.String      => Right(ProtobufValue.String(""))
      case FieldType.Bytes       => Right(ProtobufValue.Bytes(Chunk.empty))
      case FieldType.Named(name) => pool.resolve(scope, name).flatMap:
          case Left(_)     => take(scope, fieldType, Raw.Varint(0))
          case Right(full) => Left(s"a message $full has presence, so it has no zero")
      case scalar => take(
          scope,
          scalar,
          if wireOf(scalar) == ProtobufWire.Fixed64Type then Raw.Fixed64(0)
          else if wireOf(scalar) == ProtobufWire.Fixed32Type then Raw.Fixed32(0)
          else Raw.Varint(0)
        )

  private def take(scope: String, fieldType: FieldType, raw: Raw): Either[String, ProtobufValue] =
    (fieldType, raw) match
      case (FieldType.Double, Raw.Fixed64(bits))    => Right(ProtobufValue.Double(java.lang.Double.longBitsToDouble(bits)))
      case (FieldType.Float, Raw.Fixed32(bits))     => Right(ProtobufValue.Float(java.lang.Float.intBitsToFloat(bits)))
      case (FieldType.Int32, Raw.Varint(value))     => Right(ProtobufValue.Int32(value.toInt))
      case (FieldType.SInt32, Raw.Varint(value))    => Right(ProtobufValue.Int32((value.toInt >>> 1) ^ -(value.toInt & 1)))
      case (FieldType.SFixed32, Raw.Fixed32(value)) => Right(ProtobufValue.Int32(value))
      case (FieldType.Int64, Raw.Varint(value))     => Right(ProtobufValue.Int64(value))
      case (FieldType.SInt64, Raw.Varint(value))    => Right(ProtobufValue.Int64((value >>> 1) ^ -(value & 1)))
      case (FieldType.SFixed64, Raw.Fixed64(value)) => Right(ProtobufValue.Int64(value))
      case (FieldType.UInt32, Raw.Varint(value))    => Right(ProtobufValue.UInt32(value & 0xffffffffL))
      case (FieldType.Fixed32, Raw.Fixed32(value))  => Right(ProtobufValue.UInt32(value.toLong & 0xffffffffL))
      case (FieldType.UInt64, Raw.Varint(value))    => Right(ProtobufValue.UInt64(BigInt(java.lang.Long.toUnsignedString(value))))
      case (FieldType.Fixed64, Raw.Fixed64(value))  => Right(ProtobufValue.UInt64(BigInt(java.lang.Long.toUnsignedString(value))))
      case (FieldType.Bool, Raw.Varint(value))      => Right(ProtobufValue.Bool(value != 0))
      case (FieldType.String, Raw.Bytes(bytes))     => Right(ProtobufValue.String(new String(bytes.toArray, StandardCharsets.UTF_8)))
      case (FieldType.Bytes, Raw.Bytes(bytes))      => Right(ProtobufValue.Bytes(bytes))
      case (FieldType.Named(name), _)               => pool.resolve(scope, name).flatMap:
          case Right(full) => raw match
              case Raw.Bytes(bytes) => decode(full, bytes)
              case _                => Left(s"expected a message $full, found another wire type")
          case Left((_, enumerated)) => raw match
              case Raw.Varint(value) => Right(
                  enumerated.values.collectFirst { case (name, number) if number == value.toInt => ProtobufValue.Enum(name) }
                    .getOrElse(ProtobufValue.EnumNumber(value.toInt))
                )
              case _ => Left(s"expected a value of enum $name, found another wire type")
      case (expected, _) => Left(s"expected a ${expected.toString.toLowerCase}, found another wire type")
