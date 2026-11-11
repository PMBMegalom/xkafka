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

import cats.syntax.all.*
import com.google.protobuf.descriptor.{DescriptorProto, EnumDescriptorProto, FieldDescriptorProto, FileDescriptorProto}
import ProtobufSchema.{Enum, Field, FieldType, File, Label, Message, Syntax}

/** The .proto files ScalaPB generated messages from, as the descriptors it keeps of them, read into [[ProtobufSchema.File]]s. */
private[protobuf] object ScalaPBDescriptors:
  /** Declares ScalaPB's own options, which the file read leaves out, as it does every option but `packed` and `default`. */
  private val ScalaPBOptions = "scalapb/scalapb.proto"

  def file(proto: FileDescriptorProto): Either[String, File] =
    val name = proto.name.getOrElse("the file")
    for
      syntax <-
        proto.syntax match
          case None | Some("") | Some("proto2") => Right(Syntax.Proto2)
          case Some("proto3")                   => Right(Syntax.Proto3)
          case Some(other)                      => Left(s"$name is written in $other, which xkafka does not read")
      _        <- Either.cond(proto.extension.isEmpty, (), s"$name extends a message, and xkafka does not read extensions")
      messages <- proto.messageType.toList.traverse(message(syntax, proto.`package`.getOrElse(""), _))
    yield File(
      syntax,
      proto.`package`.filter(_.nonEmpty),
      proto.dependency.toList.filterNot(_ == ScalaPBOptions),
      messages,
      proto.enumType.toList.map(enumOf)
    )

  private def message(syntax: Syntax, scope: String, proto: DescriptorProto): Either[String, Message] =
    val name = proto.getName
    val full = if scope.isEmpty then name else s"$scope.$name"
    // A map field is a repeated field of an entry message the compiler declares in the message, which the file read declares as a map instead.
    val (entries, nested) = proto.nestedType.toList.partition(_.options.flatMap(_.mapEntry).contains(true))
    val entryNames        = entries.map(entry => s".$full.${entry.getName}" -> entry).toMap
    for
      _        <- Either.cond(proto.extension.isEmpty && proto.extensionRange.isEmpty, (), s"$full has extensions, which xkafka does not read")
      fields   <- proto.field.toList.traverse(field(syntax, full, proto, entryNames, _))
      messages <- nested.traverse(message(syntax, full, _))
    yield Message(name, fields, messages, proto.enumType.toList.map(enumOf))

  private def field(
      syntax: Syntax,
      full: String,
      message: DescriptorProto,
      entries: Map[String, DescriptorProto],
      proto: FieldDescriptorProto
  ): Either[String, Field] =
    val name = s"$full.${proto.getName}"
    for
      fieldType <-
        entries.get(proto.getTypeName) match
          case Some(entry) => (entry.field.find(_.getNumber == 1), entry.field.find(_.getNumber == 2)) match
              case (Some(key), Some(value)) => (typeOf(name, key), typeOf(name, value)).mapN(FieldType.Map(_, _))
              case _                        => Left(s"$name is a map whose entry lacks a key or a value")
          case None => typeOf(name, proto)
      // A proto3 `optional` field is the only one of a oneof the compiler makes up for it, which the file read does not declare.
      oneof = proto.oneofIndex.filterNot(_ => proto.getProto3Optional).flatMap(message.oneofDecl.lift).map(_.getName)
      label =
        if proto.label.contains(FieldDescriptorProto.Label.LABEL_REPEATED) then Label.Repeated
        else if proto.label.contains(FieldDescriptorProto.Label.LABEL_REQUIRED) then Label.Required
        else if oneof.isDefined || proto.getProto3Optional || syntax == Syntax.Proto2 then Label.Optional
        else Label.Implicit
    yield Field(
      proto.getName,
      proto.getNumber,
      fieldType,
      label,
      oneof,
      proto.options.flatMap(_.packed),
      proto.defaultValue.map(default(fieldType, _))
    )

  private def typeOf(name: String, proto: FieldDescriptorProto): Either[String, FieldType] =
    proto.`type`.map(_.name.stripPrefix("TYPE_").toLowerCase) match
      case Some("message" | "enum") => Right(FieldType.Named(proto.getTypeName))
      case Some("group")            => Left(s"$name is a group, which xkafka does not read")
      case Some(scalar)             => ProtobufSchema.scalars.get(scalar).toRight(s"$name has type $scalar, which xkafka does not read")
      case None                     => Left(s"$name has no type")

  /** A default as a .proto file writes it: a descriptor keeps text as it is and bytes escaped, and the file quotes both. */
  private def default(fieldType: FieldType, value: String): String =
    fieldType match
      case FieldType.String => s"\"${ProtobufSchemaText.escape(value)}\""
      case FieldType.Bytes  => s"\"$value\""
      case _                => value

  private def enumOf(proto: EnumDescriptorProto): Enum = Enum(proto.getName, proto.value.toList.map(value => value.getName -> value.getNumber))
