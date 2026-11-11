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

/** A .proto file as a value, for schemas known only as the program runs. [[ProtobufSchemaText]] reads and writes it as text. */
object ProtobufSchema:
  enum Syntax derives CanEqual:
    case Proto2, Proto3

  /** A file: its syntax, its package, the files it imports by name, and the messages and enums it declares. */
  final case class File(syntax: Syntax, packageName: Option[String], imports: List[String], messages: List[Message], enums: List[Enum]):
    /** The message named `fullName`, without a leading dot, and its path through the file's messages, as Confluent's message indexes give it. */
    def message(fullName: String): Option[(Message, List[Int])] =
      def within(prefix: String, messages: List[Message], path: List[Int]): Option[(Message, List[Int])] =
        messages.zipWithIndex.collectFirstSome: (message, index) =>
          val full = if prefix.isEmpty then message.name else s"$prefix.${message.name}"
          if full == fullName then Some((message, (index :: path).reverse))
          else if fullName.startsWith(s"$full.") then within(full, message.messages, index :: path)
          else None
      within(packageName.getOrElse(""), messages, Nil)

    /** The full name of the message at `path` through the file's messages. */
    def messageAt(path: List[Int]): Option[String] =
      def at(prefix: String, messages: List[Message], path: List[Int]): Option[String] =
        path match
          case Nil           => None
          case index :: rest => messages.lift(index).flatMap: message =>
              val full = if prefix.isEmpty then message.name else s"$prefix.${message.name}"
              if rest.isEmpty then Some(full) else at(full, message.messages, rest)
      at(packageName.getOrElse(""), messages, path)

  /** A message: its fields, in the order they are declared, and the messages and enums it declares. */
  final case class Message(name: String, fields: List[Field], messages: List[Message] = Nil, enums: List[Enum] = Nil)

  /** A field. A field of a `oneof` names it, and is labelled [[Label.Optional]]. `packed` is set where the file says whether the field is packed, and
    * `default` where a proto2 file gives the field a default, as the file writes it.
    */
  final case class Field(
      name: String,
      number: Int,
      fieldType: FieldType,
      label: Label,
      oneof: Option[String] = None,
      packed: Option[Boolean] = None,
      default: Option[String] = None
  )

  enum Label derives CanEqual:
    /** A proto3 field without a label: it records no presence, and is left out where it is zero. */
    case Implicit

    /** A field that records whether it is set. */
    case Optional

    /** A proto2 field that must be set. */
    case Required

    case Repeated

  enum FieldType derives CanEqual:
    case Double, Float, Int32, Int64, UInt32, UInt64, SInt32, SInt64, Fixed32, Fixed64, SFixed32, SFixed64, Bool, String, Bytes

    /** A message or enum, by its name as the file writes it, which is resolved in the scope of the message the field is in. */
    case Named(name: scala.Predef.String)

    case Map(key: FieldType, value: FieldType)

  /** An enum and its values' names and numbers, in the order they are declared. */
  final case class Enum(name: String, values: List[(String, Int)])

  private[protobuf] val scalars: Map[String, FieldType] =
    List(
      FieldType.Double,
      FieldType.Float,
      FieldType.Int32,
      FieldType.Int64,
      FieldType.UInt32,
      FieldType.UInt64,
      FieldType.SInt32,
      FieldType.SInt64,
      FieldType.Fixed32,
      FieldType.Fixed64,
      FieldType.SFixed32,
      FieldType.SFixed64,
      FieldType.Bool,
      FieldType.String,
      FieldType.Bytes
    ).map(scalar => scalar.toString.toLowerCase -> scalar).toMap
