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

package xkafka.jsonschema

import java.time.{Instant, OffsetDateTime, OffsetTime}
import java.time.format.DateTimeFormatter

import scala.util.Try

import cats.syntax.all.*
import io.circe.{Json, JsonObject}
import xkafka.schemaregistry.Shown

/** What the classes xkafka-codegen generates from JSON Schema files call: codecs of their schemas, and the writing and reading of their properties.
  */
object Generated:
  /** A schema generated code holds as its text, which the generator wrote from a valid schema. */
  def parsed(text: String): Json = io.circe.jawn.parse(text).fold(error => throw new IllegalArgumentException(error.message), identity)

  /** The codec of a type generated from a whole schema file, whose JSON is `described`, registered as it is and named by its `title`. */
  def codec[A](described: Json, encode: A => Either[String, Json], decode: Json => Either[String, A]): JsonSchemaCodec[A] =
    val (write, read) = (encode, decode)
    new JsonSchemaCodec[A]:
      override private[jsonschema] def describe(seen: Set[String]): Described = Described(described, Map.empty)
      override def name: Option[String]                                       = described.hcursor.get[String]("title").toOption
      override def encode(value: A): Either[String, Json]                     = write(value)
      override def decode(json: Json): Either[String, A]                      = read(json)

  /** The codec of a type generated from part of a schema file, which is never registered on its own, so its schema accepts any value. */
  def part[A](encode: A => Either[String, Json], decode: Json => Either[String, A]): JsonSchemaCodec[A] =
    JsonSchemaCodec.instance(Json.obj(), encode, decode)

  /** Any JSON value, as it is. */
  val anything: JsonSchemaCodec[Json] = part(Right(_), Right(_))

  /** A time of day with its offset from UTC, JSON Schema's `time`, written with its seconds, as RFC 3339 has it. */
  val time: JsonSchemaCodec[OffsetTime] =
    JsonSchemaCodec[String]
      .emap(text => Try(OffsetTime.parse(text)).toEither.leftMap(_ => s"'${Shown(text)}' is not a time with an offset"))(_.format(
        DateTimeFormatter.ISO_OFFSET_TIME
      ))

  /** An instant, JSON Schema's `date-time`, read with any offset from UTC and written in UTC. */
  val dateTime: JsonSchemaCodec[Instant] =
    JsonSchemaCodec[String]
      .emap(text => Try(OffsetDateTime.parse(text).toInstant).toEither.leftMap(_ => s"'${Shown(text)}' is not a date-time"))(_.toString)

  /** An object of `properties`, each written where it is not left out. */
  def write(properties: Either[String, Option[(String, Json)]]*): Either[String, Json] =
    properties.toList.sequence.map(written => Json.fromFields(written.flatten))

  /** A property that is always written. */
  def property[A](name: String, value: A)(using codec: JsonSchemaCodec[A]): Either[String, Option[(String, Json)]] =
    codec.encode(value).bimap(reason => s"$name: $reason", json => Some(name -> json))

  /** A property that is left out where it has no value. */
  def optional[A](name: String, value: Option[A])(using codec: JsonSchemaCodec[A]): Either[String, Option[(String, Json)]] =
    value.flatTraverse(property(name, _))

  /** A property whose value is always `value`, which tells the schema's branches apart. */
  def constant(name: String, value: Json): Either[String, Option[(String, Json)]] = Right(Some(name -> value))

  /** The properties of `json`, an object of the type `name`, whose properties are `known`; where `strict`, it must have no others. */
  def read(json: Json, name: String, strict: Boolean, known: Set[String]): Either[String, JsonObject] =
    json.asObject.toRight(s"expected an object of $name, found ${Shown(json.noSpaces)}").flatMap: fields =>
      fields.keys.filterNot(known).toList match
        case others if strict && others.nonEmpty => Left(s"$name has no properties ${others.mkString(", ")}")
        case _                                   => Right(fields)

  /** A property that must be there. */
  def readRequired[A](fields: JsonObject, name: String)(using codec: JsonSchemaCodec[A]): Either[String, A] =
    fields(name).toRight(s"no property '$name'").flatMap(decoded(name))

  /** A property that may be left out, or null. */
  def readOptional[A](fields: JsonObject, name: String)(using codec: JsonSchemaCodec[A]): Either[String, Option[A]] =
    fields(name).filterNot(_.isNull).traverse(decoded(name))

  /** A property that takes `default` where it is left out. */
  def readDefaulted[A](fields: JsonObject, name: String, default: => A)(using codec: JsonSchemaCodec[A]): Either[String, A] =
    fields(name).fold(Right(default))(decoded(name))

  /** A property whose value must be `value`. */
  def readConstant(fields: JsonObject, name: String, value: Json): Either[String, Unit] =
    fields(name) match
      case Some(found) if found == value => Right(())
      case Some(found)                   => Left(s"'$name' is ${found.noSpaces}, not ${value.noSpaces}")
      case None                          => Left(s"no property '$name'")

  private def decoded[A](name: String)(json: Json)(using codec: JsonSchemaCodec[A]): Either[String, A] =
    codec.decode(json).leftMap(reason => s"$name: $reason")

  /** The text of each of an enum's values, written and read as text. */
  def symbols[A](name: String, symbols: List[(A, String)]): (A => Either[String, Json], Json => Either[String, A]) = (
    value =>
      symbols.collectFirst { case (known, text) if known == value => Json.fromString(text) }.toRight(s"${Shown(value)} is not a value of $name"),
    json =>
      json.asString.flatMap(text => symbols.collectFirst { case (known, `text`) => known })
        .toRight(s"expected a value of $name, found ${Shown(json.noSpaces)}")
  )

  /** Reads a value of the `oneOf` or `anyOf` `name` by its `branches`, named, each reading strictly, refusing properties its schema does not name, or
    * not. A value is read by the one branch that reads it strictly, or else by the one that reads it at all; where several do, a `oneOf`, whose value
    * must be of exactly one branch, fails, and an `anyOf` takes the first.
    */
  def union[A](name: String, exactlyOne: Boolean, branches: List[(String, (Json, Boolean) => Either[String, A])]): Json => Either[String, A] =
    json =>
      def reading(strict: Boolean) = branches.flatMap((branch, read) => read(json, strict).toOption.map(branch -> _))
      reading(strict = true).some.filter(_.nonEmpty).getOrElse(reading(strict = false)) match
        case Nil                            => Left(s"expected a value of $name, found ${Shown(json.noSpaces)}, which none of its branches reads")
        case (_, value) :: Nil              => Right(value)
        case (_, value) :: _ if !exactlyOne => Right(value)
        case several                        =>
          Left(s"expected a value of exactly one branch of $name, found ${Shown(json.noSpaces)}, which ${several.map(_._1).mkString(", ")} all read")
