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

/** The number the registry gives a schema, which the records written with it carry. */
opaque type SchemaId = Int

object SchemaId:
  def apply(value: Int): SchemaId = value

  extension (id: SchemaId) def value: Int = id

  given CanEqual[SchemaId, SchemaId] = CanEqual.derived

/** The name the registry keeps a schema's versions under, as `orders-value` for the values of the topic `orders`. */
opaque type Subject = String

object Subject:
  /** The subject named `value`, refused where it is empty. */
  def from(value: String): Either[SchemaRegistryError, Subject] = Either.cond(value.nonEmpty, value, SchemaRegistryError.EmptySubject)

  extension (subject: Subject) def value: String = subject

  given CanEqual[Subject, Subject] = CanEqual.derived

/** The language a schema is written in. */
enum SchemaType derives CanEqual:
  case Avro, Protobuf, Json

  /** How the registry names it. */
  def name: String =
    this match
      case Avro     => "AVRO"
      case Protobuf => "PROTOBUF"
      case Json     => "JSON"

object SchemaType:
  /** The type the registry calls `name`, where it is one of these. */
  def named(name: String): Option[SchemaType] = values.find(_.name == name)

/** A schema another one uses, known to it as `name`, and registered as `version` of `subject`. */
final case class SchemaReference(name: String, subject: Subject, version: Int)

/** A schema's text in its language, with the schemas it uses. */
final case class Schema(text: String, schemaType: SchemaType, references: List[SchemaReference] = Nil)

/** A schema as registered: its id, and the version of `subject` it is. */
final case class Registered(id: SchemaId, subject: Subject, version: Int, schema: Schema)

/** Whether a schema could be registered as a subject's next version, under the subject's compatibility rule. */
enum Compatibility derives CanEqual:
  case Compatible

  /** It could not, for the reasons the registry gives. */
  case Incompatible(reasons: List[String])
