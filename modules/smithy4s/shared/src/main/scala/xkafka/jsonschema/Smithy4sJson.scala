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

import cats.effect.Concurrent
import cats.syntax.all.*
import fs2.Chunk
import io.circe.Json
import smithy.api.{Default, Documentation, JsonName, Length, Pattern, Range, Required, TimestampFormat}
import smithy4s.{Blob, Document, Hints, ShapeId}
import smithy4s.schema.{CollectionTag, EnumTag, Primitive, Schema as SmithySchema}
import smithy4s.schema.Schema.*
import xkafka.Serde
import xkafka.schemaregistry.{Part, RegistrySerde, Schema, SchemaFormat, SchemaRegistry, SchemaType, SubjectNaming, Writing}

/** Types smithy4s generates, as JSON Schema values of the registry: written and read by smithy4s-json, so records are the JSON smithy4s's services
  * write and read, with the JSON Schema that describes that JSON, made from the type's smithy4s schema.
  */
object Smithy4sJsonFormat:
  def apply[A](using schema: SmithySchema[A]): SchemaFormat[A] =
    val encoder = smithy4s.json.Json.payloadCodecs.encoders.fromSchema(schema)
    val decoder = smithy4s.json.Json.payloadCodecs.decoders.fromSchema(schema)
    new SchemaFormat[A]:
      override val schema: Schema             = Schema(Smithy4sJsonSchema.of(summon[SmithySchema[A]]).noSpaces, SchemaType.Json)
      override val recordName: Option[String] = Some(Smithy4sJsonSchema.name(summon[SmithySchema[A]].shapeId))

      // JSON names its properties, so a value is written the same whatever schema it is written with.
      override def writer(target: Schema): Either[String, A => Either[String, Chunk[Byte]]] =
        JsonSchemaFormat.json(target).map: _ =>
          value =>
            Either.catchNonFatal(Chunk.array(encoder.encode(value).toArray)).leftMap(error => s"the value cannot be written: ${error.getMessage}")

      override def reader(writer: Schema): Either[String, Chunk[Byte] => Either[String, A]] =
        // smithy4s-json counts no depth, so a record too deep is refused before it is read, as xkafka's own JSON reading refuses it.
        JsonSchemaFormat.json(writer).map(_ =>
          bytes =>
            JsonSchemaFormat.read(bytes).flatMap(_ => decoder.decode(Blob(bytes.toArray)).leftMap(error => s"the record cannot be read: $error"))
        )

/** Serdes of types smithy4s generates, written and read as smithy4s-json writes and reads them, with the registry's schemas, in Confluent's wire
  * format, as Confluent's `KafkaJsonSchemaSerializer` writes JSON.
  */
object Smithy4sJsonSerde:
  def of[F[_]: Concurrent, A: SmithySchema](
      registry: SchemaRegistry[F],
      part: Part,
      naming: SubjectNaming = SubjectNaming.TopicName,
      writing: Writing = Writing.Register
  ): F[Serde[F, A]] = RegistrySerde.of(registry, Smithy4sJsonFormat[A], part, naming, writing)

/** The JSON Schemas, of draft 7, of the JSON smithy4s-json writes and reads. */
object Smithy4sJsonSchema:
  /** The schema of the JSON of values of `schema`, titled by its shape's full name. */
  def of[A](schema: SmithySchema[A]): Json =
    val root                                                                                        = schema.shapeId
    val described                                                                                   = describe(schema, Set.empty, root)
    def close(pending: List[(String, () => Described)], done: Map[String, Json]): Map[String, Json] =
      pending match
        case Nil                                      => done
        case (name, _) :: rest if done.contains(name) => close(rest, done)
        case (name, body) :: rest                     =>
          val next = body()
          close(rest ++ next.definitions.toList, done.updated(name, next.json))
    val definitions = close(described.definitions.toList, Map.empty)
    val fields      = described.json.asObject.fold(List("allOf" -> Json.arr(described.json)))(_.toList)
    Json.fromFields(
      List("$schema" -> Json.fromString("http://json-schema.org/draft-07/schema#"), "title" -> Json.fromString(name(root))) ++
        schema.hints.get(Documentation).map(doc => "description" -> Json.fromString(doc.value)) ++ fields ++
        Option.when(definitions.nonEmpty)("definitions" -> Json.fromFields(definitions.toList.sortBy(_._1)))
    )

  /** A shape's full name, as Confluent's tools name a JSON Schema by its title. */
  def name(id: ShapeId): String = s"${id.namespace}.${id.name}"

  /** A schema's JSON, with the definitions of the shapes it refers to, by their names. */
  private final case class Described(json: Json, definitions: Map[String, () => Described])

  private object Described:
    def of(json: Json): Described = Described(json, Map.empty)

  private def typed(name: String, more: (String, Json)*): Json = Json.fromFields(("type" -> Json.fromString(name)) +: more)

  private val nullable: Json = typed("null")

  /** `json`, or null. */
  private def orNull(json: Json): Json =
    json.hcursor.downField("anyOf").focus.flatMap(_.asArray).filter(_.contains(nullable))
      .fold(Json.obj("anyOf" -> Json.arr(nullable, json)))(_ => json)

  /** The schema of `schema`, inside the shapes `inside`, whose root is `root`. A struct or union already inside is a reference to its definition. */
  private def describe(schema: SmithySchema[?], inside: Set[ShapeId], root: ShapeId): Described =
    schema match
      case PrimitiveSchema(_, hints, tag)      => Described.of(primitive(tag, hints))
      case CollectionSchema(_, _, tag, member) =>
        val items = describe(member, inside, root)
        val set   =
          tag match
            case CollectionTag.SetTag => List("uniqueItems" -> Json.True)
            case _                    => Nil
        items.copy(json = typed("array", ("items" -> items.json) :: set*))
      case MapSchema(_, _, _, _, value) =>
        val values = describe(value, inside, root)
        values.copy(json = typed("object", "additionalProperties" -> values.json))
      case EnumerationSchema(_, _, tag, values) => tag match
          case EnumTag.StringEnum(_, None) => Described
              .of(typed("string", "enum" -> Json.fromValues(values.map(value => Json.fromString(value.stringValue)))))
          case EnumTag.StringEnum(_, _) => Described.of(typed("string"))
          case EnumTag.IntEnum(_, None) => Described
              .of(typed("integer", "enum" -> Json.fromValues(values.map(value => Json.fromInt(value.intValue)))))
          case EnumTag.IntEnum(_, _) => Described.of(typed("integer"))
      case struct: StructSchema[?]     => named(struct.shapeId, inside, root)(within => structure(struct, within, root))
      case union: UnionSchema[?]       => named(union.shapeId, inside, root)(within => alternatives(union, within, root))
      case OptionSchema(_, underlying) =>
        val inner = describe(underlying, inside, root)
        inner.copy(json = orNull(inner.json))
      case BijectionSchema(underlying, _)           => describe(underlying, inside, root)
      case RefinementSchema(underlying, refinement) =>
        val inner = describe(underlying, inside, root)
        inner.copy(json = constrained(inner.json, refinement))
      case LazySchema(suspend) => describe(suspend.value, inside, root)

  /** A struct's or union's schema, or, where it is already inside itself, a reference to its definition, or to the root. */
  private def named(id: ShapeId, inside: Set[ShapeId], root: ShapeId)(body: Set[ShapeId] => Described): Described =
    if inside.contains(id) && id == root then Described.of(Json.obj("$ref" -> Json.fromString("#")))
    else if inside.contains(id) then
      Described(Json.obj("$ref" -> Json.fromString(s"#/definitions/${name(id)}")), Map(name(id) -> (() => body(Set(root, id)))))
    else body(inside + id)

  private def primitive(tag: Primitive[?], hints: Hints): Json =
    tag match
      case Primitive.PShort | Primitive.PInt | Primitive.PLong | Primitive.PByte | Primitive.PBigInt => typed("integer")
      case Primitive.PFloat | Primitive.PDouble | Primitive.PBigDecimal | Primitive.PDuration        => typed("number")
      case Primitive.PBoolean                                                                        => typed("boolean")
      case Primitive.PString | Primitive.PLocalTime                                                  => typed("string")
      case Primitive.PUUID           => typed("string", "format" -> Json.fromString("uuid"))
      case Primitive.PLocalDate      => typed("string", "format" -> Json.fromString("date"))
      case Primitive.POffsetDateTime => typed("string", "format" -> Json.fromString("date-time"))
      case Primitive.PBlob           => typed("string", "contentEncoding" -> Json.fromString("base64"))
      case Primitive.PDocument       => Json.obj()
      case Primitive.PTimestamp      => hints.get(TimestampFormat).getOrElse(TimestampFormat.EPOCH_SECONDS) match
          case TimestampFormat.DATE_TIME => typed("string", "format" -> Json.fromString("date-time"))
          case TimestampFormat.HTTP_DATE => typed("string")
          case _                         => typed("number")

  /** `json` with the keywords of a constraint smithy4s checks, `@length`, `@range` or `@pattern`, where JSON Schema has them. */
  private def constrained[A, B](json: Json, refinement: smithy4s.Refinement[A, B]): Json =
    val constraint          = Document.Encoder.fromSchema(refinement.tag.schema).encode(refinement.constraint)
    def number(key: String) =
      constraint match
        case Document.DObject(fields) => fields.get(key).collect { case Document.DNumber(value) => value }
        case _                        => None
    val kind                              = json.hcursor.get[String]("type").toOption
    val bytes                             = json.hcursor.downField("contentEncoding").succeeded
    def bounds(low: String, high: String) =
      number("min").map(low -> Json.fromBigDecimal(_)).toList ++ number("max").map(high -> Json.fromBigDecimal(_))
    val keywords =
      refinement.tag.id match
        case Length.id => kind match
            case Some("string") if !bytes => bounds("minLength", "maxLength")
            case Some("array")            => bounds("minItems", "maxItems")
            case Some("object")           => bounds("minProperties", "maxProperties")
            case _                        => Nil
        case Range.id   => bounds("minimum", "maximum")
        case Pattern.id => constraint match
            case Document.DString(value) => List("pattern" -> Json.fromString(value))
            case _                       => Nil
        case _ => Nil
    keywords match
      case Nil  => json
      case more => json.asObject.filter(fields => !fields.contains("$ref") && !fields.contains("anyOf") && !fields.contains("oneOf")) match
          case Some(fields) => Json.fromJsonObject(more.foldLeft(fields)((all, keyword) => all.add(keyword._1, keyword._2)))
          case None         => Json.obj("allOf" -> Json.arr(json, Json.fromFields(more)))

  private def structure(struct: StructSchema[?], inside: Set[ShapeId], root: ShapeId): Described =
    val fields =
      struct.fields.filterNot(_.hints.has(alloy.JsonUnknown)).map: field =>
        val label     = field.hints.get(JsonName).fold(field.label)(_.value)
        val required  = field.hints.has(Required)
        val described = describe(field.schema, inside, root)
        val default   = field.hints.get(Default).map(found => json(found.value))
        // A field that is not required may be left out, or null, which smithy4s reads as its default or as none.
        val value = if required then described.json else orNull(described.json)
        val full  =
          value.deepMerge(Json.fromFields(
            field.hints.get(Documentation).map(doc => "description" -> Json.fromString(doc.value)).toList ++ default.map("default" -> _)
          ))
        (label, required, full, described.definitions)
    Described(
      Json.fromFields(
        List("type" -> Json.fromString("object")) ++ struct.hints.get(Documentation).map(doc => "description" -> Json.fromString(doc.value)) ++ List(
          "properties" -> Json.fromFields(fields.map(field => field._1 -> field._3)),
          "required"   -> Json.fromValues(fields.collect { case (label, true, _, _) => Json.fromString(label) })
        )
      ),
      fields.map(_._4).foldLeft(Map.empty[String, () => Described])(_ ++ _)
    )

  private def alternatives(union: UnionSchema[?], inside: Set[ShapeId], root: ShapeId): Described =
    val alts =
      union.alternatives.filterNot(_.hints.has(alloy.JsonUnknown)).map: alt =>
        (alt.hints.get(JsonName).fold(alt.label)(_.value), describe(alt.schema, inside, root))
    val definitions = alts.map(_._2.definitions).foldLeft(Map.empty[String, () => Described])(_ ++ _)
    val branches    =
      union.hints.get(alloy.Discriminated) match
        case Some(property) => alts.map: (label, described) =>
            val tag =
              Json.obj(
                "type"       -> Json.fromString("object"),
                "properties" -> Json.obj(property.value -> Json.obj("const" -> Json.fromString(label))),
                "required"   -> Json.arr(Json.fromString(property.value))
              )
            Json.obj("allOf" -> Json.arr(described.json, tag))
        case None if union.hints.has(alloy.Untagged) => alts.map(_._2.json)
        case None                                    => alts.map: (label, described) =>
            Json.obj(
              "type"                 -> Json.fromString("object"),
              "properties"           -> Json.obj(label -> described.json),
              "required"             -> Json.arr(Json.fromString(label)),
              "additionalProperties" -> Json.False
            )
    val keyword = if union.hints.has(alloy.Untagged) then "anyOf" else "oneOf"
    Described(
      Json.fromFields(
        union.hints.get(Documentation).map(doc => "description" -> Json.fromString(doc.value)).toList :+ (keyword -> Json.fromValues(branches))
      ),
      definitions
    )

  /** A smithy4s document as JSON. */
  private def json(document: Document): Json =
    document match
      case Document.DNumber(value)  => Json.fromBigDecimal(value)
      case Document.DString(value)  => Json.fromString(value)
      case Document.DBoolean(value) => Json.fromBoolean(value)
      case Document.DNull           => Json.Null
      case Document.DArray(values)  => Json.fromValues(values.map(json))
      case Document.DObject(values) => Json.fromFields(values.toList.map((key, value) => key -> json(value)))
