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

package xkafka.avro

import cats.syntax.all.*
import fs2.Chunk
import io.circe.{parser, Json, JsonObject}

/** Avro schemas as the JSON of an `.avsc` file, as the registry keeps them. */
object AvroSchemaJson:
  private val primitives: Map[String, AvroSchema] =
    Map(
      "null"    -> AvroSchema.Null,
      "boolean" -> AvroSchema.Boolean,
      "int"     -> AvroSchema.Int,
      "long"    -> AvroSchema.Long,
      "float"   -> AvroSchema.Float,
      "double"  -> AvroSchema.Double,
      "bytes"   -> AvroSchema.Bytes,
      "string"  -> AvroSchema.String
    )

  /** The logical types that take no parameters. */
  private val simpleLogicalTypes =
    List(
      LogicalType.Uuid,
      LogicalType.Date,
      LogicalType.TimeMillis,
      LogicalType.TimeMicros,
      LogicalType.TimestampMillis,
      LogicalType.TimestampMicros,
      LogicalType.TimestampNanos,
      LogicalType.LocalTimestampMillis,
      LogicalType.LocalTimestampMicros,
      LogicalType.LocalTimestampNanos
    )

  /** The schema `text` writes, or why it is not one. A named type is defined where it is first named, and named again by its name alone. */
  def parse(text: String): Either[String, AvroSchema] =
    parser.parse(text).leftMap(_.getMessage).flatMap(json => read(json, None, Map.empty).map(_._1))

  /** The schema as compact JSON: each named type defined where it first appears, and named by its name after. */
  def render(schema: AvroSchema): String = write(schema, None, AvroSchema.definitions(schema), Set.empty)._1.noSpaces

  private type Defined = Map[FullName, AvroSchema]

  private def read(json: Json, namespace: Option[String], defined: Defined): Either[String, (AvroSchema, Defined)] =
    json.fold(
      Left("a schema cannot be null"),
      _ => Left("a schema cannot be a boolean"),
      _ => Left("a schema cannot be a number"),
      name => named(name, namespace, defined).map((_, defined)),
      branches =>
        branches.toList.foldM((List.empty[AvroSchema], defined)) { case ((sofar, known), branch) =>
          read(branch, namespace, known).map((schema, more) => (sofar :+ schema, more))
        }.flatMap((schemas, known) => union(schemas).map((_, known))),
      fields => complex(fields, namespace, defined)
    )

  private def named(name: String, namespace: Option[String], defined: Defined): Either[String, AvroSchema] =
    primitives.get(name).toRight(name).orElse:
      val full = FullName.from(name, namespace)
      // A name without a namespace may name a type defined in no namespace, as Java looks it up.
      List(full, FullName(None, name)).find(defined.contains).map(AvroSchema.Named(_)).toRight(s"'$name' names no type defined before it")

  /** A union, refused where it holds a union, or two branches of one type other than named types of different names, as the specification says. */
  private def union(branches: List[AvroSchema]): Either[String, AvroSchema] =
    def kind(schema: AvroSchema): String =
      schema match
        case AvroSchema.Logical(logicalType)  => kind(logicalType.underlying)
        case named if named.fullName.nonEmpty => named.fullName.fold("")(_.value)
        case other                            => other.productPrefix
    if branches.exists { case AvroSchema.Union(_) => true; case _ => false } then Left("a union cannot hold a union")
    else
      branches.map(kind).diff(branches.map(kind).distinct).headOption match
        case Some(twice) => Left(s"a union holds '$twice' twice")
        case None        => Right(AvroSchema.Union(branches))

  private def complex(fields: JsonObject, namespace: Option[String], defined: Defined): Either[String, (AvroSchema, Defined)] =
    def string(key: String)            = fields(key).flatMap(_.asString)
    def number(key: String)            = fields(key).flatMap(_.asNumber).flatMap(_.toInt)
    def required(key: String)          = string(key).toRight(s"a schema of type '${string("type").getOrElse("")}' needs a '$key'")
    def fullName                       = required("name").map(name => FullName.from(name, string("namespace").orElse(namespace)))
    def aliases(space: Option[String]) = fields("aliases").flatMap(_.asArray).toList.flatten.flatMap(_.asString).map(FullName.from(_, space))
    def define(name: FullName, schema: AvroSchema, known: Defined) =
      if known.contains(name) then Left(s"'${name.value}' is defined twice") else Right(known.updated(name, schema))
    fields("type") match
      case None                                  => Left("a schema object needs a 'type'")
      case Some(inner) if inner.asString.isEmpty => read(inner, namespace, defined)
      case Some(inner)                           => inner.asString.getOrElse("") match
          case "record" | "error" => fullName.flatMap: name =>
              // The record is defined before its fields are read, so a field can name it.
              val placeholder = AvroSchema.Record(name, Nil)
              define(name, placeholder, defined).flatMap: known =>
                fields("fields").flatMap(_.asArray).toRight(s"record '${name.value}' needs 'fields'").flatMap: list =>
                  list.toList.foldM((List.empty[AvroField], known)) { case ((sofar, known), json) =>
                    field(json, name.namespace, known).map((read, more) => (sofar :+ read, more))
                  }.flatMap: (read, known) =>
                    val names = read.map(_.name)
                    if names.distinct.size != names.size then Left(s"record '${name.value}' names a field twice")
                    else
                      val record = AvroSchema.Record(name, read, string("doc"), aliases(name.namespace))
                      Right((record, known.updated(name, record)))
          case "enum" => fullName.flatMap: name =>
              val symbols = fields("symbols").flatMap(_.asArray).map(_.toList.flatMap(_.asString))
              symbols.toRight(s"enum '${name.value}' needs 'symbols'").flatMap: symbols =>
                val default = string("default")
                if default.exists(!symbols.contains(_)) then Left(s"enum '${name.value}' has a default that is not one of its symbols")
                else
                  val schema = AvroSchema.Enum(name, symbols, default, string("doc"), aliases(name.namespace))
                  define(name, schema, defined).map((schema, _))
          case "fixed" => fullName.flatMap: name =>
              number("size").filter(_ >= 0).toRight(s"fixed '${name.value}' needs a 'size'").flatMap: size =>
                val fixed  = AvroSchema.Fixed(name, size, aliases(name.namespace))
                val schema =
                  (string("logicalType"), number("precision")) match
                    case (Some("decimal"), Some(precision)) if precision > 0 && number("scale").getOrElse(0) <= precision =>
                      AvroSchema.Logical(LogicalType.FixedDecimal(name, size, precision, number("scale").getOrElse(0)))
                    case _ => fixed
                define(name, schema, defined).map((schema, _))
          case "array" => fields("items").toRight("an array needs 'items'").flatMap(read(_, namespace, defined))
              .map((items, known) => (AvroSchema.Array(items), known))
          case "map" => fields("values").toRight("a map needs 'values'").flatMap(read(_, namespace, defined))
              .map((values, known) => (AvroSchema.Map(values), known))
          case other => named(other, namespace, defined).map: base =>
              // A logical type that is unknown or does not fit its type is left out, as the specification says, so the type is read as it is.
              val logical =
                (string("logicalType"), base) match
                  case (Some("decimal"), AvroSchema.Bytes) => number("precision")
                      .filter(precision => precision > 0 && number("scale").getOrElse(0) <= precision)
                      .map(precision => LogicalType.Decimal(precision, number("scale").getOrElse(0)))
                  case (Some(name), _) => simpleLogicalTypes.find(known => known.name == name && known.underlying == base)
                  case (None, _)       => None
              (logical.fold(base)(AvroSchema.Logical(_)), defined)

  private def field(json: Json, namespace: Option[String], defined: Defined): Either[String, (AvroField, Defined)] =
    json.asObject.toRight("a field must be an object").flatMap: fields =>
      fields("name").flatMap(_.asString).toRight("a field needs a 'name'").flatMap: name =>
        fields("type").toRight(s"field '$name' needs a 'type'").flatMap(read(_, namespace, defined)).flatMap: (schema, known) =>
          fields("default").traverse(default(_, schema, known).leftMap(reason => s"field '$name' has a default that does not fit its type: $reason"))
            .map: default =>
              val doc     = fields("doc").flatMap(_.asString)
              val aliases = fields("aliases").flatMap(_.asArray).toList.flatten.flatMap(_.asString)
              (AvroField(name, schema, default, doc, aliases), known)

  /** The value `json` gives a field of `schema` by default, as the specification writes defaults: bytes as text of the characters 0 to 255, and a
    * union's as its first branch's.
    */
  def default(json: Json, schema: AvroSchema, defined: Map[FullName, AvroSchema]): Either[String, AvroValue] =
    def number = json.asNumber.toRight(s"expected a number, found ${json.noSpaces}")
    def text   = json.asString.toRight(s"expected text, found ${json.noSpaces}")
    def bytes  =
      text.flatMap: chars =>
        if chars.forall(_ < 256) then Right(Chunk.array(chars.map(_.toByte).toArray)) else Left(s"'$chars' holds a character above 255")
    schema match
      case AvroSchema.Null                       => Either.cond(json.isNull, AvroValue.Null, s"expected null, found ${json.noSpaces}")
      case AvroSchema.Boolean                    => json.asBoolean.map(AvroValue.Boolean(_)).toRight(s"expected a boolean, found ${json.noSpaces}")
      case AvroSchema.Int                        => number.flatMap(_.toInt.toRight(s"${json.noSpaces} is not an int")).map(AvroValue.Int(_))
      case AvroSchema.Long                       => number.flatMap(_.toLong.toRight(s"${json.noSpaces} is not a long")).map(AvroValue.Long(_))
      case AvroSchema.Float                      => number.map(value => AvroValue.Float(value.toDouble.toFloat))
      case AvroSchema.Double                     => number.map(value => AvroValue.Double(value.toDouble))
      case AvroSchema.Bytes                      => bytes.map(AvroValue.Bytes(_))
      case AvroSchema.String                     => text.map(AvroValue.String(_))
      case AvroSchema.Record(name, fields, _, _) => json.asObject.toRight(s"expected an object, found ${json.noSpaces}").flatMap: supplied =>
          fields.traverse: field =>
            supplied(field.name).fold(field.default.toRight(s"no value for field '${field.name}'"))(default(_, field.schema, defined))
              .map(field.name -> _)
          .map(values => AvroValue.Record(name, values.toVector))
      case AvroSchema.Enum(name, symbols, _, _, _) => text.filterOrElse(symbols.contains, s"not a symbol of '${name.value}'")
          .map(AvroValue.Enum(name, _))
      case AvroSchema.Array(items) => json.asArray.toRight(s"expected an array, found ${json.noSpaces}")
          .flatMap(_.traverse(default(_, items, defined))).map(AvroValue.Array(_))
      case AvroSchema.Map(values) => json.asObject.toRight(s"expected an object, found ${json.noSpaces}")
          .flatMap(_.toVector.traverse((key, value) => default(value, values, defined).map(key -> _))).map(AvroValue.Map(_))
      case AvroSchema.Union(branches)      => branches.headOption.toRight("a union without branches has no values").flatMap(default(json, _, defined))
      case AvroSchema.Fixed(name, size, _) => bytes.filterOrElse(_.size == size, s"expected $size bytes").map(AvroValue.Fixed(name, _))
      case AvroSchema.Logical(logicalType) => default(json, logicalType.underlying, defined)
      case AvroSchema.Named(name)          => defined.get(name).toRight(s"'${name.value}' is not defined").flatMap(default(json, _, defined))

  /** The default `value` of a field of `schema`, as JSON. */
  def defaultJson(value: AvroValue, schema: AvroSchema, defined: Map[FullName, AvroSchema]): Json =
    def chars(bytes: Chunk[Byte]) = Json.fromString(bytes.toArray.map(byte => (byte & 0xff).toChar).mkString)
    (value, schema) match
      case (_, AvroSchema.Union(first :: _))                                  => defaultJson(value, first, defined)
      case (_, AvroSchema.Logical(logicalType))                               => defaultJson(value, logicalType.underlying, defined)
      case (_, AvroSchema.Named(name)) if defined.contains(name)              => defaultJson(value, defined(name), defined)
      case (AvroValue.Null, _)                                                => Json.Null
      case (AvroValue.Boolean(value), _)                                      => Json.fromBoolean(value)
      case (AvroValue.Int(value), _)                                          => Json.fromInt(value)
      case (AvroValue.Long(value), _)                                         => Json.fromLong(value)
      case (AvroValue.Float(value), _)                                        => Json.fromFloatOrString(value)
      case (AvroValue.Double(value), _)                                       => Json.fromDoubleOrString(value)
      case (AvroValue.Bytes(bytes), _)                                        => chars(bytes)
      case (AvroValue.String(value), _)                                       => Json.fromString(value)
      case (AvroValue.Enum(_, symbol), _)                                     => Json.fromString(symbol)
      case (AvroValue.Fixed(_, bytes), _)                                     => chars(bytes)
      case (AvroValue.Record(_, fields), AvroSchema.Record(_, schemas, _, _)) => Json.fromFields(
          fields.map((name, value) => name -> schemas.find(_.name == name).fold(Json.Null)(field => defaultJson(value, field.schema, defined)))
        )
      case (AvroValue.Array(items), AvroSchema.Array(schema)) => Json.fromValues(items.map(defaultJson(_, schema, defined)))
      case (AvroValue.Map(entries), AvroSchema.Map(schema))   => Json
          .fromFields(entries.map((key, value) => key -> defaultJson(value, schema, defined)))
      case _ => Json.Null

  private def write(
      schema: AvroSchema,
      namespace: Option[String],
      defined: Map[FullName, AvroSchema],
      written: Set[FullName]
  ): (Json, Set[FullName]) =
    def name(full: FullName)                 = if full.namespace == namespace then full.name else full.value
    def header(kind: String, full: FullName) =
      List("type" -> Json.fromString(kind), "name" -> Json.fromString(full.name)) ++
        Option.when(full.namespace != namespace)("namespace" -> Json.fromString(full.namespace.getOrElse("")))
    def aliases(names: List[FullName], space: Option[String]) =
      Option.when(names.nonEmpty)(
        "aliases" -> Json.fromValues(names.map(alias => Json.fromString(if alias.namespace == space then alias.name else alias.value)))
      )
    schema match
      case AvroSchema.Null                                  => (Json.fromString("null"), written)
      case AvroSchema.Boolean                               => (Json.fromString("boolean"), written)
      case AvroSchema.Int                                   => (Json.fromString("int"), written)
      case AvroSchema.Long                                  => (Json.fromString("long"), written)
      case AvroSchema.Float                                 => (Json.fromString("float"), written)
      case AvroSchema.Double                                => (Json.fromString("double"), written)
      case AvroSchema.Bytes                                 => (Json.fromString("bytes"), written)
      case AvroSchema.String                                => (Json.fromString("string"), written)
      case AvroSchema.Named(full)                           => (Json.fromString(name(full)), written)
      case named if named.fullName.exists(written.contains) => (named.fullName.fold(Json.Null)(full => Json.fromString(name(full))), written)
      case AvroSchema.Record(full, fields, doc, names)      =>
        val (rendered, after) =
          fields.foldLeft((List.empty[Json], written + full)) { case ((sofar, seen), field) =>
            val (schema, more) = write(field.schema, full.namespace, defined, seen)
            val json           =
              Json.fromFields(
                List("name" -> Json.fromString(field.name), "type" -> schema) ++ field.doc.map("doc" -> Json.fromString(_)) ++
                  field.default.map("default" -> defaultJson(_, field.schema, defined)) ++
                  Option.when(field.aliases.nonEmpty)("aliases" -> Json.fromValues(field.aliases.map(Json.fromString)))
              )
            (sofar :+ json, more)
          }
        (
          Json.fromFields(
            header("record", full) ++ doc.map("doc" -> Json.fromString(_)) ++ List("fields" -> Json.fromValues(rendered)) ++
              aliases(names, full.namespace)
          ),
          after
        )
      case AvroSchema.Enum(full, symbols, default, doc, names) =>
        val json =
          Json.fromFields(
            header("enum", full) ++ doc.map("doc" -> Json.fromString(_)) ++ List("symbols" -> Json.fromValues(symbols.map(Json.fromString))) ++
              default.map("default" -> Json.fromString(_)) ++ aliases(names, full.namespace)
          )
        (json, written + full)
      case AvroSchema.Fixed(full, size, names) =>
        (Json.fromFields(header("fixed", full) ++ List("size" -> Json.fromInt(size)) ++ aliases(names, full.namespace)), written + full)
      case AvroSchema.Array(items) => write(items, namespace, defined, written)
          .leftMap(items => Json.obj("type" -> Json.fromString("array"), "items" -> items))
      case AvroSchema.Map(values) => write(values, namespace, defined, written)
          .leftMap(values => Json.obj("type" -> Json.fromString("map"), "values" -> values))
      case AvroSchema.Union(branches) => branches.foldLeft((List.empty[Json], written)) { case ((sofar, seen), branch) =>
          write(branch, namespace, defined, seen).leftMap(sofar :+ _)
        }.leftMap(Json.fromValues)
      case AvroSchema.Logical(LogicalType.FixedDecimal(full, size, precision, scale)) =>
        val json =
          Json.fromFields(
            header("fixed", full) ++ List(
              "size"        -> Json.fromInt(size),
              "logicalType" -> Json.fromString("decimal"),
              "precision"   -> Json.fromInt(precision),
              "scale"       -> Json.fromInt(scale)
            )
          )
        (json, written + full)
      case AvroSchema.Logical(logicalType @ LogicalType.Decimal(precision, scale)) => (
          Json.obj(
            "type"        -> Json.fromString("bytes"),
            "logicalType" -> Json.fromString(logicalType.name),
            "precision"   -> Json.fromInt(precision),
            "scale"       -> Json.fromInt(scale)
          ),
          written
        )
      case AvroSchema.Logical(logicalType) =>
        val (base, _) = write(logicalType.underlying, namespace, defined, written)
        (Json.obj("type" -> base, "logicalType" -> Json.fromString(logicalType.name)), written)
