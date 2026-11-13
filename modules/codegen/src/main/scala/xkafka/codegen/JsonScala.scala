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

package xkafka.codegen

import java.time.{LocalDate, OffsetDateTime, OffsetTime}
import java.util.{Base64, UUID}

import scala.util.Try

import cats.syntax.all.*
import io.circe.{Json, JsonObject}

/** The Scala source of the type a JSON Schema file describes, with the types it holds in its companion, where its package also holds the types named
  * `neighbours`.
  */
private[codegen] final class JsonScala(root: JsonSchemaGenerator.Root, bigDecimals: Boolean, neighbours: Set[String]):
  import JsonScala.*

  private val rootPath = List(root.name)

  private def fullName(path: List[String]): String = s"${root.packageName}.${path.mkString(".")}"

  /** The schema's definitions, by the references that name them. */
  private val definitions: Map[String, (String, Json)] =
    List("definitions", "$defs").flatMap: section =>
      root.schema.hcursor.downField(section).focus.flatMap(_.asObject).toList.flatMap(_.toList).map: (key, node) =>
        s"#/$section/${key.replace("~", "~0").replace("/", "~1")}" -> (key, node)
    .toMap

  def source: Either[String, String] =
    for
      top     <- rootDeclaration
      decls   <- close(top.refs, top.decls)
      checked <- unique(decls)
      _       <- choiceBranches(checked)
      text    <- Renderer(checked, choiceParents(checked)).file
    yield text

  /** The root's type and what it refers to. */
  private def rootDeclaration: Either[String, Walked] =
    root.schema.asObject.toRight("the schema is not an object").flatMap: schema =>
      if schema.contains("oneOf") || schema.contains("anyOf") then choice(schema)
      else
        walk(root.schema, "#", Nil, root.name, Set.empty).flatMap: walked =>
          walked.shape match
            case Shape.Named(`rootPath`) => Right(walked)
            case _ => Left("the schema is not an object, an enum of text, or a oneOf or anyOf of objects, which the generator makes types of")

  /** A root that is a `oneOf` or `anyOf` of objects, as a sealed trait whose cases are its branches. */
  private def choice(schema: JsonObject): Either[String, Walked] =
    for
      found <- unionOf(schema, "#")
      (keyword, branches) = found
      walked <-
        branches.zipWithIndex.traverse: (branch, index) =>
          walk(branch, s"#/$keyword/$index", rootPath, branchName(branch, root.name, index), Set.empty)
      paths <-
        walked.traverse:
          case Walked(Shape.Named(path), _, _) => Right(path)
          case _                               => Left(s"#/$keyword has a branch that is not an object, which a sealed trait cannot have as a case")
      _ <- Either.cond(paths.distinct.size == paths.size, (), s"#/$keyword has a branch twice")
    yield
      val doc = schema("description").flatMap(_.asString)
      Walked(Shape.Named(rootPath), Decl.Choice(rootPath, doc, paths, keyword == "oneOf") :: walked.flatMap(_.decls), walked.flatMap(_.refs).toSet)

  private def choiceParents(decls: List[Decl]): Map[List[String], String] =
    decls.collect { case Decl.Choice(path, _, branches, _) => branches.map(_ -> scalaPath(path)) }.flatten.toMap

  private def choiceBranches(decls: List[Decl]): Either[String, Unit] =
    decls.collectFirst {
      case Decl.Choice(_, _, branches, _)
          if branches.exists(branch => !decls.exists(decl => decl.path == branch && decl.isInstanceOf[Decl.Record])) =>
        "the schema has a branch that is not an object, which a sealed trait cannot have as a case"
    }.toLeft(())

  /** The declarations of the definitions `pending` refers to, and of those they refer to, with `found`. */
  private def close(pending: Set[String], found: List[Decl]): Either[String, List[Decl]] =
    def go(pending: List[String], done: Set[String], found: List[Decl]): Either[String, List[Decl]] =
      pending match
        case Nil                                   => Right(found)
        case first :: rest if done.contains(first) => go(rest, done, found)
        case first :: rest                         =>
          val (key, node) = definitions(first)
          for
            name   <- Names.pascal(key).toRight(s"$first makes no Scala name")
            walked <- walk(node, first, rootPath, name, Set.empty)
            more   <- go(rest ++ walked.refs.toList.sorted, done + first, found ++ walked.decls)
          yield more
    go(pending.toList.sorted, Set.empty, found)

  /** The declarations, each once, where none shares its name with another in the same companion, or with the root. */
  private def unique(decls: List[Decl]): Either[String, List[Decl]] =
    val distinct = decls.distinct
    distinct.groupBy(_.path).collectFirst { case (path, copies) if copies.size > 1 => s"two types would be named ${fullName(path)}" }
      .orElse(distinct.collectFirst {
        case decl if decl.path.size > 1 && decl.path.last == root.name => s"${fullName(decl.path)} would be named as the schema's own type"
      }).toLeft(distinct)

  /** The type of `node`, at `where`, with the types it declares in the companion of `owner`, where a type of its own is named `hint`. A reference to
    * a definition without a type of its own is followed, unless it is one of `inlining`, which `node` is inside.
    */
  private def walk(node: Json, where: String, owner: List[String], hint: String, inlining: Set[String]): Either[String, Walked] =
    node.asBoolean match
      case Some(true)  => Right(Walked.plain(Kind.Anything))
      case Some(false) => Left(s"$where accepts no value, which has no Scala type")
      case None        => node.asObject.toRight(s"$where is not a schema").flatMap: schema =>
          refused(schema, where) *> {
            if schema.contains("$ref") then reference(schema, where, inlining)
            else if schema.contains("oneOf") || schema.contains("anyOf") then union(schema, where, owner, hint, inlining)
            else if schema.contains("const") then Left(s"$where is a constant, which the generator reads only as a required property")
            else
              enumeration(schema, where).flatMap:
                case Some(symbols) => Right(
                    Walked(maybe(Shape.Named(owner :+ hint), nullable(schema)), List(Decl.Symbols(owner :+ hint, doc(schema), symbols)), Set.empty)
                  )
                case None => typed(schema, where, owner, hint, inlining)
          }

  private def doc(schema: JsonObject): Option[String] = schema("description").flatMap(_.asString)

  private def refused(schema: JsonObject, where: String): Either[String, Unit] =
    val combining = List("allOf", "if", "then", "else", "dependentSchemas", "patternProperties", "prefixItems", "$dynamicRef", "$recursiveRef")
    combining.find(schema.contains) match
      case Some(keyword) => Left(s"$where has $keyword, which the generator does not read")
      case None          =>
        val dependencies = schema("dependencies").flatMap(_.asObject).exists(_.values.exists(_.isObject))
        Either.cond(!dependencies, (), s"$where has dependencies of schemas, which the generator does not read")

  private def reference(schema: JsonObject, where: String, inlining: Set[String]): Either[String, Walked] =
    val shaping = List("type", "properties", "items", "oneOf", "anyOf", "enum", "const", "additionalProperties")
    for
      target <- schema("$ref").flatMap(_.asString).toRight(s"$where has a $$ref that is not text")
      _      <- shaping.find(schema.contains).map(keyword => s"$where has $keyword beside its $$ref, which the generator does not read").toLeft(())
      walked <-
        if target == "#" then Right(Walked(Shape.Named(rootPath), Nil, Set.empty))
        else
          definitions.get(target) match
            case None => Left(s"$where refers to $target, which is not a definition of the same file; the generator reads no other references yet")
            case Some((key, node)) => Names.pascal(key).toRight(s"$target makes no Scala name").flatMap: name =>
                node.asObject.filter(namesType) match
                  case Some(defined)                     => Right(Walked(maybe(Shape.Named(rootPath :+ name), nullable(defined)), Nil, Set(target)))
                  case None if inlining.contains(target) => Left(s"$where refers to $target, which holds itself without a type of its own")
                  case None                              => walk(node, target, rootPath, name, inlining + target)
    yield walked

  /** Whether a definition is declared as a type of its own: an object or an enum of text. */
  private def namesType(schema: JsonObject): Boolean =
    !schema.contains("$ref") && !schema.contains("oneOf") && !schema.contains("anyOf") &&
      (enumeration(schema, "").toOption.flatten.isDefined ||
        types(schema).toOption.exists(_.filterNot(_ == "null") == List("object")) && !mapLike(schema))

  private def nullable(schema: JsonObject): Boolean =
    types(schema).toOption.exists(_.contains("null")) || schema("enum").flatMap(_.asArray).exists(_.contains(Json.Null))

  /** The values of an enum of text, by their Scala names, or none where `schema` is not one. */
  private def enumeration(schema: JsonObject, where: String): Either[String, Option[List[(String, String)]]] =
    schema("enum").flatMap(_.asArray).map(_.toList.filterNot(_.isNull)) match
      case None                                                         => Right(None)
      case Some(values) if values.nonEmpty && values.forall(_.isString) =>
        val texts = values.flatMap(_.asString)
        texts.traverse(text => symbolName(text).toRight(s"$where has the value '$text', which makes no Scala name").map(_ -> text)).flatMap: named =>
          named.groupBy(_._1).collectFirst { case (scala, copies) if copies.size > 1 => s"$where has two values named $scala in Scala" }
            .toLeft(Some(named))
      case Some(_) if schema.contains("type") => Right(None)
      case Some(_)                            => Left(s"$where is an enum of values other than text, without a type")

  private def symbolName(text: String): Option[String] =
    (if Names.isIdentifier(text) then Some(text) else Names.pascal(text)).map: name =>
      if reserved.contains(name) then s"${name}_" else Names.symbol(name)._1

  private def mapLike(schema: JsonObject): Boolean = !schema.contains("properties") && !schema("additionalProperties").contains(Json.False)

  /** The JSON types of `schema`, inferred from its keywords where it names none, `any` where it has none of them. */
  private def types(schema: JsonObject): Either[String, List[String]] =
    schema("type") match
      case Some(one) if one.isString => Right(one.asString.toList)
      case Some(several) => several.asArray.flatMap(_.traverse(_.asString)).map(_.toList).toRight("a type that is neither text nor a list of it")
      case None          =>
        if List("properties", "additionalProperties", "required").exists(schema.contains) then Right(List("object"))
        else if schema.contains("items") then Right(List("array"))
        else Right(List("any"))

  private def typed(schema: JsonObject, where: String, owner: List[String], hint: String, inlining: Set[String]): Either[String, Walked] =
    types(schema).leftMap(reason => s"$where has $reason").flatMap: all =>
      val inner =
        all.filterNot(_ == "null") match
          case Nil        => Left(s"$where is null alone, which has no Scala type")
          case one :: Nil => single(one, schema, where, owner, hint, inlining)
          case several    => several.traverse(single(_, schema, where, owner, hint, inlining)).flatMap: walked =>
              checkedUnion(walked.map(_.shape), where)
                .map(shapes => Walked(Shape.Union(shapes, exactlyOne = false), walked.flatMap(_.decls), walked.flatMap(_.refs).toSet))
      inner.map(walked => walked.copy(shape = maybe(walked.shape, all.contains("null"))))

  private def single(
      kind: String,
      schema: JsonObject,
      where: String,
      owner: List[String],
      hint: String,
      inlining: Set[String]
  ): Either[String, Walked] =
    kind match
      case "any"     => Right(Walked.plain(Kind.Anything))
      case "boolean" => Right(Walked.plain(Kind.Bool))
      case "integer" => Right(Walked.plain(Kind.Integer))
      case "number"  => Right(Walked.plain(if bigDecimals then Kind.Decimal else Kind.Number))
      case "string"  =>
        if schema("contentEncoding").flatMap(_.asString).contains("base64") then Right(Walked.plain(Kind.Bytes))
        else
          schema("format").flatMap(_.asString) match
            case Some("date-time") => Right(Walked.plain(Kind.DateTime))
            case Some("date")      => Right(Walked.plain(Kind.Date))
            case Some("time")      => Right(Walked.plain(Kind.Time))
            case Some("uuid")      => Right(Walked.plain(Kind.Uuid))
            case _                 => Right(Walked.plain(Kind.Text))
      case "array" => schema("items") match
          case None                         => Right(Walked(Shape.Many(Shape.Plain(Kind.Anything)), Nil, Set.empty))
          case Some(items) if items.isArray => Left(s"$where has a list of items, one schema for each place, which the generator does not read")
          case Some(items)                  => walk(items, s"$where/items", owner, s"${hint}Item", inlining)
              .map(walked => walked.copy(shape = Shape.Many(walked.shape)))
      case "object" =>
        if !mapLike(schema) then record(schema, where, owner :+ hint, inlining)
        else
          schema("additionalProperties") match
            case None | Some(Json.True) => Right(Walked(Shape.Keyed(Shape.Plain(Kind.Anything)), Nil, Set.empty))
            case Some(values)           => walk(values, s"$where/additionalProperties", owner, s"${hint}Value", inlining)
                .map(walked => walked.copy(shape = Shape.Keyed(walked.shape)))
      case other => Left(s"$where has the type $other, which JSON Schema does not have")

  private def record(schema: JsonObject, where: String, path: List[String], inlining: Set[String]): Either[String, Walked] =
    for
      _ <-
        Either.cond(
          schema("additionalProperties").forall(extra => extra.isBoolean),
          (),
          s"$where has both properties and a schema for others, which the generator does not read"
        )
      properties <- schema("properties").fold(Right(Nil))(_.asObject.map(_.toList).toRight(s"$where has properties that are not an object"))
      required = schema("required").flatMap(_.asArray).toList.flatten.flatMap(_.asString).toSet
      fields <- properties.traverse((name, node) => field(name, node, required(name), s"$where/properties/$name", path, inlining))
      _      <-
        fields.map(_._1).filterNot(_.presence.isInstanceOf[Presence.Constant]).map(_.scala).groupBy(identity).collectFirst {
          case (scala, copies) if copies.size > 1 => s"$where has two properties named $scala in Scala"
        }.toLeft(())
    yield Walked(Shape.Named(path), Decl.Record(path, doc(schema), fields.map(_._1)) :: fields.flatMap(_._2.decls), fields.flatMap(_._2.refs).toSet)

  /** A property's field, and what its type declares. */
  private def field(
      name: String,
      node: Json,
      required: Boolean,
      where: String,
      owner: List[String],
      inlining: Set[String]
  ): Either[String, (Field, Walked)] =
    val schema = node.asObject.getOrElse(JsonObject.empty)
    val single = schema("enum").flatMap(_.asArray).collect { case Vector(one) if one.isString => one }
    schema("const").orElse(single) match
      case Some(value) if required && value.isString =>
        Right((Field(name, "", Shape.Plain(Kind.Text), Presence.Constant(value), None, None), Walked.plain(Kind.Text)))
      case Some(_) if schema.contains("const") => Left(s"$where is a constant that is not text, or not required, which the generator does not read")
      case _                                   =>
        for
          scala  <- (if Names.isIdentifier(name) then Some(name) else Names.camel(name)).map(Names.field(_)._1).toRight(s"$where makes no Scala name")
          hint   <- Names.pascal(name).toRight(s"$where makes no Scala name")
          walked <- walk(node, where, owner, hint, inlining)
        yield
          val default  = schema("default")
          val presence = if required then Presence.Required else if default.forall(_.isNull) then Presence.Optional else Presence.Defaulted
          (Field(name, scala, walked.shape, presence, default.filterNot(_ => presence == Presence.Optional), doc(schema)), walked)

  private def union(schema: JsonObject, where: String, owner: List[String], hint: String, inlining: Set[String]): Either[String, Walked] =
    for
      found <- unionOf(schema, where)
      (keyword, branches) = found
      walked <-
        branches.zipWithIndex.traverse: (branch, index) =>
          if isNull(branch) then Right(None)
          else walk(branch, s"$where/$keyword/$index", owner, branchName(branch, hint, index), inlining).map(Some(_))
      present = walked.flatten
      shapes  = present.map(walked => strip(walked.shape))
      shape <-
        shapes match
          case Nil        => Left(s"$where is null alone, which has no Scala type")
          case one :: Nil => Right(one)
          case several    => checkedUnion(several, where).map(Shape.Union(_, keyword == "oneOf"))
    yield
      val nulls = walked.contains(None) || present.exists(_.shape.isInstanceOf[Shape.Maybe]) || nullable(schema)
      Walked(maybe(shape, nulls), present.flatMap(_.decls), present.flatMap(_.refs).toSet)

  private def unionOf(schema: JsonObject, where: String): Either[String, (String, List[Json])] =
    val beside = List("properties", "items", "enum", "const", "additionalProperties").find(schema.contains)
    (schema("oneOf"), schema("anyOf")) match
      case (Some(_), Some(_))     => Left(s"$where has both oneOf and anyOf, which the generator does not read")
      case _ if beside.isDefined  => Left(s"$where has ${beside.mkString} beside a oneOf or anyOf, which the generator does not read")
      case (Some(branches), None) => branches.asArray.map(_.toList).map("oneOf" -> _).toRight(s"$where has a oneOf that is not a list")
      case (None, Some(branches)) => branches.asArray.map(_.toList).map("anyOf" -> _).toRight(s"$where has an anyOf that is not a list")
      case (None, None)           => Left(s"$where is not a union")

  private def isNull(branch: Json): Boolean =
    branch.asObject.exists(schema => schema("type").contains(Json.fromString("null")) || schema("const").contains(Json.Null))

  /** A union's branch's name, its title where that is a Scala name, and its place where not. */
  private def branchName(branch: Json, hint: String, index: Int): String =
    branch.hcursor.get[String]("title").toOption.filter(Names.isIdentifier).getOrElse(s"$hint${index + 1}")

  /** The branches of a union, where each is told from the others by the class of its values, as a value of a union is written. */
  private def checkedUnion(shapes: List[Shape], where: String): Either[String, List[Shape]] =
    def erased(shape: Shape): Either[String, String] =
      shape match
        case Shape.Plain(Kind.Anything) => Left(s"$where has a branch of any value, which no other branch can be told from")
        case Shape.Plain(kind)          => Right(kind.toString)
        case Shape.Named(path)          => Right(path.mkString("."))
        case Shape.Many(_)              => Right("List")
        case Shape.Keyed(_)             => Right("Map")
        case Shape.Maybe(_)             => Left(s"$where has null inside a branch")
        case Shape.Union(_, _)          => Left(s"$where has a union inside a union, which the generator does not read")
    shapes.traverse(erased).flatMap: classes =>
      classes.groupBy(identity).collectFirst {
        case (_, copies) if copies.size > 1 => s"$where has two branches of the same Scala class, which a value written could not be told to be of"
      }.toLeft(shapes)

  private def maybe(shape: Shape, nulls: Boolean): Shape =
    shape match
      case _: Shape.Maybe => shape
      case _              => if nulls then Shape.Maybe(shape) else shape

  /** The source of the file, from all its declarations, where the cases of a sealed trait extend the trait `parents` names. */
  private final class Renderer(decls: List[Decl], parents: Map[List[String], String]):
    private val byPath   = decls.map(decl => decl.path -> decl).toMap
    private val names    = neighbours ++ decls.map(_.path.last) ++ decls.collect { case Decl.Symbols(_, _, symbols) => symbols.map(_._1) }.flatten
    private val access   = s"private[${Names.term(root.packageName.split('.').last)}]"
    private val codecs   = "_root_.xkafka.jsonschema.JsonSchemaCodec"
    private val helpers  = "_root_.xkafka.jsonschema.Generated"
    private val jsonType = "_root_.io.circe.Json"

    private def builtin(name: String): String = Names.builtin(name, names)

    private def result(scala: String): String = s"${builtin("Either")}[${builtin("String")}, $scala]"

    def file: Either[String, String] =
      declaration(byPath(rootPath)).map: text =>
        s"""// Generated by xkafka-codegen from the JSON Schema ${root.file.getFileName}. Changes made here are lost when it is generated again.
           |
           |package ${root.packageName.split('.').map(Names.term).mkString(".")}
           |
           |$text""".stripMargin

    def scala(shape: Shape): String =
      shape match
        case Shape.Plain(kind)        => plain(kind)
        case Shape.Named(path)        => scalaPath(path)
        case Shape.Many(items)        => s"${builtin("List")}[${scala(items)}]"
        case Shape.Keyed(values)      => s"${builtin("Map")}[${builtin("String")}, ${scala(values)}]"
        case Shape.Maybe(inner)       => s"${builtin("Option")}[${scala(inner)}]"
        case Shape.Union(branches, _) => branches.map(scala).mkString("(", " | ", ")")

    private def plain(kind: Kind): String =
      kind match
        case Kind.Anything => jsonType
        case Kind.Bool     => builtin("Boolean")
        case Kind.Integer  => builtin("Long")
        case Kind.Number   => builtin("Double")
        case Kind.Decimal  => builtin("BigDecimal")
        case Kind.Text     => builtin("String")
        case Kind.Uuid     => "_root_.java.util.UUID"
        case Kind.DateTime => "_root_.java.time.Instant"
        case Kind.Date     => "_root_.java.time.LocalDate"
        case Kind.Time     => "_root_.java.time.OffsetTime"
        case Kind.Bytes    => s"_root_.fs2.Chunk[${builtin("Byte")}]"

    private def label(path: List[String]): String = Names.quote(fullName(path))

    private def declaration(decl: Decl): Either[String, String] =
      decl match
        case record: Decl.Record   => recordSource(record)
        case symbols: Decl.Symbols => Right(symbolsSource(symbols))
        case choice: Decl.Choice   => Right(choiceSource(choice))

    private def nested(path: List[String]): Either[String, List[String]] =
      decls.filter(decl => decl.path.init == path && decl.path.size == path.size + 1).sortBy(_.path.last).traverse(declaration)

    private def companion(path: List[String], members: List[String]): Either[String, String] =
      nested(path).map: inner =>
        val schema =
          Option.when(path == rootPath)(s"/** The JSON Schema ${root
              .name} was generated from, which its codec registers. */\nval jsonSchema: $jsonType =\n${indent(schemaValue(root.schema))}")
        val name  = Names.typeName(path.last)
        val codec =
          val decode = if byPath.get(path).exists(_.isInstanceOf[Decl.Record]) then "decodeJson(_, false)" else "decodeJson"
          if path == rootPath then s"given $codecs[$name] = $helpers.codec(jsonSchema, encodeJson, $decode)"
          else s"$access given $codecs[$name] = $helpers.part(encodeJson, $decode)"
        s"object $name${Names.opening(name)}\n" + (schema.toList ++ inner ++ members :+ codec).map(indent).mkString("\n\n") + "\n"

    private def recordSource(record: Decl.Record): Either[String, String] =
      val name   = Names.typeName(record.path.last)
      val fields = record.fields.filterNot(_.presence.isInstanceOf[Presence.Constant])
      for
        parameters <- fields.traverse(parameter)
        reads      <- record.fields.zipWithIndex.traverse(read)
        body =
          val shown  = if parameters.isEmpty then "()" else parameters.mkString("(\n    ", ",\n    ", "\n)")
          val extend = parents.get(record.path).fold("")(parent => s" extends $parent")
          val docs   = scaladoc(record.doc, fields.flatMap(field => field.doc.map(field.scala -> _)))
          s"${docs}final case class $name$shown$extend\n\n"
        writes = record.fields.map(write)
        known  = record.fields.map(field => Names.quote(field.json)).mkString(", ")
        made   = record.fields.zipWithIndex.collect { case (field, index) if !field.presence.isInstanceOf[Presence.Constant] => s"value${index + 1}" }
        decoding =
          if reads.flatten.isEmpty then s"    .map(_ => $name(${made.mkString(", ")}))"
          else
            s"    .flatMap: jsonFields =>\n      for\n${reads.flatten.map("        " + _).mkString("\n")}\n      yield ${scalaPath(
                record.path
              )}(${made.mkString(", ")})"
        needed <- givens(record.path, fields.map(_.shape))
        members =
          needed ++ List(
            s"$access def encodeJson(value: ${scalaPath(record.path)}): ${result(jsonType)} =\n  $helpers.write(${writes.map("\n    " + _)
                .mkString(",")}\n  )",
            s"$access def decodeJson(json: $jsonType, strict: ${builtin("Boolean")}): ${result(
                scalaPath(record.path)
              )} =\n  $helpers.read(json, ${label(record.path)}, strict, ${builtin("Set")}($known))\n$decoding"
          )
        text <- companion(record.path, members)
      yield body + text

    private def parameter(field: Field): Either[String, String] =
      val default =
        field.presence match
          case Presence.Optional => Right(Some(builtin("None")))
          case _                 => field.default
              .traverse(literal(field.shape, _).leftMap(reason => s"${field.json} has a default the generator cannot write: $reason"))
      default.map(value => s"${field.scala}${Names.colon(field.scala)}${fieldType(field)}${value.fold("")(" = " + _)}")

    private def fieldType(field: Field): String =
      field.presence match
        case Presence.Optional => scala(Shape.Maybe(strip(field.shape)))
        case _                 => scala(field.shape)

    private def write(field: Field): String =
      val name = Names.quote(field.json)
      field.presence match
        case Presence.Constant(value) => s"$helpers.constant($name, ${json(value)})"
        case Presence.Optional        => s"$helpers.optional[${scala(strip(field.shape))}]($name, value.${field.scala})"
        case _                        => s"$helpers.property[${scala(field.shape)}]($name, value.${field.scala})"

    private def read(field: Field, index: Int): Either[String, Option[String]] =
      val name = Names.quote(field.json)
      field.presence match
        case Presence.Constant(value) => Right(Some(s"_ <- $helpers.readConstant(jsonFields, $name, ${json(value)})"))
        case Presence.Optional        => Right(Some(s"value${index + 1} <- $helpers.readOptional[${scala(strip(field.shape))}](jsonFields, $name)"))
        case Presence.Required        => Right(Some(s"value${index + 1} <- $helpers.readRequired[${scala(field.shape)}](jsonFields, $name)"))
        case Presence.Defaulted       => field.default.toRight(s"${field.json} has no default").flatMap(literal(field.shape, _))
            .leftMap(reason => s"${field.json} has a default the generator cannot write: $reason")
            .map(value => Some(s"value${index + 1} <- $helpers.readDefaulted[${scala(field.shape)}](jsonFields, $name, $value)"))

    /** The codecs that the fields of `shapes` need given in their companion: of union types, and of types read otherwise than xkafka's own. */
    private def givens(path: List[String], shapes: List[Shape]): Either[String, List[String]] =
      def inside(shape: Shape): List[Shape] =
        shape match
          case Shape.Many(inner)        => shape :: inside(inner)
          case Shape.Keyed(inner)       => shape :: inside(inner)
          case Shape.Maybe(inner)       => shape :: inside(inner)
          case Shape.Union(branches, _) => shape :: branches.flatMap(inside)
          case other                    => List(other)
      val all     = shapes.flatMap(inside).distinct
      val special =
        all.collect {
          case Shape.Plain(Kind.DateTime) => s"private given $codecs[_root_.java.time.Instant] = $helpers.dateTime"
          case Shape.Plain(Kind.Time)     => s"private given $codecs[_root_.java.time.OffsetTime] = $helpers.time"
          case Shape.Plain(Kind.Anything) => s"private given $codecs[$jsonType] = $helpers.anything"
        }
      val unions = all.collect { case union: Shape.Union => union }.distinct
      unions.groupBy(scala).collectFirst {
        case (both, kinds) if kinds.size > 1 => s"${fullName(path)} has $both both as a oneOf and as an anyOf, which one type cannot hold"
      }.toLeft(special ++ unions.zipWithIndex.map((union, index) => unionGiven(union, s"jsonUnion${index + 1}")))

    private def unionGiven(union: Shape.Union, name: String): String =
      val all      = scala(union)
      val writes   = union.branches.map(branch => s"\n      case value: ${pattern(branch)} => summon[$codecs[${scala(branch)}]].encode(value)")
      val branches =
        union.branches.map: branch =>
          val reading =
            branch match
              case Shape.Named(path) if byPath.get(path).exists(_.isInstanceOf[Decl.Record]) =>
                s"(json: $jsonType, strict: ${builtin("Boolean")}) => ${scalaPath(path)}.decodeJson(json, strict)"
              case Shape.Named(path) => s"(json: $jsonType, _: ${builtin("Boolean")}) => ${scalaPath(path)}.decodeJson(json)"
              case other             => s"(json: $jsonType, _: ${builtin("Boolean")}) => summon[$codecs[${scala(other)}]].decode(json)"
          s"\n      (${Names.quote(scala(branch))}, $reading)"
      s"private given $name: $codecs[$all] =\n  $helpers.part[$all](\n    {${writes.mkString}\n    },\n    $helpers.union[$all](${Names
          .quote(all)}, ${union.exactlyOne}, ${builtin("List")}(${branches.mkString(",")}\n    ))\n  )"

    /** The type a value of a union's branch is matched by, where its type arguments cannot be checked. */
    private def pattern(shape: Shape): String =
      shape match
        case Shape.Plain(Kind.Bytes) => s"_root_.fs2.Chunk[${builtin("Byte")} @unchecked]"
        case Shape.Many(items)       => s"${builtin("List")}[${scala(items)} @unchecked]"
        case Shape.Keyed(values)     => s"${builtin("Map")}[${builtin("String")} @unchecked, ${scala(values)} @unchecked]"
        case other                   => scala(other)

    private def symbolsSource(symbols: Decl.Symbols): String =
      val name    = Names.typeName(symbols.path.last)
      val full    = scalaPath(symbols.path)
      val cases   = symbols.symbols.map((scala, _) => s"  case $scala").mkString("\n")
      val pairs   = symbols.symbols.map((scala, text) => s"$full.$scala -> ${Names.quote(text)}").mkString(", ")
      val members =
        List(
          s"private val jsonSymbols = $helpers.symbols[$full](${label(symbols.path)}, ${builtin("List")}($pairs))",
          s"$access def encodeJson(value: $full): ${result(jsonType)} = jsonSymbols._1(value)",
          s"$access def decodeJson(json: $jsonType): ${result(full)} = jsonSymbols._2(json)"
        )
      s"${scaladoc(symbols.doc, Nil)}enum $name${Names.opening(name)}\n$cases\n\n" + companion(symbols.path, members).fold(identity, identity)

    private def choiceSource(choice: Decl.Choice): String =
      val name     = Names.typeName(choice.path.last)
      val full     = scalaPath(choice.path)
      val branches =
        choice.branches.map(path =>
          s"\n    (${Names.quote(scalaPath(path))}, (json: $jsonType, strict: ${builtin("Boolean")}) => ${scalaPath(path)}.decodeJson(json, strict))"
        )
      val members =
        List(
          s"private val jsonReader = $helpers.union[$full](${label(choice.path)}, ${choice.exactlyOne}, ${builtin("List")}(${branches
              .mkString(",")}\n  ))",
          s"$access def encodeJson(value: $full): ${result(jsonType)} =\n  value match" +
            choice.branches.map(path => s"\n    case value: ${scalaPath(path)} => ${scalaPath(path)}.encodeJson(value)").mkString,
          s"$access def decodeJson(json: $jsonType): ${result(full)} = jsonReader(json)"
        )
      s"${scaladoc(choice.doc, Nil)}sealed trait $name\n\n" + companion(choice.path, members).fold(identity, identity)

    /** The Scala expression of `value`, a value of `shape`. */
    private def literal(shape: Shape, value: Json): Either[String, String] =
      def text = value.asString.toRight(s"${value.noSpaces} is not text")
      shape match
        case Shape.Maybe(_) if value.isNull => Right(builtin("None"))
        case Shape.Maybe(inner)             => literal(inner, value).map(inner => s"${builtin("Some")}($inner)")
        case Shape.Many(items)              => value.asArray.toRight(s"${value.noSpaces} is not a list").flatMap(_.toList.traverse(literal(items, _)))
            .map(_.mkString(s"${builtin("List")}(", ", ", ")"))
        case Shape.Keyed(values) => value.asObject.toRight(s"${value.noSpaces} is not an object")
            .flatMap(_.toList.traverse((key, item) => literal(values, item).map(item => s"${Names.quote(key)} -> $item")))
            .map(_.mkString(s"${builtin("Map")}(", ", ", ")"))
        case Shape.Union(branches, _) => branches.collectFirstSome(literal(_, value).toOption)
            .toRight(s"${value.noSpaces} is of none of the union's branches")
        case Shape.Named(path) => byPath.get(path) match
            case Some(Decl.Symbols(_, _, symbols)) => text.flatMap(found =>
                symbols.collectFirst { case (scala, `found`) => s"${scalaPath(path)}.$scala" }.toRight(s"'$found' is not a value of the enum")
              )
            case Some(Decl.Record(_, _, fields)) => value.asObject.toRight(s"${value.noSpaces} is not an object").flatMap: written =>
                fields.filterNot(_.presence.isInstanceOf[Presence.Constant]).traverse: field =>
                  (written(field.json), field.presence) match
                    case (Some(item), Presence.Optional) => literal(Shape.Maybe(strip(field.shape)), item)
                        .map(item => Some(s"${field.scala} = $item"))
                    case (Some(item), _)                                    => literal(field.shape, item).map(item => Some(s"${field.scala} = $item"))
                    case (None, Presence.Required) if field.default.isEmpty => Left(s"${value.noSpaces} has no property '${field.json}'")
                    case (None, _)                                          => Right(None)
                .map(_.flatten.mkString(s"${scalaPath(path)}(", ", ", ")"))
            case Some(Decl.Choice(_, _, branches, _)) => branches.collectFirstSome(branch => literal(Shape.Named(branch), value).toOption)
                .toRight(s"${value.noSpaces} is of none of the cases")
            case None => Left(s"${scalaPath(path)} is not declared")
        case Shape.Plain(kind) => kind match
            case Kind.Anything => Right(json(value))
            case Kind.Bool     => value.asBoolean.map(_.toString).toRight(s"${value.noSpaces} is not a boolean")
            case Kind.Integer  => value.asNumber.flatMap(_.toLong).map(long => s"${long}L").toRight(s"${value.noSpaces} is not an integer")
            case Kind.Number   => value.asNumber.map(_.toDouble).filter(_.isFinite).map(_.toString).toRight(s"${value.noSpaces} is not a number")
            case Kind.Decimal  => value.asNumber.map(number => s"${builtin("BigDecimal")}(${Names.quote(number.toString)})")
                .toRight(s"${value.noSpaces} is not a number")
            case Kind.Text => text.map(Names.quote)
            case Kind.Uuid => text.flatMap(found => Try(UUID.fromString(found)).toEither.leftMap(_ => s"'$found' is not a UUID"))
                .map(uuid => s"_root_.java.util.UUID.fromString(${Names.quote(uuid.toString)})")
            case Kind.DateTime => text
                .flatMap(found => Try(OffsetDateTime.parse(found).toInstant).toEither.leftMap(_ => s"'$found' is not a date-time"))
                .map(instant => s"_root_.java.time.Instant.parse(${Names.quote(instant.toString)})")
            case Kind.Date => text.flatMap(found => Try(LocalDate.parse(found)).toEither.leftMap(_ => s"'$found' is not a date"))
                .map(date => s"_root_.java.time.LocalDate.parse(${Names.quote(date.toString)})")
            case Kind.Time => text.flatMap(found => Try(OffsetTime.parse(found)).toEither.leftMap(_ => s"'$found' is not a time with an offset"))
                .map(time => s"_root_.java.time.OffsetTime.parse(${Names.quote(time.toString)})")
            case Kind.Bytes => text.flatMap(found => Try(Base64.getDecoder.decode(found)).toEither.leftMap(_ => s"'$found' is not base64"))
                .map(bytes => bytes.mkString(s"_root_.fs2.Chunk[${builtin("Byte")}](", ", ", ")"))

private[codegen] object JsonScala:
  /** What a JSON Schema's value is read into, where it is not a type of the file's own. */
  enum Kind:
    case Anything, Bool, Integer, Number, Decimal, Text, Uuid, DateTime, Date, Time, Bytes

  /** A value's Scala type. */
  enum Shape:
    case Plain(kind: Kind)
    case Named(path: List[String])
    case Many(items: Shape)
    case Keyed(values: Shape)
    case Maybe(inner: Shape)
    case Union(branches: List[Shape], exactlyOne: Boolean)

  /** A type the file declares: a case class, an enum or a sealed trait, at `path` from the root's type. */
  enum Decl:
    def path: List[String]

    case Record(path: List[String], doc: Option[String], fields: List[Field])
    case Symbols(path: List[String], doc: Option[String], symbols: List[(String, String)])
    case Choice(path: List[String], doc: Option[String], branches: List[List[String]], exactlyOne: Boolean)

  /** How a property is written and read: always there; left out where it has no value; taking its default where left out; or always one value. */
  enum Presence:
    case Required, Optional, Defaulted
    case Constant(value: Json)

  /** A property, named `json` in JSON and `scala` in Scala. */
  final case class Field(json: String, scala: String, shape: Shape, presence: Presence, default: Option[Json], doc: Option[String])

  /** A value's type, with the declarations it needs and the definitions it refers to. */
  final case class Walked(shape: Shape, decls: List[Decl], refs: Set[String])

  object Walked:
    def plain(kind: Kind): Walked = Walked(Shape.Plain(kind), Nil, Set.empty)

  /** The members of a generated companion, which an enum's value would clash with. */
  private val reserved = Set("jsonSchema", "jsonSymbols", "encodeJson", "decodeJson")

  def scalaPath(path: List[String]): String = path.map(Names.typeName).mkString(".")

  def strip(shape: Shape): Shape =
    shape match
      case Shape.Maybe(inner) => inner
      case other              => other

  def indent(text: String): String = text.linesIterator.map(line => if line.isEmpty then line else s"  $line").mkString("\n")

  def scaladoc(doc: Option[String], params: List[(String, String)]): String =
    // Scala's comments nest, so text must neither end the comment nor open another inside it.
    def safe(text: String) = Names.paired(text).replace("*/", "*&#47;").replace("/*", "&#47;*")
    val lines              =
      doc.toList.flatMap(text => safe(text).linesIterator.toList) ++ (if params.nonEmpty && doc.nonEmpty then List("") else Nil) ++
        params.map((name, text) => s"@param $name ${safe(text).linesIterator.mkString(" ")}")
    lines match
      case Nil => ""
      case _   => lines.mkString("/** ", "\n  * ", "\n  */\n").replace(" \n", "\n")

  /** The schema as an expression: built value by value where it is small, and read from its text where building it would take more code than one
    * method of the JVM holds.
    */
  private def schemaValue(schema: Json): String =
    val text = schema.noSpaces
    if text.length <= LargeSchema then json(schema) else s"_root_.xkafka.jsonschema.Generated.parsed(${Names.text(text)})"

  /** The length of schema text past which the schema is read from its text. */
  private val LargeSchema = 8192

  /** The Scala expression of a JSON value, as it is. */
  def json(value: Json): String =
    def listed(open: String, items: List[String]) = if items.isEmpty then s"$open()" else s"$open(\n${indent(items.mkString(",\n"))}\n)"
    value.fold(
      "_root_.io.circe.Json.Null",
      boolean => if boolean then "_root_.io.circe.Json.True" else "_root_.io.circe.Json.False",
      number => s"_root_.io.circe.Json.fromJsonNumber(_root_.io.circe.JsonNumber.fromDecimalStringUnsafe(${Names.quote(number.toString)}))",
      text => s"_root_.io.circe.Json.fromString(${Names.quote(text)})",
      items => listed("_root_.io.circe.Json.arr", items.toList.map(json)),
      fields => listed("_root_.io.circe.Json.obj", fields.toList.map((key, item) => s"${Names.quote(key)} -> ${json(item)}"))
    )
