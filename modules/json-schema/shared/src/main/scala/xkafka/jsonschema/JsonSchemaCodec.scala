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

import java.time.{Instant, LocalDate, LocalDateTime, LocalTime}
import java.util.{Base64, UUID}

import scala.compiletime.summonInline
import scala.deriving.Mirror
import scala.reflect.TypeTest
import scala.util.Try

import cats.syntax.all.*
import fs2.Chunk
import io.circe.Json
import xkafka.schemaregistry.{DefaultOf, Meta}

/** A schema as it is described inside another: its JSON, and the types it names by reference, by name, with how to describe each. */
private[jsonschema] final case class Described(json: Json, definitions: Map[String, () => Described])

/** How values of `A` are written as JSON, and read back, with the JSON Schema that describes them.
  *
  * Codecs are given for Scala's primitive types, text, bytes, options, collections, UUIDs and `java.time` types, and derived for case classes, sealed
  * traits and enums with `derives JsonSchemaCodec`. Other types are made from these with [[imap]] and [[emap]], or with [[JsonSchemaCodec.from]] from
  * a schema and circe's codecs.
  */
trait JsonSchemaCodec[A]:
  self =>

  /** The schema, where a type already described in `seen`, the types it is inside, is a reference to its definition. */
  private[jsonschema] def describe(seen: Set[String]): Described

  /** Whether a field of this type may be left out, as an optional one may. */
  private[jsonschema] def optional: Boolean = false

  /** The discriminator values of a sealed trait's cases, which a sealed trait it is a case of takes as its own. */
  private[jsonschema] def tags: Option[Set[String]] = None

  /** The full name of the type, which naming subjects by record takes, where it is a case class, sealed trait or enum. */
  def name: Option[String] = None

  /** The schema, as a JSON Schema document of draft 7, the draft Confluent's serializers use by default. */
  final def schema: Json = JsonSchemaCodec.document(describe(Set.empty))

  def encode(value: A): Either[String, Json]

  def decode(json: Json): Either[String, A]

  /** A codec for `B`, written as the `A` that `to` makes of it and read as `from` makes of an `A`. */
  final def imap[B](from: A => B)(to: B => A): JsonSchemaCodec[B] = emap(value => Right(from(value)))(to)

  /** A codec for `B`, written as the `A` that `to` makes of it and read as `from` makes of an `A`, which may refuse it. */
  final def emap[B](from: A => Either[String, B])(to: B => A): JsonSchemaCodec[B] =
    new JsonSchemaCodec[B]:
      override private[jsonschema] def describe(seen: Set[String]): Described = self.describe(seen)
      override private[jsonschema] def optional: Boolean                      = self.optional
      override def name: Option[String]                                       = self.name
      override def encode(value: B): Either[String, Json]                     = self.encode(to(value))
      override def decode(json: Json): Either[String, B]                      = self.decode(json).flatMap(from)

object JsonSchemaCodec:
  def apply[A](using codec: JsonSchemaCodec[A]): JsonSchemaCodec[A] = codec

  /** A codec of `schema`, writing with `encode` and reading with `decode`. */
  def instance[A](described: Json, encode: A => Either[String, Json], decode: Json => Either[String, A]): JsonSchemaCodec[A] =
    val (write, read) = (encode, decode)
    new JsonSchemaCodec[A]:
      override private[jsonschema] def describe(seen: Set[String]): Described = Described(described, Map.empty)
      override def encode(value: A): Either[String, Json]                     = write(value)
      override def decode(json: Json): Either[String, A]                      = read(json)

  /** A codec of a schema written by hand, with circe's codecs for its values, which must write what the schema describes. */
  def from[A](schema: Json, encoder: io.circe.Encoder[A], decoder: io.circe.Decoder[A]): JsonSchemaCodec[A] =
    instance(schema, value => Right(encoder(value)), json => decoder.decodeJson(json).leftMap(_.getMessage))

  /** JSON values of `schema` as they are, for schemas known only as the program runs; its `title` names it, as Confluent's tools name a schema. */
  def generic(described: Json): JsonSchemaCodec[Json] =
    new JsonSchemaCodec[Json]:
      override private[jsonschema] def describe(seen: Set[String]): Described = Described(described, Map.empty)
      override def name: Option[String]                                       = described.hcursor.get[String]("title").toOption
      override def encode(value: Json): Either[String, Json]                  = Right(value)
      override def decode(json: Json): Either[String, Json]                   = Right(json)

  private val draft7 = "http://json-schema.org/draft-07/schema#"

  /** The schema `root` describes, with the definitions of the types it names by reference. */
  private def document(root: Described): Json =
    def close(pending: List[(String, () => Described)], done: Map[String, Json]): Map[String, Json] =
      pending match
        case Nil                                      => done
        case (name, _) :: rest if done.contains(name) => close(rest, done)
        case (name, describe) :: rest                 =>
          val described = describe()
          close(rest ++ described.definitions.toList, done.updated(name, described.json))
    val definitions = close(root.definitions.toList, Map.empty)
    val fields      = root.json.asObject.fold(List("allOf" -> Json.arr(root.json)))(_.toList)
    // A schema written by hand keeps the draft it names.
    val draft = fields.collectFirst { case ("$schema", named) => named }.getOrElse(Json.fromString(draft7))
    Json.fromFields(
      ("$schema" -> draft) ::
        fields.filterNot(_._1 == "$schema") ++ Option.when(definitions.nonEmpty)("definitions" -> Json.fromFields(definitions.toList.sortBy(_._1)))
    )

  private def typed(name: String): Json = Json.obj("type" -> Json.fromString(name))

  private def primitive[A](schema: Json)(write: A => Json)(read: Json => Option[A]): JsonSchemaCodec[A] =
    instance(schema, value => Right(write(value)), json => read(json).toRight(s"expected a value of ${schema.noSpaces}, found ${json.noSpaces}"))

  given JsonSchemaCodec[Boolean] = primitive(typed("boolean"))(Json.fromBoolean)(_.asBoolean)
  given JsonSchemaCodec[Int]     = primitive(typed("integer"))(Json.fromInt)(_.asNumber.flatMap(_.toInt))
  given JsonSchemaCodec[Long]    = primitive(typed("integer"))(Json.fromLong)(_.asNumber.flatMap(_.toLong))

  /** A number that JSON can hold: not infinite or NaN, which are refused. */
  given JsonSchemaCodec[Double] =
    instance(
      typed("number"),
      value => Json.fromDouble(value).toRight(s"$value is not a number JSON can hold"),
      _.asNumber.map(_.toDouble).toRight("expected a number")
    )
  given JsonSchemaCodec[Float] =
    instance(
      typed("number"),
      value => Json.fromFloat(value).toRight(s"$value is not a number JSON can hold"),
      _.asNumber.map(_.toFloat).toRight("expected a number")
    )
  given JsonSchemaCodec[BigDecimal] = primitive(typed("number"))(Json.fromBigDecimal)(_.asNumber.flatMap(_.toBigDecimal))
  given JsonSchemaCodec[String]     = primitive(typed("string"))(Json.fromString)(_.asString)

  /** Bytes as base64 text. */
  given JsonSchemaCodec[Chunk[Byte]] =
    JsonSchemaCodec[String].emap(text => Try(Chunk.array(Base64.getDecoder.decode(text))).toEither.leftMap(_ => s"'$text' is not base64"))(bytes =>
      Base64.getEncoder.encodeToString(bytes.toArray)
    ).withSchema(Json.obj("type" -> Json.fromString("string"), "contentEncoding" -> Json.fromString("base64")))

  extension [A](codec: JsonSchemaCodec[A])
    /** The same codec, described by `schema`. */
    private def withSchema(schema: Json): JsonSchemaCodec[A] = instance(schema, codec.encode, codec.decode)

  /** Text in ISO-8601 form, with JSON Schema's `format` where it names one. */
  private def text[A](format: Option[String])(parse: String => A)(write: A => String): JsonSchemaCodec[A] =
    JsonSchemaCodec[String].emap(value => Try(parse(value)).toEither.leftMap(_ => s"'$value' is not in its ISO-8601 form"))(write)
      .withSchema(Json.fromFields(List("type" -> Json.fromString("string")) ++ format.map("format" -> Json.fromString(_))))

  given JsonSchemaCodec[UUID]      = text(Some("uuid"))(UUID.fromString)(_.toString)
  given JsonSchemaCodec[Instant]   = text(Some("date-time"))(Instant.parse)(_.toString)
  given JsonSchemaCodec[LocalDate] = text(Some("date"))(LocalDate.parse)(_.toString)

  /** A time of day without an offset, which JSON Schema's `time` format requires, so the schema names no format. */
  given JsonSchemaCodec[LocalTime] = text(None)(LocalTime.parse)(_.toString)

  /** A date and time without an offset, which JSON Schema's `date-time` format requires, so the schema names no format. */
  given JsonSchemaCodec[LocalDateTime] = text(None)(LocalDateTime.parse)(_.toString)

  /** A value that may be absent, as `oneOf` null and the value's schema, as Confluent's serializers describe one; a field of it may be left out. */
  given [A](using inner: JsonSchemaCodec[A]): JsonSchemaCodec[Option[A]] with
    override private[jsonschema] def describe(seen: Set[String]): Described =
      val described = inner.describe(seen)
      described.copy(json = Json.obj("oneOf" -> Json.arr(typed("null"), described.json)))
    override private[jsonschema] def optional: Boolean          = true
    override def encode(value: Option[A]): Either[String, Json] = value.fold(Right(Json.Null))(inner.encode)
    override def decode(json: Json): Either[String, Option[A]]  = if json.isNull then Right(None) else inner.decode(json).map(Some(_))

  private def array[A, C](inner: JsonSchemaCodec[A])(items: C => Iterable[A], make: Vector[A] => C): JsonSchemaCodec[C] =
    new JsonSchemaCodec[C]:
      override private[jsonschema] def describe(seen: Set[String]): Described =
        val described = inner.describe(seen)
        described.copy(json = Json.obj("type" -> Json.fromString("array"), "items" -> described.json))
      override def encode(value: C): Either[String, Json] = items(value).toVector.traverse(inner.encode).map(Json.fromValues)
      override def decode(json: Json): Either[String, C]  =
        json.asArray.toRight(s"expected an array, found ${json.noSpaces}").flatMap(_.traverse(inner.decode)).map(make)

  given [A](using inner: JsonSchemaCodec[A]): JsonSchemaCodec[List[A]]   = array(inner)(identity, _.toList)
  given [A](using inner: JsonSchemaCodec[A]): JsonSchemaCodec[Vector[A]] = array(inner)(identity, identity)
  given [A](using inner: JsonSchemaCodec[A]): JsonSchemaCodec[Seq[A]]    = array(inner)(identity, identity)
  given [A](using inner: JsonSchemaCodec[A]): JsonSchemaCodec[Set[A]]    = array(inner)(identity, _.toSet)

  /** Values by text keys, as an object whose every property is one. */
  given [A](using inner: JsonSchemaCodec[A]): JsonSchemaCodec[Map[String, A]] with
    override private[jsonschema] def describe(seen: Set[String]): Described =
      val described = inner.describe(seen)
      described.copy(json = Json.obj("type" -> Json.fromString("object"), "additionalProperties" -> described.json))
    override def encode(value: Map[String, A]): Either[String, Json] =
      value.toList.traverse((key, item) => inner.encode(item).map(key -> _)).map(Json.fromFields)
    override def decode(json: Json): Either[String, Map[String, A]] =
      json.asObject.toRight(s"expected an object, found ${json.noSpaces}").flatMap(_.toList.traverse((key, item) => inner.decode(item).map(key -> _)))
        .map(_.toMap)

  /** A field's default as its codec writes it, which derived schemas take. */
  given [A](using resolved: Resolved[A]): DefaultOf[JsonSchemaCodec, Json, A] = value => resolved.codec.encode(value)

  /** A codec for a case class, as an object of its fields, closed to others, as Confluent's compatibility checks need for fields to be added; or for
    * a sealed trait or enum: as text, one of its cases' names, where every case is an object, and as `oneOf` its cases' objects where not, each
    * telling its case by its property `type`. Names come from the Scala names and packages, and the annotations of `xkafka.schemaregistry` change
    * them; a field's Scala default is its default in the schema, and the value read where a record lacks it.
    */
  inline def derived[A](using mirror: Mirror.Of[A]): JsonSchemaCodec[A] =
    inline mirror match
      case product: Mirror.ProductOf[A] =>
        val named = Meta.named[A]
        ObjectCodec[A, product.MirroredElemTypes](
          named,
          Meta.fields[A],
          summonInline[Fields[product.MirroredElemTypes]],
          Meta.defaults[A, JsonSchemaCodec, Json],
          summonInline[Elements[A, product.MirroredElemTypes]].apply,
          product.fromTuple
        )
      case sum: Mirror.SumOf[A] => SumCodec[A](Meta.named[A], summonInline[Cases[A, sum.MirroredElemTypes]].cases)

  private[jsonschema] def fullName(named: Meta.Named): String = named.namespace.fold(named.name)(space => s"$space.${named.name}")

/** A case class's codec, writing its fields as an object's properties. */
private[jsonschema] final class ObjectCodec[A, T <: Tuple](
    named: Meta.Named,
    infos: List[Meta.Field],
    fields: Fields[T],
    defaults: List[Option[() => Either[String, Json]]],
    toTuple: A => T,
    fromTuple: T => A
) extends JsonSchemaCodec[A]:
  private val full = JsonSchemaCodec.fullName(named)

  /** Each field's default, where it has one its codec could write. */
  private lazy val written: List[Option[Json]] = defaults.map(_.flatMap(_().toOption))

  override def name: Option[String] = Some(full)

  override private[jsonschema] def describe(seen: Set[String]): Described =
    if seen.contains(full) then Described(Json.obj("$ref" -> Json.fromString(s"#/definitions/$full")), Map(full -> (() => body(Set(full)))))
    else body(seen + full)

  private def body(inside: Set[String]): Described =
    val described  = fields.describe(inside)
    val properties =
      infos.lazyZip(described).lazyZip(written).map: (info, field, default) =>
        val kept = default.filterNot(_ => info.noDefault)
        info.name -> Json.fromFields(
          field.json.asObject.fold(List("allOf" -> Json.arr(field.json)))(_.toList) ++ info.doc.map("description" -> Json.fromString(_)) ++
            kept.map("default" -> _)
        )
    val required =
      infos.lazyZip(fields.optional).lazyZip(written).collect {
        case (info, false, default) if default.forall(_ => info.noDefault) => Json.fromString(info.name)
      }
    val json =
      Json.fromFields(
        List("type" -> Json.fromString("object"), "title" -> Json.fromString(full)) ++ named.doc.map("description" -> Json.fromString(_)) ++
          List("properties" -> Json.fromFields(properties), "required" -> Json.fromValues(required), "additionalProperties" -> Json.False)
      )
    Described(json, described.map(_.definitions).foldLeft(Map.empty[String, () => Described])(_ ++ _))

  override def encode(value: A): Either[String, Json] =
    fields.encode(toTuple(value)).bimap(reason => s"$full: $reason", values => Json.fromFields(infos.map(_.name).zip(values)))

  override def decode(json: Json): Either[String, A] =
    json.asObject.toRight(s"expected an object of $full, found ${json.noSpaces}").flatMap: read =>
      // A field is read under its name or one of its aliases; one the record lacks takes its default, and an optional one none.
      infos.lazyZip(written).lazyZip(fields.optional).toList.traverse: (info, default, optional) =>
        (info.name :: info.aliases).collectFirstSome(read(_)).orElse(default).orElse(Option.when(optional)(Json.Null))
          .toRight(s"no field '${info.name}'")
      .flatMap(fields.decode).bimap(reason => s"$full: $reason", fromTuple)

/** A sealed trait's or enum's codec: text, a case's name, where every case is an object, and `oneOf` its cases' objects, each with its case's name in
  * the property `type`, where not.
  */
private[jsonschema] final class SumCodec[A](named: Meta.Named, cases: List[Case[A]]) extends JsonSchemaCodec[A]:
  private val full       = JsonSchemaCodec.fullName(named)
  private val singletons = cases.traverse(_.singleton)

  /** The tags each case's values carry: its own name, or its own cases' where it is a sealed trait of its own. */
  private val tagged: List[(Case[A], Set[String])] = cases.map(known => known -> known.codec.tags.getOrElse(Set(known.tag)))

  override def name: Option[String]                          = Some(full)
  override private[jsonschema] def tags: Option[Set[String]] = Some(tagged.flatMap(_._2).toSet)

  override private[jsonschema] def describe(seen: Set[String]): Described =
    singletons match
      case Some(_) => Described(
          Json.fromFields(
            List("type" -> Json.fromString("string"), "title" -> Json.fromString(full)) ++ named.doc.map("description" -> Json.fromString(_)) ++
              List("enum" -> Json.fromValues(cases.map(known => Json.fromString(known.tag))))
          ),
          Map.empty
        )
      case None if seen.contains(full) => Described(Json.obj("$ref" -> Json.fromString(s"#/definitions/$full")), Map(full -> (() => body(Set(full)))))
      case None                        => body(seen + full)

  private def body(inside: Set[String]): Described =
    val described = cases.map(known => known -> known.codec.describe(inside))
    // A case that is a sealed trait of its own adds its cases, already told apart by their own names.
    val branches =
      described.flatMap: (known, of) =>
        known.codec.tags match
          case Some(_) => of.json.hcursor.downField("oneOf").focus.flatMap(_.asArray).fold(List(of.json))(_.toList)
          case None    => List(withTag(of.json, known.tag))
    Described(
      Json.fromFields(
        List("title" -> Json.fromString(full)) ++ named.doc.map("description" -> Json.fromString(_)) ++ List("oneOf" -> Json.fromValues(branches))
      ),
      described.map(_._2.definitions).foldLeft(Map.empty[String, () => Described])(_ ++ _)
    )

  /** A case's object with its tag in the property `type`, required; a case described by reference is wrapped. */
  private def withTag(json: Json, tag: String): Json =
    val property = "type" -> Json.obj("const" -> Json.fromString(tag))
    json.asObject.filter(_.contains("properties")) match
      case Some(fields) =>
        val properties = fields("properties").flatMap(_.asObject).fold(List.empty[(String, Json)])(_.toList)
        val required   = fields("required").flatMap(_.asArray).getOrElse(Vector.empty)
        Json.fromJsonObject(
          fields.add("properties", Json.fromFields(property :: properties)).add("required", Json.fromValues(Json.fromString("type") +: required))
        )
      case None => Json
          .obj("allOf" -> Json.arr(json, Json.obj("properties" -> Json.fromFields(List(property)), "required" -> Json.arr(Json.fromString("type")))))

  override def encode(value: A): Either[String, Json] =
    cases.collectFirstSome(known => known.test.unapply(value).map(narrowed => (known, known.codec.encode(narrowed)))) match
      case None                   => Left(s"$value is none of the cases of $full")
      case Some((known, encoded)) =>
        if singletons.isDefined then Right(Json.fromString(known.tag))
        else if known.codec.tags.isDefined then encoded
        else
          encoded.flatMap: json =>
            json.asObject.toRight(s"case ${known.tag} of $full is not written as an object").flatMap: fields =>
              if fields.contains("type") then Left(s"case ${known.tag} of $full has a field named 'type', which tells the cases apart")
              else Right(Json.fromJsonObject(("type" -> Json.fromString(known.tag)) +: fields))

  override def decode(json: Json): Either[String, A] =
    if singletons.isDefined then
      json.asString.flatMap(name => cases.find(_.tag == name)).flatMap(_.singleton)
        .toRight(s"expected one of the cases of $full, found ${json.noSpaces}")
    else
      json.hcursor.get[String]("type").leftMap(_ => s"expected an object of a case of $full, with its case in 'type', found ${json.noSpaces}")
        .flatMap: tag =>
          tagged.collectFirst { case (known, owned) if owned.contains(tag) => known.codec.decode(json).map(known.widen) }
            .getOrElse(Left(s"'$tag' is not a case of $full"))

/** A codec found for a type where one is given, and derived where none is, made once it is first needed so that a type can hold itself. */
private[jsonschema] final class Resolved[A](make: => JsonSchemaCodec[A]):
  lazy val codec: JsonSchemaCodec[A] = make

private[jsonschema] object Resolved extends DerivedResolved:
  given found[A](using codec: => JsonSchemaCodec[A]): Resolved[A] = Resolved(codec)

private[jsonschema] trait DerivedResolved:
  inline given derived[A](using mirror: Mirror.Of[A]): Resolved[A] = Resolved(JsonSchemaCodec.derived[A](using mirror))

/** The values of a case class's fields, as a tuple of their types. */
private[jsonschema] trait Elements[A, T <: Tuple]:
  def apply(value: A): T

private[jsonschema] object Elements:
  given [A <: Product](using product: Mirror.ProductOf[A]): Elements[A, product.MirroredElemTypes] = value => Tuple.fromProductTyped(value)

/** The codecs of a case class's fields, whose types are `T`. */
private[jsonschema] trait Fields[T <: Tuple]:
  def describe(seen: Set[String]): List[Described]
  def optional: List[Boolean]
  def encode(values: T): Either[String, List[Json]]
  def decode(values: List[Json]): Either[String, T]

private[jsonschema] object Fields:
  given Fields[EmptyTuple] with
    override def describe(seen: Set[String]): List[Described]           = Nil
    override def optional: List[Boolean]                                = Nil
    override def encode(values: EmptyTuple): Either[String, List[Json]] = Right(Nil)
    override def decode(values: List[Json]): Either[String, EmptyTuple] = Right(EmptyTuple)

  given [H, T <: Tuple](using head: Resolved[H], tail: Fields[T]): Fields[H *: T] with
    override def describe(seen: Set[String]): List[Described]       = head.codec.describe(seen) :: tail.describe(seen)
    override def optional: List[Boolean]                            = head.codec.optional :: tail.optional
    override def encode(values: H *: T): Either[String, List[Json]] = (head.codec.encode(values.head), tail.encode(values.tail)).mapN(_ :: _)
    override def decode(values: List[Json]): Either[String, H *: T] =
      values match
        case value :: rest => (head.codec.decode(value), tail.decode(rest)).mapN(_ *: _)
        case Nil           => Left("fewer values than fields")

/** One case of a sealed trait or enum `A`: its codec and name, how a value of `A` is told to be of it, and its value where it is an object. */
private[jsonschema] trait Case[A]:
  type Of
  def codec: JsonSchemaCodec[Of]
  def tag: String
  def test: TypeTest[A, Of]
  def widen: Of => A
  def singleton: Option[A]

/** The cases of `A`, whose types are `T`. */
private[jsonschema] final case class Cases[A, T <: Tuple](cases: List[Case[A]])

private[jsonschema] object Cases:
  given [A]: Cases[A, EmptyTuple] = Cases(Nil)

  inline given [A, H, T <: Tuple](using
      resolved: Resolved[H],
      tested: TypeTest[A, H],
      up: H <:< A,
      single: Single[H],
      tail: Cases[A, T]
  ): Cases[A, H *: T] = Cases(KnownCase(Meta.named[H].name, resolved, tested, up, single) :: tail.cases)

/** A case named `tag`, whose type is `H`. */
private[jsonschema] final class KnownCase[A, H](val tag: String, resolved: Resolved[H], tested: TypeTest[A, H], up: H <:< A, single: Single[H])
    extends Case[A]:
  type Of = H
  override def codec: JsonSchemaCodec[H] = resolved.codec
  override def test: TypeTest[A, H]      = tested
  override def widen: H => A             = up
  override def singleton: Option[A]      = single.value.map(up)

/** A type's only value, where it has one. */
private[jsonschema] final case class Single[A](value: Option[A])

private[jsonschema] object Single extends NotSingle:
  given [A](using only: ValueOf[A]): Single[A] = Single(Some(only.value))

private[jsonschema] trait NotSingle:
  given [A]: Single[A] = Single(None)
