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
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime}
import java.util.UUID

import scala.compiletime.summonInline
import scala.deriving.Mirror
import scala.reflect.TypeTest
import scala.util.Try

import cats.syntax.all.*
import fs2.Chunk
import xkafka.schemaregistry.{DefaultOf, Meta}

/** A message or enum that a .proto file declares, by its names, with its declaration once it is asked for, so that a type can hold itself. */
private[protobuf] final case class Declared(namespace: Option[String], simple: String, message: Boolean, body: () => Body):
  def full: String = namespace.fold(simple)(space => s"$space.$simple")

/** A declaration's text, the messages and enums and the files it names, and why it cannot be declared, where it cannot. */
private[protobuf] final case class Body(text: String, types: List[Declared], imports: List[String], problems: List[String])

/** How a field of a type is declared: the type's name in the declaration, the messages and enums and the files it names, and why a field of it cannot
  * be declared, where it cannot.
  */
private[protobuf] final case class Described(name: String, types: List[Declared], imports: List[String], problems: List[String])

/** How a field is labelled, which says how it is written. */
private[protobuf] enum Label derives CanEqual:
  /** A single value with no presence, left out where it is zero, as proto3 leaves out a field it does not label. */
  case Scalar

  /** A single message, written wherever it is set. */
  case Message

  /** A value that may be absent, labelled `optional` where it is not a message, which has presence of its own. */
  case Optional(labelled: Boolean)

  case Repeated
  case Mapped

/** Single values, which a field, a repeated field's items or a map's values hold. */
private[protobuf] trait Single[A]:
  /** The wire type the value is written as. */
  def wire: Int

  /** Whether the value is its type's zero, which proto3 leaves out of a field without presence. */
  def zero(value: A): Boolean

  def put(value: A): Either[String, Raw]
  def take(raw: Raw): Either[String, A]

  /** The value a field without presence reads where it is absent. */
  def absent: Either[String, A]

private[protobuf] object Single:
  /** Single values of `B`, written as the `A` that `to` makes of them and read as `from` makes of an `A`. */
  def emap[A, B](self: Single[A])(from: A => Either[String, B])(to: B => A): Single[B] =
    new Single[B]:
      override def wire: Int                          = self.wire
      override def zero(value: B): Boolean            = self.zero(to(value))
      override def put(value: B): Either[String, Raw] = self.put(to(value))
      override def take(raw: Raw): Either[String, B]  = self.take(raw).flatMap(from)
      override def absent: Either[String, B]          = self.absent.flatMap(from)

/** How values of `A` are written as Protobuf, and read back, with the .proto declaration that describes them.
  *
  * Codecs are given for Scala's primitive types, text, bytes, options, collections, UUIDs and `java.time` types, and derived for case classes, sealed
  * traits and enums with `derives ProtobufCodec`, whose every field and case is numbered with [[ProtobufField]]. Other types are made from these with
  * [[imap]] and [[emap]].
  */
trait ProtobufCodec[A]:
  self =>

  private[protobuf] def describe: Described
  private[protobuf] def label: Label

  /** The codec as one of single values, where it is one; a repeated field's or map's is not. */
  private[protobuf] def single: Option[Single[A]]

  /** Field `number` holding `value`: nothing where it is absent, or where it is zero and has no `presence`. */
  private[protobuf] def write(number: Int, value: A, presence: Boolean): Either[String, Chunk[Byte]]

  /** A field's value from the values written under its number, in order; none where it is absent. */
  private[protobuf] def read(found: Vector[Raw]): Either[String, A]

  /** The full name of the message, which naming subjects by record takes, where it is a case class or sealed trait. */
  def name: Option[String] = None

  /** The .proto file, in proto3, that declares the message and the messages and enums it holds, in the message's package; or why it cannot be
    * declared, as where it is not a message or holds a list of lists.
    */
  final def schema: Either[String, String] = ProtobufCodec.file(this)

  /** The message's bytes. */
  final def encode(value: A): Either[String, Chunk[Byte]] =
    single.filter(_ => label == Label.Message).toRight(s"${describe.name} is not a message, which Protobuf writes alone").flatMap(_.put(value))
      .flatMap:
        case Raw.Bytes(bytes) => Right(bytes)
        case _                => Left(s"${describe.name} is not written as a message")

  /** A message read from its bytes. */
  final def decode(bytes: Chunk[Byte]): Either[String, A] =
    single.filter(_ => label == Label.Message).toRight(s"${describe.name} is not a message, which Protobuf reads alone")
      .flatMap(_.take(Raw.Bytes(bytes)))

  /** A codec for `B`, written as the `A` that `to` makes of it and read as `from` makes of an `A`. */
  final def imap[B](from: A => B)(to: B => A): ProtobufCodec[B] = emap(value => Right(from(value)))(to)

  /** A codec for `B`, written as the `A` that `to` makes of it and read as `from` makes of an `A`, which may refuse it. */
  final def emap[B](from: A => Either[String, B])(to: B => A): ProtobufCodec[B] =
    new ProtobufCodec[B]:
      override private[protobuf] def describe: Described                             = self.describe
      override private[protobuf] def label: Label                                    = self.label
      override private[protobuf] def single: Option[Single[B]]                       = self.single.map(Single.emap(_)(from)(to))
      override private[protobuf] def write(number: Int, value: B, presence: Boolean) = self.write(number, to(value), presence)
      override private[protobuf] def read(found: Vector[Raw]): Either[String, B]     = self.read(found).flatMap(from)
      override def name: Option[String]                                              = self.name

/** A codec of single values, which is one field. */
private[protobuf] abstract class Singular[A] extends ProtobufCodec[A], Single[A]:
  override private[protobuf] def single: Option[Single[A]] = Some(this)

  override private[protobuf] def write(number: Int, value: A, presence: Boolean): Either[String, Chunk[Byte]] =
    if !presence && zero(value) then Right(Chunk.empty) else put(value).map(ProtobufWire.field(number, _))

  override private[protobuf] def read(found: Vector[Raw]): Either[String, A] = found.lastOption.fold(absent)(take)

object ProtobufCodec:
  def apply[A](using codec: ProtobufCodec[A]): ProtobufCodec[A] = codec

  private def kind(raw: Raw): String =
    raw match
      case Raw.Varint(_)  => "a varint"
      case Raw.Fixed64(_) => "eight fixed bytes"
      case Raw.Bytes(_)   => "length-prefixed bytes"
      case Raw.Fixed32(_) => "four fixed bytes"

  private def scalar[A](named: String, wireType: Int, empty: A, isZero: A => Boolean)(
      toRaw: A => Raw
  )(fromRaw: PartialFunction[Raw, A]): ProtobufCodec[A] =
    new Singular[A]:
      override private[protobuf] def describe: Described = Described(named, Nil, Nil, Nil)
      override private[protobuf] def label: Label        = Label.Scalar
      override def wire: Int                             = wireType
      override def zero(value: A): Boolean               = isZero(value)
      override def put(value: A): Either[String, Raw]    = Right(toRaw(value))
      override def take(raw: Raw): Either[String, A]     = fromRaw.lift(raw).toRight(s"expected a $named, found ${kind(raw)}")
      override def absent: Either[String, A]             = Right(empty)

  private def int32(named: String, wire: Int)(write: Int => Raw)(read: PartialFunction[Raw, Int]): ProtobufCodec[Int] =
    scalar[Int](named, wire, 0, _ == 0)(write)(read)

  private def int64(named: String, wire: Int)(write: Long => Raw)(read: PartialFunction[Raw, Long]): ProtobufCodec[Long] =
    scalar[Long](named, wire, 0L, _ == 0L)(write)(read)

  given ProtobufCodec[Boolean] =
    scalar[Boolean]("bool", ProtobufWire.VarintType, false, !_)(value => Raw.Varint(if value then 1 else 0)) { case Raw.Varint(value) => value != 0 }

  given ProtobufCodec[Int]  = int32("int32", ProtobufWire.VarintType)(value => Raw.Varint(value.toLong)) { case Raw.Varint(value) => value.toInt }
  given ProtobufCodec[Long] = int64("int64", ProtobufWire.VarintType)(Raw.Varint(_)) { case Raw.Varint(value) => value }

  /** `Int` as `sint32`, which writes numbers near zero, negative or not, in few bytes; give it to use it in place of `int32`. */
  val sint32: ProtobufCodec[Int] =
    int32("sint32", ProtobufWire.VarintType)(value => Raw.Varint(((value << 1) ^ (value >> 31)).toLong & 0xffffffffL)) { case Raw.Varint(value) =>
      (value.toInt >>> 1) ^ -(value.toInt & 1)
    }

  /** `Long` as `sint64`, which writes numbers near zero, negative or not, in few bytes; give it to use it in place of `int64`. */
  val sint64: ProtobufCodec[Long] =
    int64("sint64", ProtobufWire.VarintType)(value => Raw.Varint((value << 1) ^ (value >> 63))) { case Raw.Varint(value) =>
      (value >>> 1) ^ -(value & 1)
    }

  /** `Int` as `fixed32`, four bytes whatever the number, read as unsigned by languages that have unsigned numbers. */
  val fixed32: ProtobufCodec[Int] = int32("fixed32", ProtobufWire.Fixed32Type)(Raw.Fixed32(_)) { case Raw.Fixed32(value) => value }

  /** `Long` as `fixed64`, eight bytes whatever the number, read as unsigned by languages that have unsigned numbers. */
  val fixed64: ProtobufCodec[Long] = int64("fixed64", ProtobufWire.Fixed64Type)(Raw.Fixed64(_)) { case Raw.Fixed64(value) => value }

  /** `Int` as `sfixed32`, four bytes whatever the number. */
  val sfixed32: ProtobufCodec[Int] = int32("sfixed32", ProtobufWire.Fixed32Type)(Raw.Fixed32(_)) { case Raw.Fixed32(value) => value }

  /** `Long` as `sfixed64`, eight bytes whatever the number. */
  val sfixed64: ProtobufCodec[Long] = int64("sfixed64", ProtobufWire.Fixed64Type)(Raw.Fixed64(_)) { case Raw.Fixed64(value) => value }

  given ProtobufCodec[Float] =
    scalar[Float]("float", ProtobufWire.Fixed32Type, 0f, java.lang.Float.floatToRawIntBits(_) == 0)(value =>
      Raw.Fixed32(java.lang.Float.floatToRawIntBits(value))
    ) { case Raw.Fixed32(value) => java.lang.Float.intBitsToFloat(value) }

  given ProtobufCodec[Double] =
    scalar[Double]("double", ProtobufWire.Fixed64Type, 0d, java.lang.Double.doubleToRawLongBits(_) == 0L)(value =>
      Raw.Fixed64(java.lang.Double.doubleToRawLongBits(value))
    ) { case Raw.Fixed64(value) => java.lang.Double.longBitsToDouble(value) }

  given ProtobufCodec[String] =
    scalar[String]("string", ProtobufWire.BytesType, "", _.isEmpty)(value => Raw.Bytes(Chunk.array(value.getBytes(StandardCharsets.UTF_8)))) {
      case Raw.Bytes(value) => new String(value.toArray, StandardCharsets.UTF_8)
    }

  given ProtobufCodec[Chunk[Byte]] =
    scalar[Chunk[Byte]]("bytes", ProtobufWire.BytesType, Chunk.empty, _.isEmpty)(Raw.Bytes(_)) { case Raw.Bytes(value) => value }

  /** Text that `parse` reads, which a field of reads as `empty` where it is absent. */
  private def text[A](parse: String => A)(write: A => String): ProtobufCodec[A] =
    ProtobufCodec[String].emap(value => Try(parse(value)).toEither.leftMap(_ => s"'$value' is not in its ISO-8601 form"))(write)

  /** A decimal number as text, as `BigDecimal` writes it. */
  given ProtobufCodec[BigDecimal] =
    ProtobufCodec[String].emap(value => Try(BigDecimal(value)).toEither.leftMap(_ => s"'$value' is not a decimal number"))(_.toString)

  given ProtobufCodec[UUID]          = text(UUID.fromString)(_.toString)
  given ProtobufCodec[LocalDate]     = text(LocalDate.parse)(_.toString)
  given ProtobufCodec[LocalTime]     = text(LocalTime.parse)(_.toString)
  given ProtobufCodec[LocalDateTime] = text(LocalDateTime.parse)(_.toString)

  /** The seconds of the first and last instants `google.protobuf.Timestamp` holds, the start of year 1 and the end of year 9999. */
  private val (earliest, latest) = (-62135596800L, 253402300799L)

  /** An instant as `google.protobuf.Timestamp`, seconds and nanoseconds since the epoch, which every Protobuf library and the registry know. */
  given ProtobufCodec[Instant] =
    new Singular[Instant]:
      override private[protobuf] def describe: Described = Described("google.protobuf.Timestamp", Nil, List("google/protobuf/timestamp.proto"), Nil)
      override private[protobuf] def label: Label        = Label.Message
      override def wire: Int                             = ProtobufWire.BytesType
      override def zero(value: Instant): Boolean         = false
      override def put(value: Instant): Either[String, Raw] =
        if value.getEpochSecond < earliest || value.getEpochSecond > latest then Left(s"$value is outside the years 1 to 9999 a Timestamp holds")
        else
          val seconds = if value.getEpochSecond == 0 then Chunk.empty else ProtobufWire.field(1, Raw.Varint(value.getEpochSecond))
          val nanos   = if value.getNano == 0 then Chunk.empty else ProtobufWire.field(2, Raw.Varint(value.getNano.toLong))
          Right(Raw.Bytes(seconds ++ nanos))
      override def take(raw: Raw): Either[String, Instant] =
        raw match
          case Raw.Bytes(bytes) => ProtobufWire.fields(bytes).flatMap: fields =>
              val seconds = fields.collect { case (1, Raw.Varint(value)) => value }.lastOption.getOrElse(0L)
              val nanos   = fields.collect { case (2, Raw.Varint(value)) => value.toInt }.lastOption.getOrElse(0)
              if nanos < 0 || nanos > 999999999 then Left(s"a Timestamp's nanoseconds are $nanos")
              else Try(Instant.ofEpochSecond(seconds, nanos.toLong)).toEither.leftMap(_ => s"a Timestamp of $seconds seconds is no instant")
          case other => Left(s"expected a Timestamp, found ${kind(other)}")
      override def absent: Either[String, Instant] = Right(Instant.EPOCH)

  /** The problem with `held` holding `inner`'s values where they are lists, maps or options, which Protobuf has no field for. */
  private def unheld[A](inner: ProtobufCodec[A], held: String): List[String] =
    val holding =
      inner.label match
        case Label.Optional(_) => Some("options")
        case Label.Repeated    => Some("lists")
        case Label.Mapped      => Some("maps")
        case _                 => None
    holding.map(kind => s"$held holding $kind cannot be declared, as Protobuf has no field that holds it").toList

  private def notSingle(held: String): String = s"$held holding lists, maps or options cannot be written, as Protobuf has no field that holds it"

  /** A value that may be absent: a field labelled `optional`, or a message's field, which has presence of its own. Absent reads as none. */
  given [A](using inner: ProtobufCodec[A]): ProtobufCodec[Option[A]] with
    override private[protobuf] def describe: Described =
      val described = inner.describe
      described.copy(problems = described.problems ++ unheld(inner, "an option"))
    override private[protobuf] def label: Label                      = Label.Optional(inner.label != Label.Message)
    override private[protobuf] def single: Option[Single[Option[A]]] = None
    override private[protobuf] def write(number: Int, value: Option[A], presence: Boolean): Either[String, Chunk[Byte]] =
      inner.single.toRight(notSingle("an option")).flatMap(_ => value.fold(Right(Chunk.empty))(inner.write(number, _, presence = true)))
    override private[protobuf] def read(found: Vector[Raw]): Either[String, Option[A]] =
      if found.isEmpty then Right(None) else inner.read(found).map(Some(_))

  /** A repeated field, packed where its values are numbers, as proto3 writes them; packed and unpacked values are both read. */
  private def repeated[A, C](inner: ProtobufCodec[A])(items: C => Iterable[A], make: Vector[A] => C): ProtobufCodec[C] =
    new ProtobufCodec[C]:
      override private[protobuf] def describe: Described =
        val described = inner.describe
        described.copy(problems = described.problems ++ unheld(inner, "a list"))
      override private[protobuf] def label: Label                                                                 = Label.Repeated
      override private[protobuf] def single: Option[Single[C]]                                                    = None
      override private[protobuf] def write(number: Int, value: C, presence: Boolean): Either[String, Chunk[Byte]] =
        inner.single.toRight(notSingle("a list")).flatMap: one =>
          items(value).toVector.traverse(one.put).map: raws =>
            if raws.isEmpty then Chunk.empty
            else if one.wire != ProtobufWire.BytesType then
              ProtobufWire.field(number, Raw.Bytes(raws.foldLeft(Chunk.empty[Byte])((packed, raw) => packed ++ ProtobufWire.payload(raw))))
            else raws.foldLeft(Chunk.empty[Byte])((written, raw) => written ++ ProtobufWire.field(number, raw))
      override private[protobuf] def read(found: Vector[Raw]): Either[String, C] =
        inner.single.toRight(notSingle("a list")).flatMap: one =>
          found.flatTraverse:
            case Raw.Bytes(bytes) if one.wire != ProtobufWire.BytesType => ProtobufWire.packed(bytes, one.wire).flatMap(_.traverse(one.take))
            case raw                                                    => one.take(raw).map(Vector(_))
          .map(make)

  given [A](using inner: ProtobufCodec[A]): ProtobufCodec[List[A]]   = repeated(inner)(identity, _.toList)
  given [A](using inner: ProtobufCodec[A]): ProtobufCodec[Vector[A]] = repeated(inner)(identity, identity)
  given [A](using inner: ProtobufCodec[A]): ProtobufCodec[Seq[A]]    = repeated(inner)(identity, identity)
  given [A](using inner: ProtobufCodec[A]): ProtobufCodec[Set[A]]    = repeated(inner)(identity, _.toSet)

  /** Values by text keys, as a `map<string, ...>` field: each entry a message of its key and value, the later of two with one key read. */
  given [A](using inner: ProtobufCodec[A]): ProtobufCodec[Map[String, A]] with
    override private[protobuf] def describe: Described =
      val described = inner.describe
      described.copy(name = s"map<string, ${described.name}>", problems = described.problems ++ unheld(inner, "a map"))
    override private[protobuf] def label: Label                                                                              = Label.Mapped
    override private[protobuf] def single: Option[Single[Map[String, A]]]                                                    = None
    override private[protobuf] def write(number: Int, value: Map[String, A], presence: Boolean): Either[String, Chunk[Byte]] =
      inner.single.toRight(notSingle("a map")).flatMap: one =>
        value.toList
          .traverse((key, item) => (ProtobufCodec[String].write(1, key, presence = true), one.put(item)).mapN(_ ++ ProtobufWire.field(2, _)))
          .map(_.foldLeft(Chunk.empty[Byte])((written, entry) => written ++ ProtobufWire.field(number, Raw.Bytes(entry))))
    override private[protobuf] def read(found: Vector[Raw]): Either[String, Map[String, A]] =
      inner.single.toRight(notSingle("a map")).flatMap: one =>
        found.traverse:
          case Raw.Bytes(entry) => ProtobufWire.fields(entry).flatMap: fields =>
              def at(number: Int) = fields.collect { case (`number`, raw) => raw }
              (ProtobufCodec[String].read(at(1)), at(2).lastOption.fold(one.absent)(one.take)).tupled
          case other => Left(s"expected a map's entry, found ${kind(other)}")
        .map(_.toMap)

  /** A field's default as its codec writes it under number 1, which a derived message reads where a record lacks the field. */
  given [A](using resolved: Resolved[A]): DefaultOf[ProtobufCodec, Chunk[Byte], A] = value => resolved.codec.write(1, value, presence = true)

  /** A codec for a case class, as a message of its fields; for an enum or sealed trait whose cases are all objects, as an enum; and for another
    * sealed trait or enum, as a message whose one field, of a `oneof`, is its case. Every field and case is numbered with [[ProtobufField]]; an enum
    * needs a case numbered 0, which a field of it reads where it is absent.
    *
    * A field with a Scala default is labelled `optional` and written even where it is zero, so a record lacking it reads the default; other fields
    * are written as proto3 writes them, left out where they are zero and read as zero where they are absent. Names come from the Scala names and
    * packages, which the annotations of `xkafka.schemaregistry` change.
    */
  inline def derived[A](using mirror: Mirror.Of[A]): ProtobufCodec[A] =
    inline mirror match
      case product: Mirror.ProductOf[A] => MessageCodec[A, product.MirroredElemTypes](
          Meta.named[A],
          Meta.fields[A],
          Numbers.fields[A],
          summonInline[Fields[product.MirroredElemTypes]],
          Meta.defaults[A, ProtobufCodec, Chunk[Byte]],
          summonInline[Elements[A, product.MirroredElemTypes]].apply,
          product.fromTuple
        )
      case sum: Mirror.SumOf[A] =>
        Numbers.cases[A]
        SumCodec[A](Meta.named[A], summonInline[Cases[A, sum.MirroredElemTypes]].cases)

  /** A field's or case's name with its first letter in lower case, as Protobuf names fields. */
  private[protobuf] def fieldName(name: String): String = name.take(1).toLowerCase + name.drop(1)

  /** Lines of documentation as comments, indented by `indent`. */
  private[protobuf] def comment(doc: Option[String], indent: String): String =
    doc.fold("")(_.linesIterator.map(line => s"$indent// $line\n").mkString)

  /** The .proto file declaring `codec`'s message, first, and every message and enum it holds, in the message's package. */
  private[protobuf] def file[A](codec: ProtobufCodec[A]): Either[String, String] =
    val root = codec.describe
    root.types.headOption.filter(declared => declared.message && codec.label == Label.Message && declared.simple == root.name) match
      case None      => Left(s"${root.name} is not a message, and a Protobuf record is one")
      case Some(top) =>
        def close(pending: List[Declared], done: Vector[(Declared, Body)]): Vector[(Declared, Body)] =
          pending match
            case Nil                                                         => done
            case declared :: rest if done.exists(_._1.full == declared.full) => close(rest, done)
            case declared :: rest                                            =>
              val body = declared.body()
              close(rest ++ body.types, done :+ (declared -> body))
        val declared = close(List(top), Vector.empty)
        // Every type is declared in the package of the message, by its simple name, so two types may not share one.
        val clashes =
          declared.map(_._1).groupBy(_.simple).toList.sortBy(_._1).collect {
            case (simple, many) if many.size > 1 => s"${many.map(_.full).mkString(" and ")} would both be declared as $simple"
          }
        (root.problems ++ declared.flatMap(_._2.problems) ++ clashes).distinct match
          case Nil =>
            val imports = (root.imports ++ declared.flatMap(_._2.imports)).distinct.sorted
            Right(
              (List("syntax = \"proto3\";") ++ top.namespace.map(space => s"package $space;") ++
                Option.when(imports.nonEmpty)(imports.map(file => s"import \"$file\";").mkString("\n")) ++ declared.map(_._2.text))
                .mkString("", "\n\n", "\n")
            )
          case problems => Left(problems.mkString("; "))

/** A case class's codec, writing its fields as a message's. */
private[protobuf] final class MessageCodec[A, T <: Tuple](
    named: Meta.Named,
    infos: List[Meta.Field],
    numbers: List[Int],
    fields: Fields[T],
    defaults: List[Option[() => Either[String, Chunk[Byte]]]],
    toTuple: A => T,
    fromTuple: T => A
) extends Singular[A]:
  private val full = named.namespace.fold(named.name)(space => s"$space.${named.name}")

  /** Each field's default as it is written, where it has one that applies: one of a single value, not left out with `SchemaNoDefault`. */
  private lazy val written: List[Option[Vector[Raw]]] =
    infos.lazyZip(fields.labels).lazyZip(defaults).map: (info, label, default) =>
      default.filter(_ => !info.noDefault && (label == Label.Scalar || label == Label.Message))
        .flatMap(_().flatMap(ProtobufWire.fields).toOption.map(_.map(_._2)))

  private lazy val presence: List[Boolean] = written.map(_.isDefined)

  override def name: Option[String] = Some(full)

  override private[protobuf] def label: Label = Label.Message

  override private[protobuf] def describe: Described =
    Described(named.name, List(Declared(named.namespace, named.name, message = true, () => body)), Nil, Nil)

  private def body: Body =
    val described = fields.describe
    val lines     =
      infos.lazyZip(numbers).lazyZip(described).lazyZip(fields.labels).zip(presence).map:
        case ((info, number, field, label), present) =>
          val prefix =
            label match
              case Label.Optional(true)    => "optional "
              case Label.Scalar if present => "optional "
              case Label.Repeated          => "repeated "
              case _                       => ""
          ProtobufCodec.comment(info.doc, "  ") + s"  $prefix${field.name} ${info.name} = $number;\n"
    Body(
      ProtobufCodec.comment(named.doc, "") + s"message ${named.name} {\n${lines.mkString}}",
      described.flatMap(_.types),
      described.flatMap(_.imports),
      infos.zip(described).flatMap((info, field) => field.problems.map(problem => s"field ${info.name} of $full: $problem"))
    )

  override def wire: Int               = ProtobufWire.BytesType
  override def zero(value: A): Boolean = false

  override def put(value: A): Either[String, Raw] =
    fields.write(toTuple(value), numbers, presence).bimap(
      reason => s"$full: $reason",
      written => Raw.Bytes(numbers.zip(written).sortBy(_._1).foldLeft(Chunk.empty[Byte])((all, field) => all ++ field._2))
    )

  override def take(raw: Raw): Either[String, A] =
    raw match
      case Raw.Bytes(bytes) => ProtobufWire.fields(bytes).flatMap: read =>
          val byNumber = read.groupMap(_._1)(_._2)
          // A field the record lacks takes its default, where it has one.
          fields.read(numbers.zip(written).map((number, default) => byNumber.get(number).orElse(default).getOrElse(Vector.empty)))
        .bimap(reason => s"$full: $reason", fromTuple)
      case _ => Left(s"expected a message of $full, found another wire type")

  override def absent: Either[String, A] = take(Raw.Bytes(Chunk.empty))

private[protobuf] object SumCodec:
  /** An enum where every case is an object, and a message of a `oneof` of the cases where not. */
  def apply[A](named: Meta.Named, cases: List[Case[A]]): ProtobufCodec[A] =
    cases.traverse(known => known.singleton.map(known -> _)).fold(OneofCodec(named, cases))(EnumCodec(named, _))

  /** A name in capitals, with words parted by underscores, as Protobuf names enum values. */
  def constant(name: String): String = name.replaceAll("([a-z0-9])([A-Z])", "$1_$2").replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2").toUpperCase

/** An enum of objects, each a value of a Protobuf enum named with the enum's name and its own, as `STATUS_OPEN`. */
private[protobuf] final class EnumCodec[A](named: Meta.Named, values: List[(Case[A], A)]) extends Singular[A]:
  private val full = named.namespace.fold(named.name)(space => s"$space.${named.name}")

  override def name: Option[String] = Some(full)

  override private[protobuf] def label: Label = Label.Scalar

  override private[protobuf] def describe: Described =
    Described(named.name, List(Declared(named.namespace, named.name, message = false, () => body)), Nil, Nil)

  private def body: Body =
    // Protobuf takes an enum's first value as its zero.
    val ordered = values.filter(_._1.number == 0) ++ values.filterNot(_._1.number == 0)
    val lines   = ordered.map((known, _) => s"  ${SumCodec.constant(named.name)}_${SumCodec.constant(known.tag)} = ${known.number};\n")
    Body(ProtobufCodec.comment(named.doc, "") + s"enum ${named.name} {\n${lines.mkString}}", Nil, Nil, Nil)

  private def numberOf(value: A): Either[String, Int] =
    values.collectFirst { case (known, _) if known.test.unapply(value).isDefined => known.number }.toRight(s"$value is none of the cases of $full")

  override def wire: Int                          = ProtobufWire.VarintType
  override def zero(value: A): Boolean            = numberOf(value).contains(0)
  override def put(value: A): Either[String, Raw] = numberOf(value).map(number => Raw.Varint(number.toLong))

  override def take(raw: Raw): Either[String, A] =
    raw match
      case Raw.Varint(number) => values.collectFirst { case (known, value) if known.number.toLong == number.toInt.toLong => value }
          .toRight(s"$number is not a case of $full")
      case _ => Left(s"expected a case of $full, found another wire type")

  override def absent: Either[String, A] = take(Raw.Varint(0))

/** A sealed trait's or enum's codec, as a message with one field, its case, of a `oneof` named as the trait is. */
private[protobuf] final class OneofCodec[A](named: Meta.Named, cases: List[Case[A]]) extends Singular[A]:
  private val full = named.namespace.fold(named.name)(space => s"$space.${named.name}")

  override def name: Option[String] = Some(full)

  override private[protobuf] def label: Label = Label.Message

  override private[protobuf] def describe: Described =
    Described(named.name, List(Declared(named.namespace, named.name, message = true, () => body)), Nil, Nil)

  private def body: Body =
    val described = cases.map(known => known -> known.codec.describe)
    val lines     = described.map((known, case_) => s"    ${case_.name} ${ProtobufCodec.fieldName(known.tag)} = ${known.number};\n")
    val unheld    =
      cases.filter(known => known.codec.label != Label.Scalar && known.codec.label != Label.Message)
        .map(known => s"case ${known.tag} of $full is not a single value, which a oneof's field holds")
    Body(
      ProtobufCodec.comment(named.doc, "") + s"message ${named.name} {\n  oneof ${ProtobufCodec.fieldName(named.name)} {\n${lines.mkString}  }\n}",
      described.flatMap(_._2.types),
      described.flatMap(_._2.imports),
      described.flatMap(_._2.problems) ++ unheld
    )

  override def wire: Int               = ProtobufWire.BytesType
  override def zero(value: A): Boolean = false

  override def put(value: A): Either[String, Raw] =
    cases.collectFirstSome(known => known.test.unapply(value).map(narrowed => known.codec.write(known.number, narrowed, presence = true)))
      .getOrElse(Left(s"$value is none of the cases of $full")).map(Raw.Bytes(_))

  override def take(raw: Raw): Either[String, A] =
    raw match
      case Raw.Bytes(bytes) => ProtobufWire.fields(bytes).flatMap: read =>
          // Of a oneof's fields, the last one written is the one set.
          read.reverseIterator.flatMap((number, value) => cases.find(_.number == number).map(_ -> value)).nextOption() match
            case Some((known, value)) => known.codec.read(Vector(value)).map(known.widen)
            case None                 => Left(s"no case of $full is set")
      case _ => Left(s"expected a message of $full, found another wire type")

  override def absent: Either[String, A] = take(Raw.Bytes(Chunk.empty))

/** A codec found for a type where one is given, and derived where none is, made once it is first needed so that a type can hold itself. */
private[protobuf] final class Resolved[A](make: => ProtobufCodec[A]):
  lazy val codec: ProtobufCodec[A] = make

private[protobuf] object Resolved extends DerivedResolved:
  given found[A](using codec: => ProtobufCodec[A]): Resolved[A] = Resolved(codec)

private[protobuf] trait DerivedResolved:
  inline given derived[A](using mirror: Mirror.Of[A]): Resolved[A] = Resolved(ProtobufCodec.derived[A](using mirror))

/** The values of a case class's fields, as a tuple of their types. */
private[protobuf] trait Elements[A, T <: Tuple]:
  def apply(value: A): T

private[protobuf] object Elements:
  given [A <: Product](using product: Mirror.ProductOf[A]): Elements[A, product.MirroredElemTypes] = value => Tuple.fromProductTyped(value)

/** The codecs of a case class's fields, whose types are `T`. */
private[protobuf] trait Fields[T <: Tuple]:
  def describe: List[Described]
  def labels: List[Label]

  /** Each field as it is written under its number, written even where it is zero where it has `presence`. */
  def write(values: T, numbers: List[Int], presence: List[Boolean]): Either[String, List[Chunk[Byte]]]

  /** The fields from the values written under each one's number. */
  def read(found: List[Vector[Raw]]): Either[String, T]

private[protobuf] object Fields:
  given Fields[EmptyTuple] with
    override def describe: List[Described]                                                                                 = Nil
    override def labels: List[Label]                                                                                       = Nil
    override def write(values: EmptyTuple, numbers: List[Int], presence: List[Boolean]): Either[String, List[Chunk[Byte]]] = Right(Nil)
    override def read(found: List[Vector[Raw]]): Either[String, EmptyTuple]                                                = Right(EmptyTuple)

  given [H, T <: Tuple](using head: Resolved[H], tail: Fields[T]): Fields[H *: T] with
    override def describe: List[Described] = head.codec.describe :: tail.describe
    override def labels: List[Label]       = head.codec.label :: tail.labels
    override def write(values: H *: T, numbers: List[Int], presence: List[Boolean]): Either[String, List[Chunk[Byte]]] =
      (
        head.codec.write(numbers.head, values.head, presence.head).leftMap(reason => s"field ${numbers.head}: $reason"),
        tail.write(values.tail, numbers.tail, presence.tail)
      ).mapN(_ :: _)
    override def read(found: List[Vector[Raw]]): Either[String, H *: T] =
      found match
        case values :: rest => (head.codec.read(values), tail.read(rest)).mapN(_ *: _)
        case Nil            => Left("fewer values than fields")

/** One case of a sealed trait or enum `A`: its codec, name and number, how a value of `A` is told to be of it, and its value where it is an object.
  */
private[protobuf] trait Case[A]:
  type Of
  def codec: ProtobufCodec[Of]
  def tag: String
  def number: Int
  def test: TypeTest[A, Of]
  def widen: Of => A
  def singleton: Option[A]

/** The cases of `A`, whose types are `T`. */
private[protobuf] final case class Cases[A, T <: Tuple](cases: List[Case[A]])

private[protobuf] object Cases:
  given [A]: Cases[A, EmptyTuple] = Cases(Nil)

  inline given [A, H, T <: Tuple](using
      resolved: Resolved[H],
      tested: TypeTest[A, H],
      up: H <:< A,
      only: Only[H],
      tail: Cases[A, T]
  ): Cases[A, H *: T] = Cases(KnownCase(Meta.named[H].name, Numbers.of[H], resolved, tested, up, only) :: tail.cases)

/** A case named `tag` and numbered `number`, whose type is `H`. */
private[protobuf] final class KnownCase[A, H](
    val tag: String,
    val number: Int,
    resolved: Resolved[H],
    tested: TypeTest[A, H],
    up: H <:< A,
    only: Only[H]
) extends Case[A]:
  type Of = H
  override def codec: ProtobufCodec[H] = resolved.codec
  override def test: TypeTest[A, H]    = tested
  override def widen: H => A           = up
  override def singleton: Option[A]    = only.value.map(up)

/** A type's only value, where it has one. */
private[protobuf] final case class Only[A](value: Option[A])

private[protobuf] object Only extends NotOnly:
  given [A](using only: ValueOf[A]): Only[A] = Only(Some(only.value))

private[protobuf] trait NotOnly:
  given [A]: Only[A] = Only(None)
