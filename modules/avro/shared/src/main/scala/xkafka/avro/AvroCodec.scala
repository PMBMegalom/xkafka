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

import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, ZoneOffset}
import java.util.UUID

import scala.compiletime.summonInline
import scala.deriving.Mirror
import scala.reflect.TypeTest
import scala.util.Try

import cats.syntax.all.*
import fs2.Chunk

/** How values of `A` are written as Avro values of a schema, and read back from them.
  *
  * Codecs are given for Scala's primitive types, text, bytes, options, collections, UUIDs and `java.time` types, and derived for case classes, sealed
  * traits and enums with `derives AvroCodec`. Other types are made from these with [[imap]] and [[emap]].
  */
trait AvroCodec[A]:
  self =>

  /** The schema, where a record already defined in `seen`, the records it is inside, is named by its name alone. */
  private[avro] def schemaWithin(seen: Set[FullName]): AvroSchema

  /** The schema values are written with. */
  final def schema: AvroSchema = schemaWithin(Set.empty)

  def encode(value: A): Either[String, AvroValue]

  /** The value `value` is, which was read with this codec's schema. */
  def decode(value: AvroValue): Either[String, A]

  /** A codec for `B`, written as the `A` that `to` makes of it and read as `from` makes of an `A`. */
  final def imap[B](from: A => B)(to: B => A): AvroCodec[B] = emap(value => Right(from(value)))(to)

  /** A codec for `B`, written as the `A` that `to` makes of it and read as `from` makes of an `A`, which may refuse it. */
  final def emap[B](from: A => Either[String, B])(to: B => A): AvroCodec[B] =
    new AvroCodec[B]:
      override private[avro] def schemaWithin(seen: Set[FullName]): AvroSchema = self.schemaWithin(seen)
      override def encode(value: B): Either[String, AvroValue]                 = self.encode(to(value))
      override def decode(value: AvroValue): Either[String, B]                 = self.decode(value).flatMap(from)

object AvroCodec:
  def apply[A](using codec: AvroCodec[A]): AvroCodec[A] = codec

  /** A codec of `schema`, writing with `encode` and reading with `decode`. */
  def instance[A](avro: AvroSchema, encode: A => Either[String, AvroValue], decode: AvroValue => Either[String, A]): AvroCodec[A] =
    val (write, read) = (encode, decode)
    new AvroCodec[A]:
      override private[avro] def schemaWithin(seen: Set[FullName]): AvroSchema = avro
      override def encode(value: A): Either[String, AvroValue]                 = write(value)
      override def decode(value: AvroValue): Either[String, A]                 = read(value)

  /** Values of `schema` as they are, for schemas known only as the program runs. A value that does not fit the schema is refused when it is written.
    */
  def generic(schema: AvroSchema): AvroCodec[AvroValue] = instance(schema, Right(_), Right(_))

  private def primitive[A](schema: AvroSchema)(write: A => AvroValue)(read: PartialFunction[AvroValue, A]): AvroCodec[A] =
    instance(
      schema,
      value => Right(write(value)),
      value => read.lift(value).toRight(s"expected a value of ${AvroSchemaJson.render(schema)}, found $value")
    )

  given AvroCodec[Boolean]     = primitive(AvroSchema.Boolean)(AvroValue.Boolean(_)) { case AvroValue.Boolean(value) => value }
  given AvroCodec[Int]         = primitive(AvroSchema.Int)(AvroValue.Int(_)) { case AvroValue.Int(value) => value }
  given AvroCodec[Long]        = primitive(AvroSchema.Long)(AvroValue.Long(_)) { case AvroValue.Long(value) => value }
  given AvroCodec[Float]       = primitive(AvroSchema.Float)(AvroValue.Float(_)) { case AvroValue.Float(value) => value }
  given AvroCodec[Double]      = primitive(AvroSchema.Double)(AvroValue.Double(_)) { case AvroValue.Double(value) => value }
  given AvroCodec[String]      = primitive(AvroSchema.String)(AvroValue.String(_)) { case AvroValue.String(value) => value }
  given AvroCodec[Chunk[Byte]] = primitive(AvroSchema.Bytes)(AvroValue.Bytes(_)) { case AvroValue.Bytes(value) => value }

  /** A value that may be absent, as a union of null and the value's schema, null first, so a field of it may default to null. */
  given [A](using inner: AvroCodec[A]): AvroCodec[Option[A]] with
    override private[avro] def schemaWithin(seen: Set[FullName]): AvroSchema =
      inner.schemaWithin(seen) match
        case AvroSchema.Union(branches) => AvroSchema.Union(AvroSchema.Null :: branches.filterNot(_ == AvroSchema.Null))
        case other                      => AvroSchema.Union(List(AvroSchema.Null, other))
    override def encode(value: Option[A]): Either[String, AvroValue] = value.fold(Right(AvroValue.Null))(inner.encode)
    override def decode(value: AvroValue): Either[String, Option[A]] =
      value match
        case AvroValue.Null => Right(None)
        case other          => inner.decode(other).map(Some(_))

  private def array[A, C](inner: AvroCodec[A])(items: C => Iterable[A], make: Vector[A] => C): AvroCodec[C] =
    new AvroCodec[C]:
      override private[avro] def schemaWithin(seen: Set[FullName]): AvroSchema = AvroSchema.Array(inner.schemaWithin(seen))
      override def encode(value: C): Either[String, AvroValue]                 = items(value).toVector.traverse(inner.encode).map(AvroValue.Array(_))
      override def decode(value: AvroValue): Either[String, C]                 =
        value match
          case AvroValue.Array(values) => values.traverse(inner.decode).map(make)
          case other                   => Left(s"expected an array, found $other")

  given [A](using inner: AvroCodec[A]): AvroCodec[List[A]]   = array(inner)(identity, _.toList)
  given [A](using inner: AvroCodec[A]): AvroCodec[Vector[A]] = array(inner)(identity, identity)
  given [A](using inner: AvroCodec[A]): AvroCodec[Seq[A]]    = array(inner)(identity, identity)
  given [A](using inner: AvroCodec[A]): AvroCodec[Set[A]]    = array(inner)(identity, _.toSet)

  /** Values by text keys, as an Avro map. */
  given [A](using inner: AvroCodec[A]): AvroCodec[Map[String, A]] with
    override private[avro] def schemaWithin(seen: Set[FullName]): AvroSchema = AvroSchema.Map(inner.schemaWithin(seen))
    override def encode(value: Map[String, A]): Either[String, AvroValue]    =
      value.toVector.traverse((key, item) => inner.encode(item).map(key -> _)).map(AvroValue.Map(_))
    override def decode(value: AvroValue): Either[String, Map[String, A]] =
      value match
        case AvroValue.Map(entries) => entries.traverse((key, item) => inner.decode(item).map(key -> _)).map(_.toMap)
        case other                  => Left(s"expected a map, found $other")

  private def logical[A, B](logicalType: LogicalType, underlying: AvroCodec[B])(from: B => A)(to: A => B): AvroCodec[A] =
    instance(
      AvroSchema.Logical(logicalType),
      value => attempt(to(value)).flatMap(underlying.encode),
      value => underlying.decode(value).flatMap(read => attempt(from(read)))
    )

  /** What `run` gives, or why it threw, for the conversions of numbers and times that throw where a value is out of their range. */
  private def attempt[A](run: => A): Either[String, A] = Try(run).toEither.leftMap(error => s"${error.getClass.getSimpleName}: ${error.getMessage}")

  given AvroCodec[UUID] = logical(LogicalType.Uuid, AvroCodec[String])(UUID.fromString)(_.toString)

  /** An instant as `timestamp-millis`, Avro's and Confluent's usual choice, which keeps the instant to the millisecond. */
  given AvroCodec[Instant] = logical(LogicalType.TimestampMillis, AvroCodec[Long])(Instant.ofEpochMilli)(_.toEpochMilli)

  /** An instant as `timestamp-micros`. */
  val instantMicros: AvroCodec[Instant] =
    logical(LogicalType.TimestampMicros, AvroCodec[Long])(micros =>
      Instant.ofEpochSecond(Math.floorDiv(micros, 1000000L), Math.floorMod(micros, 1000000L) * 1000L)
    )(instant => Math.addExact(Math.multiplyExact(instant.getEpochSecond, 1000000L), instant.getNano / 1000L))

  /** An instant as `timestamp-nanos`. */
  val instantNanos: AvroCodec[Instant] =
    logical(LogicalType.TimestampNanos, AvroCodec[Long])(nanos =>
      Instant.ofEpochSecond(Math.floorDiv(nanos, 1000000000L), Math.floorMod(nanos, 1000000000L))
    )(instant => Math.addExact(Math.multiplyExact(instant.getEpochSecond, 1000000000L), instant.getNano.toLong))

  given AvroCodec[LocalDate] =
    logical(LogicalType.Date, AvroCodec[Int])(days => LocalDate.ofEpochDay(days.toLong))(date => Math.toIntExact(date.toEpochDay))

  /** A time of day as `time-micros`, which keeps it to the microsecond. */
  given AvroCodec[LocalTime] =
    logical(LogicalType.TimeMicros, AvroCodec[Long])(micros => LocalTime.ofNanoOfDay(Math.multiplyExact(micros, 1000L)))(_.toNanoOfDay / 1000L)

  /** A time of day as `time-millis`. */
  val localTimeMillis: AvroCodec[LocalTime] =
    logical(LogicalType.TimeMillis, AvroCodec[Int])(millis => LocalTime.ofNanoOfDay(millis.toLong * 1000000L))(time =>
      (time.toNanoOfDay / 1000000L).toInt
    )

  /** A date and time as `local-timestamp-micros`, which keeps it to the microsecond. */
  given AvroCodec[LocalDateTime] =
    logical(LogicalType.LocalTimestampMicros, instantMicros)(LocalDateTime.ofInstant(_, ZoneOffset.UTC))(_.toInstant(ZoneOffset.UTC))

  /** A date and time as `local-timestamp-millis`. */
  val localDateTimeMillis: AvroCodec[LocalDateTime] =
    logical(LogicalType.LocalTimestampMillis, AvroCodec[Instant])(LocalDateTime.ofInstant(_, ZoneOffset.UTC))(_.toInstant(ZoneOffset.UTC))

  /** Decimal numbers of at most `precision` digits, `scale` of them after the point, written as the bytes of their unscaled value. A number that
    * needs more digits, or more after the point, is refused rather than rounded.
    */
  def decimal(precision: Int, scale: Int): AvroCodec[BigDecimal] =
    val write =
      (value: BigDecimal) =>
        attempt(value.bigDecimal.setScale(scale, java.math.RoundingMode.UNNECESSARY))
          .leftMap(_ => s"$value has more than $scale digits after the point")
          .filterOrElse(_.precision <= precision, s"$value has more than $precision digits")
          .map(scaled => Chunk.array(scaled.unscaledValue.toByteArray))
    val read = (bytes: Chunk[Byte]) => Right(BigDecimal(new java.math.BigDecimal(new java.math.BigInteger(bytes.toArray), scale)))
    instance(
      AvroSchema.Logical(LogicalType.Decimal(precision, scale)),
      value => write(value).flatMap(AvroCodec[Chunk[Byte]].encode),
      value => AvroCodec[Chunk[Byte]].decode(value).flatMap(read)
    )

  /** A codec for a case class, as a record of its fields, or for a sealed trait or enum: as an Avro enum where every case is an object, and as a
    * union of its cases' records where not. Names come from the Scala names and packages, and [[AvroName]], [[AvroNamespace]], [[AvroDoc]],
    * [[AvroAlias]] and [[AvroNoDefault]] change them; a field's Scala default is its default in the schema.
    */
  inline def derived[A](using mirror: Mirror.Of[A]): AvroCodec[A] =
    inline mirror match
      case product: Mirror.ProductOf[A] =>
        val named = Meta.named[A]
        RecordCodec[A, product.MirroredElemTypes](
          FullName(named.namespace, named.name),
          named,
          Meta.fields[A],
          summonInline[Fields[product.MirroredElemTypes]],
          Meta.defaults[A],
          summonInline[Elements[A, product.MirroredElemTypes]].apply,
          product.fromTuple
        )
      case sum: Mirror.SumOf[A] =>
        val named = Meta.named[A]
        SumCodec[A](FullName(named.namespace, named.name), named, summonInline[Cases[A, sum.MirroredElemTypes]].cases)

  /** A field's default as its codec writes it. */
  private[avro] def encodeDefault[A](value: A)(using resolved: Resolved[A]): Either[String, AvroValue] = resolved.codec.encode(value)

/** The values of a case class's fields, as a tuple of their types. */
private[avro] trait Elements[A, T <: Tuple]:
  def apply(value: A): T

private[avro] object Elements:
  given [A <: Product](using product: Mirror.ProductOf[A]): Elements[A, product.MirroredElemTypes] = value => Tuple.fromProductTyped(value)

/** A case class's codec, writing its fields in order as a record's. */
private[avro] final class RecordCodec[A, T <: Tuple](
    name: FullName,
    named: Meta.Named,
    infos: List[Meta.Field],
    fields: Fields[T],
    defaults: List[Option[() => Either[String, AvroValue]]],
    toTuple: A => T,
    fromTuple: T => A
) extends AvroCodec[A]:
  override private[avro] def schemaWithin(seen: Set[FullName]): AvroSchema =
    if seen.contains(name) then AvroSchema.Named(name)
    else
      val inside  = seen + name
      val schemas = fields.schemas(inside)
      // A default its field's codec cannot write, as an instant past the largest timestamp, is left out.
      val avro =
        infos.lazyZip(schemas).lazyZip(defaults).map: (info, schema, default) =>
          val kept = default.filterNot(_ => info.noDefault).flatMap(_().toOption)
          AvroField(info.name, kept.fold(schema)(first(schema, _)), kept, info.doc, info.aliases)
      AvroSchema.Record(name, avro, named.doc, named.aliases.map(FullName.from(_, name.namespace)))

  /** The schema with the union branch `default` is a value of first, as Avro writes a union field's default in its first branch. */
  private def first(schema: AvroSchema, default: AvroValue): AvroSchema =
    schema match
      case AvroSchema.Union(branches) => branches.partition(AvroBinary.fits(_, default, AvroSchema.definitions(schema))) match
          case (fitting :: others, rest) => AvroSchema.Union(fitting :: others ++ rest)
          case _                         => schema
      case other => other

  override def encode(value: A): Either[String, AvroValue] =
    fields.encode(toTuple(value))
      .bimap(reason => s"record '${name.value}': $reason", values => AvroValue.Record(name, infos.map(_.name).zip(values).toVector))

  override def decode(value: AvroValue): Either[String, A] =
    value match
      case AvroValue.Record(_, values) => infos
          .traverse(info => values.collectFirst { case (info.name, read) => read }.toRight(s"no field '${info.name}'")).flatMap(fields.decode)
          .bimap(reason => s"record '${name.value}': $reason", fromTuple)
      case other => Left(s"expected a record '${name.value}', found $other")

/** A sealed trait's or enum's codec: an Avro enum of its cases' names where every case is an object, and a union of its cases' records where not. */
private[avro] final class SumCodec[A](name: FullName, named: Meta.Named, cases: List[Case[A]]) extends AvroCodec[A]:
  private val singletons = cases.traverse(_.singleton)

  /** The names each case's values are written under: its record's, or its own cases' where it is a sealed trait of its own. */
  private lazy val names: List[(Case[A], Set[FullName])] =
    cases.map: known =>
      known ->
        (known.codec.schema match
          case AvroSchema.Union(branches) => branches.flatMap(_.fullName).toSet
          case other                      => other.fullName.toSet)

  private def symbol(known: Case[A]): String = known.codec.schema.fullName.fold("")(_.name)

  override private[avro] def schemaWithin(seen: Set[FullName]): AvroSchema =
    singletons match
      case Some(_) => AvroSchema.Enum(name, cases.map(symbol), None, named.doc, named.aliases.map(FullName.from(_, name.namespace)))
      case None    => AvroSchema.Union(
          cases.flatMap: known =>
            known.codec.schemaWithin(seen) match
              case AvroSchema.Union(branches) => branches
              case other                      => List(other)
        )

  override def encode(value: A): Either[String, AvroValue] =
    cases.collectFirstSome(known => known.test.unapply(value).map(narrowed => (known, known.codec.encode(narrowed)))) match
      case Some((known, encoded)) => if singletons.isDefined then Right(AvroValue.Enum(name, symbol(known))) else encoded
      case None                   => Left(s"$value is none of the cases of '${name.value}'")

  override def decode(value: AvroValue): Either[String, A] =
    value match
      case AvroValue.Enum(_, read) if singletons.isDefined =>
        cases.find(symbol(_) == read).flatMap(_.singleton).toRight(s"'$read' is not a case of '${name.value}'")
      case AvroValue.Record(written, _) => names
          .collectFirst { case (known, owned) if owned.contains(written) => known.codec.decode(value).map(known.widen) }
          .getOrElse(Left(s"'${written.value}' is not a case of '${name.value}'"))
      case other => Left(s"expected a case of '${name.value}', found $other")

/** A codec found for a type where one is given, and derived where none is, made once it is first needed so that a type can hold itself. */
private[avro] final class Resolved[A](make: => AvroCodec[A]):
  lazy val codec: AvroCodec[A] = make

private[avro] object Resolved extends DerivedResolved:
  given found[A](using codec: => AvroCodec[A]): Resolved[A] = Resolved(codec)

private[avro] trait DerivedResolved:
  inline given derived[A](using mirror: Mirror.Of[A]): Resolved[A] = Resolved(AvroCodec.derived[A](using mirror))

/** The codecs of a case class's fields, whose types are `T`. */
private[avro] trait Fields[T <: Tuple]:
  def schemas(seen: Set[FullName]): List[AvroSchema]
  def encode(values: T): Either[String, List[AvroValue]]
  def decode(values: List[AvroValue]): Either[String, T]

private[avro] object Fields:
  given Fields[EmptyTuple] with
    override def schemas(seen: Set[FullName]): List[AvroSchema]              = Nil
    override def encode(values: EmptyTuple): Either[String, List[AvroValue]] = Right(Nil)
    override def decode(values: List[AvroValue]): Either[String, EmptyTuple] = Right(EmptyTuple)

  given [H, T <: Tuple](using head: Resolved[H], tail: Fields[T]): Fields[H *: T] with
    override def schemas(seen: Set[FullName]): List[AvroSchema]          = head.codec.schemaWithin(seen) :: tail.schemas(seen)
    override def encode(values: H *: T): Either[String, List[AvroValue]] = (head.codec.encode(values.head), tail.encode(values.tail)).mapN(_ :: _)
    override def decode(values: List[AvroValue]): Either[String, H *: T] =
      values match
        case value :: rest => (head.codec.decode(value), tail.decode(rest)).mapN(_ *: _)
        case Nil           => Left("fewer values than fields")

/** One case of a sealed trait or enum `A`: its codec, how a value of `A` is told to be of it, and its value where it is an object. */
private[avro] trait Case[A]:
  type Of
  def codec: AvroCodec[Of]
  def test: TypeTest[A, Of]
  def widen: Of => A
  def singleton: Option[A]

/** The cases of `A`, whose types are `T`. */
private[avro] final case class Cases[A, T <: Tuple](cases: List[Case[A]])

private[avro] object Cases:
  given [A]: Cases[A, EmptyTuple] = Cases(Nil)

  given [A, H, T <: Tuple](using resolved: Resolved[H], tested: TypeTest[A, H], up: H <:< A, single: Single[H], tail: Cases[A, T]): Cases[A, H *: T] =
    val known =
      new Case[A]:
        type Of = H
        override def codec: AvroCodec[H]  = resolved.codec
        override def test: TypeTest[A, H] = tested
        override def widen: H => A        = up
        override def singleton: Option[A] = single.value.map(up)
    Cases(known :: tail.cases)

/** A type's only value, where it has one. */
private[avro] final case class Single[A](value: Option[A])

private[avro] object Single extends NotSingle:
  given [A](using only: ValueOf[A]): Single[A] = Single(Some(only.value))

private[avro] trait NotSingle:
  given [A]: Single[A] = Single(None)
