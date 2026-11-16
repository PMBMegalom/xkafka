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

import scala.compiletime.{erasedValue, summonInline}
import scala.deriving.Mirror
import scala.quoted.*
import scala.reflect.{ClassTag, TypeTest}
import scala.util.Try

import cats.data.State
import cats.syntax.all.*
import fs2.Chunk
import xkafka.schemaregistry.{Decimals, DefaultOf, Meta, Shown}

/** How values of `A` are written as Avro values of a schema, and read back from them.
  *
  * Codecs are given for Scala's primitive types, text, bytes, options, collections, UUIDs and `java.time` types, and derived for case classes, sealed
  * traits and enums with `derives AvroCodec`. Other types are made from these with [[imap]] and [[emap]].
  */
trait AvroCodec[A]:
  self =>

  /** The schema, built with the named types defined so far, each defined where it is first met and named by its name alone after. */
  private[avro] def building: AvroCodec.Building[AvroSchema]

  private lazy val built: (AvroCodec.Defining, AvroSchema) = building.run(AvroCodec.Defining.empty).value

  /** The schema values are written with. */
  final def schema: AvroSchema = built._2

  /** Why no serde of this codec can be made: two of the types it writes given one name, a type in no namespace that its schema could only name as
    * another, or a schema it must match and does not.
    */
  private[avro] final def refusal: Option[String] = built._1.clashes.headOption.orElse(AvroSchemaJson.shadowed(schema)).orElse(mismatch)

  def encode(value: A): Either[String, AvroValue]

  /** `value` encoded inside `depth` records, which a record refuses past the depth reading takes, so a value too deep is refused before it could
    * exhaust the stack. A codec holding no records encodes as `encode` does.
    */
  private[avro] def encodeWithin(value: A, @scala.annotation.unused depth: Int): Either[String, AvroValue] = encode(value)

  /** The value `value` is, which was read with this codec's schema. */
  def decode(value: AvroValue): Either[String, A]

  /** How this codec's schema differs from the schema it must match, where [[matching]] made it. */
  private[avro] def mismatch: Option[String] = None

  /** This codec, which must have the schema `text` declares, such as the file its type was generated from: serdes of it fail with
    * `RegistryException.NoSchema` where its schema is another, and say how.
    */
  final def matching(text: String): AvroCodec[A] =
    // Rendered, each named type is defined where it is first used and named after, as a file may name it, so the two compare as text. They are
    // compared once a serde asks, since a type that holds itself has no schema yet while its codec is being made.
    lazy val problem =
      val own = AvroSchemaJson.render(schema)
      AvroSchemaJson.parse(text).map(AvroSchemaJson.render) match
        case Left(error)                        => Some(s"the schema to match cannot be read: $error")
        case Right(declared) if declared == own => None
        case Right(declared)                    => Some(s"the codec's schema is $own, not $declared")
    new AvroCodec[A]:
      override private[avro] def building: AvroCodec.Building[AvroSchema] = self.building
      override private[avro] def mismatch: Option[String]                 = problem
      override def encode(value: A): Either[String, AvroValue]            = self.encode(value)
      override private[avro] def encodeWithin(value: A, depth: Int)       = self.encodeWithin(value, depth)
      override def decode(value: AvroValue): Either[String, A]            = self.decode(value)

  /** A codec for `B`, written as the `A` that `to` makes of it and read as `from` makes of an `A`. */
  final def imap[B](from: A => B)(to: B => A): AvroCodec[B] = emap(value => Right(from(value)))(to)

  /** A codec for `B`, written as the `A` that `to` makes of it and read as `from` makes of an `A`, which may refuse it. */
  final def emap[B](from: A => Either[String, B])(to: B => A): AvroCodec[B] =
    new AvroCodec[B]:
      override private[avro] def building: AvroCodec.Building[AvroSchema] = self.building
      override def encode(value: B): Either[String, AvroValue]            = self.encode(to(value))
      override private[avro] def encodeWithin(value: B, depth: Int)       = self.encodeWithin(to(value), depth)
      override def decode(value: AvroValue): Either[String, B]            = self.decode(value).flatMap(from)

object AvroCodec:
  def apply[A](using codec: AvroCodec[A]): AvroCodec[A] = codec

  /** The named types a schema has defined so far as it is built, none for one still being defined, and the names two types were given. */
  private[avro] final case class Defining(defined: Map[FullName, Option[AvroSchema]], clashes: Vector[String], comparing: Boolean = false):
    /** The definitions complete so far, by which a default is placed in the branch of its union it is a value of. */
    def complete: Map[FullName, AvroSchema] = defined.collect { case (name, Some(schema)) => name -> schema }

  private[avro] object Defining:
    val empty: Defining = Defining(Map.empty, Vector.empty)

  private[avro] type Building[A] = State[Defining, A]

  /** The named type `name`, defined by `define` where it is first met and named by its name alone after. Met again, a type is compared with the first
    * one of its name: each refers to the named types inside it by their names, so the two are told apart at once, and two that differ are two types
    * given one name, which no schema can say.
    */
  private[avro] def named(name: FullName, define: => Building[AvroSchema]): Building[AvroSchema] =
    State.get[Defining].flatMap: sofar =>
      sofar.defined.get(name) match
        case None => State.modify[Defining](d => d.copy(defined = d.defined.updated(name, None))) *>
            define.flatTap(made => State.modify[Defining](d => d.copy(defined = d.defined.updated(name, Some(made)))))
        case Some(None)                       => State.pure(AvroSchema.Named(name))
        case Some(Some(_)) if sofar.comparing => State.pure(AvroSchema.Named(name))
        case Some(Some(defined))              =>
          // Made again only to compare, the types inside it named by their names without comparing them too, so a comparison looks one level deep.
          val again = define.runA(sofar.copy(comparing = true)).value
          val same  = referring(defined) == referring(again)
          State.modify[Defining](d => if same then d else d.copy(clashes = d.clashes :+ s"two types are named '${name.value}'"))
            .as(AvroSchema.Named(name))

  /** `schema` with the named types inside it named by their names alone. */
  private def referring(schema: AvroSchema): AvroSchema =
    def inside(schema: AvroSchema): AvroSchema =
      schema match
        case named @ (AvroSchema.Record(_, _, _, _) | AvroSchema.Enum(_, _, _, _, _) | AvroSchema.Fixed(_, _, _)) => named.fullName
            .fold(named)(AvroSchema.Named(_))
        case AvroSchema.Logical(LogicalType.FixedDecimal(name, _, _, _, _)) => AvroSchema.Named(name)
        case AvroSchema.Union(branches)                                     => AvroSchema.Union(branches.map(inside))
        case AvroSchema.Array(items)                                        => AvroSchema.Array(inside(items))
        case AvroSchema.Map(values)                                         => AvroSchema.Map(inside(values))
        case other                                                          => other
    // Defaults are left out, and with them the order a default gives its union's branches, since they depend on which types are complete when the
    // type is built, which differs between its first build and a later one.
    def unordered(schema: AvroSchema): AvroSchema =
      schema match
        case AvroSchema.Union(branches) => AvroSchema.Union(branches.sortBy(AvroSchemaJson.render))
        case other                      => other
    schema match
      case record: AvroSchema.Record => record
          .copy(fields = record.fields.map(field => field.copy(schema = unordered(inside(field.schema)), default = None)))
      case other => other

  /** A codec of `schema`, writing with `encode` and reading with `decode`. */
  def instance[A](avro: AvroSchema, encode: A => Either[String, AvroValue], decode: AvroValue => Either[String, A]): AvroCodec[A] =
    val (write, read) = (encode, decode)
    new AvroCodec[A]:
      override private[avro] def building: Building[AvroSchema] = State.pure(avro)
      override def encode(value: A): Either[String, AvroValue]  = write(value)
      override def decode(value: AvroValue): Either[String, A]  = read(value)

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

  /** Bytes as an array, copied on the way in and out so that changing the array later changes nothing written or read. */
  given AvroCodec[Array[Byte]] = AvroCodec[Chunk[Byte]].imap(_.toArray)(value => Chunk.array(value.clone))

  /** A value that may be absent, as a union of null and the value's schema, null first, so a field of it may default to null. One whose value may be
    * null already, as an option's, is refused, since its none and the value's would both be written as null.
    */
  given [A](using inner: AvroCodec[A]): AvroCodec[Option[A]] with
    override private[avro] def building: Building[AvroSchema] =
      inner.building.flatMap: schema =>
        val twice =
          schema == AvroSchema.Null ||
            (schema match
              case AvroSchema.Union(branches) => branches.contains(AvroSchema.Null);
              case _                          => false)
        val clash = "an option holding an option cannot be told from none, as null is a value of both"
        State.modify[Defining](defining => defining.copy(clashes = defining.clashes :+ clash)).whenA(twice).as:
          schema match
            case AvroSchema.Union(branches) => AvroSchema.Union(AvroSchema.Null :: branches.filterNot(_ == AvroSchema.Null))
            case other                      => AvroSchema.Union(List(AvroSchema.Null, other))
    override def encode(value: Option[A]): Either[String, AvroValue]                                 = encodeWithin(value, 0)
    override private[avro] def encodeWithin(value: Option[A], depth: Int): Either[String, AvroValue] =
      value.fold(Right(AvroValue.Null))(inner.encodeWithin(_, depth))
    override def decode(value: AvroValue): Either[String, Option[A]] =
      value match
        case AvroValue.Null => Right(None)
        case other          => inner.decode(other).map(Some(_))

  private def array[A, C](inner: AvroCodec[A])(items: C => Iterable[A], make: Vector[A] => C): AvroCodec[C] =
    new AvroCodec[C]:
      override private[avro] def building: Building[AvroSchema]                                = inner.building.map(AvroSchema.Array(_))
      override def encode(value: C): Either[String, AvroValue]                                 = encodeWithin(value, 0)
      override private[avro] def encodeWithin(value: C, depth: Int): Either[String, AvroValue] =
        items(value).toVector.traverse(inner.encodeWithin(_, depth)).map(AvroValue.Array(_))
      override def decode(value: AvroValue): Either[String, C] =
        value match
          case AvroValue.Array(values) => values.traverse(inner.decode).map(make)
          case other                   => Left(s"expected an array, found ${Shown(other)}")

  given [A](using inner: AvroCodec[A]): AvroCodec[List[A]]   = array(inner)(identity, _.toList)
  given [A](using inner: AvroCodec[A]): AvroCodec[Vector[A]] = array(inner)(identity, identity)
  given [A](using inner: AvroCodec[A]): AvroCodec[Seq[A]]    = array(inner)(identity, identity)
  given [A](using inner: AvroCodec[A]): AvroCodec[Set[A]]    = array(inner)(identity, _.toSet)

  /** Values by text keys, as an Avro map. */
  given [A](using inner: AvroCodec[A]): AvroCodec[Map[String, A]] with
    override private[avro] def building: Building[AvroSchema]                                             = inner.building.map(AvroSchema.Map(_))
    override def encode(value: Map[String, A]): Either[String, AvroValue]                                 = encodeWithin(value, 0)
    override private[avro] def encodeWithin(value: Map[String, A], depth: Int): Either[String, AvroValue] =
      value.toVector.traverse((key, item) => inner.encodeWithin(item, depth).map(key -> _)).map(AvroValue.Map(_))
    override def decode(value: AvroValue): Either[String, Map[String, A]] =
      value match
        case AvroValue.Map(entries) => entries.traverse((key, item) => inner.decode(item).map(key -> _)).map(_.toMap)
        case other                  => Left(s"expected a map, found ${Shown(other)}")

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
          .leftMap(_ => s"${Shown(value)} has more than $scale digits after the point")
          .filterOrElse(_.precision <= precision, s"${Shown(value)} has more than $precision digits")
          .map(scaled => Chunk.array(scaled.unscaledValue.toByteArray))
    val read =
      (bytes: Chunk[Byte]) =>
        Either.cond(bytes.nonEmpty, BigDecimal(new java.math.BigDecimal(new java.math.BigInteger(bytes.toArray), scale)), "a decimal of no bytes")
    instance(
      AvroSchema.Logical(LogicalType.Decimal(precision, scale)),
      value => write(value).flatMap(AvroCodec[Chunk[Byte]].encode),
      value => AvroCodec[Chunk[Byte]].decode(value).flatMap(read)
    )

  /** A codec for a case class, as a record of its fields, or for a sealed trait or enum: as an Avro enum where every case is an object, and as a
    * union of its cases' records where not. Names come from the Scala names and packages, and [[SchemaName]], [[SchemaNamespace]], [[SchemaDoc]],
    * [[SchemaAlias]] and [[SchemaNoDefault]] change them; a field's Scala default is its default in the schema. A case class marked [[AvroFixed]] is
    * a `fixed`.
    */
  inline def derived[A](using mirror: Mirror.Of[A]): AvroCodec[A] =
    inline mirror match
      case product: Mirror.ProductOf[A] =>
        val named   = Meta.named[A]
        val name    = FullName(named.namespace, named.name)
        val fields  = Fields.of[A, product.MirroredElemTypes]
        val toTuple = summonInline[Elements[A, product.MirroredElemTypes]].apply
        FixedSize.of[A] match
          case Some(size) => FixedCodec[A, product.MirroredElemTypes](name, size, named, fields, toTuple, product.fromTuple)
          case None       => RecordCodec[A, product.MirroredElemTypes](
              name,
              named,
              Meta.fields[A],
              fields,
              Meta.defaults[A, AvroCodec, AvroValue],
              toTuple,
              product.fromTuple
            )
      case sum: Mirror.SumOf[A] =>
        val named = Meta.named[A]
        SumCodec[A](FullName(named.namespace, named.name), named, EnumDefault.of[A], summonInline[Cases[A, sum.MirroredElemTypes]].cases)

  /** Values of a union type, such as `Cash | Card`, as an Avro union of the types of `T` in their order: `AvroCodec.union[(Cash, Card)]`. A value is
    * written as the first of the types it is of, and read as the first whose schema fits it, so a record is read by its name.
    */
  inline def union[T <: Tuple]: AvroCodec[Tuple.Union[T]] = UnionCodec(Branch.all[Tuple.Union[T], T])

  /** A decimal number of the precision and scale `SchemaDecimal` gives its field. */
  given [P <: Int, S <: Int](using precision: ValueOf[P], scale: ValueOf[S]): AvroCodec[Decimals.Decimal[P, S]] =
    Decimals.lift(decimal(precision.value, scale.value))

  /** A field's default as its codec writes it, which derived schemas take. */
  given [A](using resolved: Resolved[A]): DefaultOf[AvroCodec, AvroValue, A] = value => resolved.codec.encode(value)

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
  override private[avro] def building: AvroCodec.Building[AvroSchema] =
    AvroCodec.named(
      name,
      (fields.building, State.get[AvroCodec.Defining]).mapN: (schemas, sofar) =>
        // A default its field's codec cannot write, as an instant past the largest timestamp, is left out, and so is one no branch of its union is
        // known to take yet, as one of the record it is in.
        // One whose value holds a union's later branch inside it is left out too, as Avro takes a nested union's default from its first branch.
        val avro =
          infos.lazyZip(schemas).lazyZip(evaluated).map: (info, schema, default) =>
            val placed =
              default.flatMap: kept =>
                first(schema, kept, sofar.complete)
                  .filter(reordered => AvroBinary.placeable(reordered, kept, sofar.complete ++ AvroSchema.definitions(reordered))).map(_ -> kept)
            AvroField(info.name, placed.fold(schema)(_._1), placed.map(_._2), info.doc, info.aliases)
        AvroSchema.Record(name, avro, named.doc, named.aliases.map(FullName.from(_, name.namespace)))
    )

  /** Each field's default, evaluated once, so that every build of the schema gives a field the same one: none where it is marked `SchemaNoDefault`,
    * or where its codec cannot write it.
    */
  private lazy val evaluated: List[Option[AvroValue]] =
    infos.lazyZip(defaults).map((info, default) => default.filterNot(_ => info.noDefault).flatMap(_().toOption)).toList

  /** The schema with the union branch `default` is a value of first, as Avro writes a union field's default in its first branch, if a branch is known
    * to take it.
    */
  private def first(schema: AvroSchema, default: AvroValue, known: Map[FullName, AvroSchema]): Option[AvroSchema] =
    schema match
      case AvroSchema.Union(branches) => branches.partition(AvroBinary.fits(_, default, known ++ AvroSchema.definitions(schema))) match
          case (fitting :: others, rest) => Some(AvroSchema.Union(fitting :: others ++ rest))
          case _                         => None
      case other => Some(other)

  override def encode(value: A): Either[String, AvroValue] = encodeWithin(value, 0)

  override private[avro] def encodeWithin(value: A, depth: Int): Either[String, AvroValue] =
    if depth >= AvroBinary.MaxDepth then Left(s"the value is nested more than ${AvroBinary.MaxDepth} deep")
    else
      fields.encode(toTuple(value), depth + 1)
        .bimap(reason => s"record '${name.value}': $reason", values => AvroValue.Record(name, infos.map(_.name).zip(values).toVector))

  override def decode(value: AvroValue): Either[String, A] =
    value match
      case AvroValue.Record(_, values) =>
        // Each field's value found once by its name, the first of a name as before.
        val byName = values.reverseIterator.toMap
        infos.traverse(info => byName.get(info.name).toRight(s"no field '${info.name}'")).flatMap(fields.decode)
          .bimap(reason => s"record '${name.value}': $reason", fromTuple)
      case other => Left(s"expected a record '${name.value}', found ${Shown(other)}")

/** A case class's codec as a `fixed` of `size` bytes, the bytes of its one field. */
private[avro] final class FixedCodec[A, T <: Tuple](
    name: FullName,
    size: Int,
    named: Meta.Named,
    field: Fields[T],
    toTuple: A => T,
    fromTuple: T => A
) extends AvroCodec[A]:
  override private[avro] def building: AvroCodec.Building[AvroSchema] =
    AvroCodec.named(name, State.pure(AvroSchema.Fixed(name, size, named.aliases.map(FullName.from(_, name.namespace)))))

  override def encode(value: A): Either[String, AvroValue] =
    field.encode(toTuple(value), 0).flatMap:
      case List(AvroValue.Bytes(bytes)) if bytes.size == size => Right(AvroValue.Fixed(name, bytes))
      case List(AvroValue.Bytes(bytes))                       => Left(s"fixed '${name.value}': ${bytes.size} bytes, not $size")
      case other                                              => Left(s"fixed '${name.value}': expected bytes, found ${Shown(other)}")

  override def decode(value: AvroValue): Either[String, A] =
    value match
      case AvroValue.Fixed(_, bytes) => field.decode(List(AvroValue.Bytes(bytes))).map(fromTuple)
      case other                     => Left(s"expected a fixed '${name.value}', found ${Shown(other)}")

/** A union type's codec, as an Avro union of its branches. */
private[avro] final class UnionCodec[U](branches: List[Branch[U]]) extends AvroCodec[U]:
  // A branch that is a union itself, as a sealed trait of records is, gives its branches, since Avro has no union inside a union.
  override private[avro] def building: AvroCodec.Building[AvroSchema] =
    branches.traverse(_.codec.building).flatMap: schemas =>
      val flat =
        schemas.flatMap:
          case AvroSchema.Union(inner) => inner
          case other                   => List(other)
      // Avro takes a union's branch by its type, so two of one type could not be told apart.
      val clash = AvroSchemaJson.twice(flat).map(kind => s"a union holds '$kind' twice")
      State.modify[AvroCodec.Defining](defining => defining.copy(clashes = defining.clashes ++ clash)).as(AvroSchema.Union(flat))

  override def encode(value: U): Either[String, AvroValue] = encodeWithin(value, 0)

  override private[avro] def encodeWithin(value: U, depth: Int): Either[String, AvroValue] =
    branches.collectFirstSome(branch => branch.tag.unapply(value).map(branch.codec.encodeWithin(_, depth)))
      .getOrElse(Left(s"${Shown(value)} is of none of the union's types"))

  override def decode(value: AvroValue): Either[String, U] =
    branches.find(branch => AvroBinary.fits(branch.codec.schema, value, AvroSchema.definitions(branch.codec.schema)))
      .toRight(s"${Shown(value)} is of none of the union's types").flatMap(branch => branch.codec.decode(value).map(branch.widen))

/** One type of a union type `U`: its codec, and how a value of `U` is told to be of it. */
private[avro] trait Branch[U]:
  type Of
  def codec: AvroCodec[Of]
  def tag: ClassTag[Of]
  def widen: Of => U

private[avro] object Branch:
  inline def all[U, T <: Tuple]: List[Branch[U]] =
    inline erasedValue[T] match
      case _: EmptyTuple     => Nil
      case _: (head *: tail) => of[U, head](summonInline[Resolved[head]], summonInline[ClassTag[head]], summonInline[head <:< U]) :: all[U, tail]

  private def of[U, H](resolved: Resolved[H], told: ClassTag[H], up: H <:< U): Branch[U] =
    new Branch[U]:
      type Of = H
      override def codec: AvroCodec[H] = resolved.codec
      override def tag: ClassTag[H]    = told
      override def widen: H => U       = up

/** The size [[AvroFixed]] gives a case class, which must have one field of bytes and no documentation, since an Avro `fixed` has none. */
private[avro] object FixedSize:
  inline def of[A]: Option[Int] = ${ sizeOf[A] }

  private def sizeOf[A: Type](using Quotes): Expr[Option[Int]] =
    import quotes.reflect.*
    val tpe    = TypeRepr.of[A]
    val symbol = tpe.typeSymbol
    symbol.annotations.collectFirst:
      case annotation @ Apply(Select(New(tpt), _), arguments) if tpt.tpe.typeSymbol.fullName == "xkafka.avro.AvroFixed" => (annotation, arguments)
    match
      case None                          => '{ None }
      case Some((annotation, arguments)) =>
        val size =
          arguments match
            case List(argument) => xkafka.schemaregistry.Meta.constantNumber(argument)
                .getOrElse(report.errorAndAbort("@AvroFixed takes a constant number", annotation.pos))
            case _ => report.errorAndAbort("@AvroFixed takes a constant number", annotation.pos)
        val fields =
          symbol.primaryConstructor.paramSymss.flatten.filterNot(_.isTypeParam).map(field => tpe.memberType(symbol.fieldMember(field.name)))
        val bytes =
          fields match
            case List(field) => field =:= TypeRepr.of[Chunk[Byte]] || field =:= TypeRepr.of[Array[Byte]]
            case _           => false
        if size < 0 then report.errorAndAbort(s"@AvroFixed($size) on ${symbol.name}: the size must not be negative", annotation.pos)
        else if !bytes then
          report.errorAndAbort(s"@AvroFixed on ${symbol.name}, which must have one field, of fs2.Chunk[Byte] or Array[Byte]", annotation.pos)
        else if symbol.annotations.exists(_.tpe.typeSymbol.fullName == "xkafka.schemaregistry.SchemaDoc") then
          report.errorAndAbort(s"@SchemaDoc on ${symbol.name}, which @AvroFixed makes a fixed: an Avro fixed has no documentation", annotation.pos)
        else '{ Some(${ Expr(size) }) }

/** The symbol [[AvroEnumDefault]] gives an enum or sealed trait, which must be one of its cases, all of them objects. */
private[avro] object EnumDefault:
  inline def of[A]: Option[String] = ${ defaultOf[A] }

  private def defaultOf[A: Type](using Quotes): Expr[Option[String]] =
    import quotes.reflect.*
    val symbol                      = TypeRepr.of[A].typeSymbol
    def renamed(of: Symbol): String = xkafka.schemaregistry.Meta.text(of.annotations, "SchemaName").headOption.getOrElse(of.name.stripSuffix("$"))
    symbol.annotations.collectFirst:
      case annotation @ Apply(Select(New(tpt), _), arguments) if tpt.tpe.typeSymbol.fullName == "xkafka.avro.AvroEnumDefault" =>
        (annotation, arguments)
    match
      case None                          => '{ None }
      case Some((annotation, arguments)) =>
        val default =
          arguments match
            case List(argument) => xkafka.schemaregistry.Meta.constantText(argument)
                .getOrElse(report.errorAndAbort("@AvroEnumDefault takes constant text", annotation.pos))
            case _ => report.errorAndAbort("@AvroEnumDefault takes constant text", annotation.pos)
        val cases = symbol.children
        if !cases.forall(child => child.isTerm || child.flags.is(Flags.Module)) then
          report.errorAndAbort(s"@AvroEnumDefault on ${symbol.name}, whose cases are not all objects, so it is not an Avro enum", annotation.pos)
        else if !cases.map(renamed).contains(default) then
          report.errorAndAbort(
            s"@AvroEnumDefault(\"$default\") on ${symbol.name}, which has no case $default: ${cases.map(renamed).mkString(", ")}",
            annotation.pos
          )
        else '{ Some(${ Expr(default) }) }

/** A sealed trait's or enum's codec: an Avro enum of its cases' names where every case is an object, and a union of its cases' records where not. */
private[avro] final class SumCodec[A](name: FullName, named: Meta.Named, default: Option[String], cases: List[Case[A]]) extends AvroCodec[A]:
  // derived is inline, so programs compiled with xkafka 0.7 call this constructor themselves.
  def this(name: FullName, named: Meta.Named, cases: List[Case[A]]) = this(name, named, None, cases)

  private val singletons = cases.traverse(_.singleton)

  /** The names each case's values are written under: its record's, or its own cases' where it is a sealed trait of its own. */
  private lazy val names: List[(Case[A], Set[FullName])] =
    cases.map: known =>
      known ->
        (known.codec.schema match
          case AvroSchema.Union(branches) => branches.flatMap(_.fullName).toSet
          case other                      => other.fullName.toSet)

  private def symbol(known: Case[A]): String = known.codec.schema.fullName.fold("")(_.name)

  override private[avro] def building: AvroCodec.Building[AvroSchema] =
    singletons match
      case Some(_) =>
        val symbols = cases.map(symbol)
        // An Avro enum's symbols are its values, so two alike would read as one.
        val twice = symbols.diff(symbols.distinct).headOption.map(symbol => s"enum '${name.value}' holds the symbol '$symbol' twice")
        AvroCodec.named(name, State.pure(AvroSchema.Enum(name, symbols, default, named.doc, named.aliases.map(FullName.from(_, name.namespace)))))
          .flatTap(_ => State.modify[AvroCodec.Defining](defining => defining.copy(clashes = defining.clashes ++ twice)))
      // Two cases of one name, though alike, would be one branch twice, which Avro could not tell apart.
      case None => cases.traverse(_.codec.building).flatMap: schemas =>
          val flat =
            schemas.flatMap:
              case AvroSchema.Union(branches) => branches
              case other                      => List(other)
          val clash = AvroSchemaJson.twice(flat).map(kind => s"'${name.value}' has two cases named '$kind'")
          State.modify[AvroCodec.Defining](defining => defining.copy(clashes = defining.clashes ++ clash)).as(AvroSchema.Union(flat))

  override def encode(value: A): Either[String, AvroValue] = encodeWithin(value, 0)

  override private[avro] def encodeWithin(value: A, depth: Int): Either[String, AvroValue] =
    cases.collectFirstSome(known => known.test.unapply(value).map(narrowed => (known, known.codec.encodeWithin(narrowed, depth)))) match
      case Some((known, encoded)) => if singletons.isDefined then Right(AvroValue.Enum(name, symbol(known))) else encoded
      case None                   => Left(s"${Shown(value)} is none of the cases of '${name.value}'")

  private def byName(written: FullName, value: AvroValue): Either[String, A] =
    names.collectFirst { case (known, owned) if owned.contains(written) => known.codec.decode(value).map(known.widen) }
      .getOrElse(Left(s"'${written.value}' is not a case of '${name.value}'"))

  override def decode(value: AvroValue): Either[String, A] =
    value match
      case AvroValue.Enum(_, read) if singletons.isDefined =>
        cases.find(symbol(_) == read).flatMap(_.singleton).toRight(s"'$read' is not a case of '${name.value}'")
      // A case that is a record, or an enum or fixed of its own, is found by the name its value is written under.
      case AvroValue.Record(written, _) => byName(written, value)
      case AvroValue.Enum(written, _)   => byName(written, value)
      case AvroValue.Fixed(written, _)  => byName(written, value)
      case other                        => Left(s"expected a case of '${name.value}', found ${Shown(other)}")

/** A codec found for a type where one is given, and derived where none is, made once it is first needed so that a type can hold itself. */
private[avro] final class Resolved[A](make: => AvroCodec[A]):
  lazy val codec: AvroCodec[A] = make

private[avro] object Resolved extends DerivedResolved:
  given found[A](using codec: => AvroCodec[A]): Resolved[A] = Resolved(codec)

private[avro] trait DerivedResolved:
  inline given derived[A](using mirror: Mirror.Of[A]): Resolved[A] = Resolved(AvroCodec.derived[A](using mirror))

/** The codecs of a case class's fields, whose types are `T`. */
private[avro] trait Fields[T <: Tuple]:
  def building: AvroCodec.Building[List[AvroSchema]]

  /** The fields' values encoded inside `depth` records. */
  def encode(values: T, depth: Int): Either[String, List[AvroValue]]
  def decode(values: List[AvroValue]): Either[String, T]

private[avro] object Fields:
  /** The codecs of `A`'s fields, whose types are `T`, where the `BigDecimal`s of a field marked `SchemaDecimal` are decimals of its precision. */
  inline def of[A, T <: Tuple]: Fields[T] = ${ fieldsOf[A, T] }

  private def fieldsOf[A: Type, T <: Tuple: Type](using Quotes): Expr[Fields[T]] =
    import quotes.reflect.*
    def elements(tuple: TypeRepr): List[TypeRepr] =
      tuple.dealias.simplified match
        case AppliedType(_, List(head, tail)) => head :: elements(tail)
        case _                                => Nil
    val names   = Meta.fieldNamesOf(TypeRepr.of[A])
    val types   = elements(TypeRepr.of[T])
    val derived =
      types.lazyZip(Meta.decimalsOf(TypeRepr.of[A]).padTo(types.size, None)).lazyZip(names.padTo(types.size, "")).map: (tpe, decimal, name) =>
        decimal.fold(tpe)(Meta.withDecimal(tpe, _, name))
    val tuple = derived.foldRight(TypeRepr.of[EmptyTuple])((head, tail) => TypeRepr.of[*:].appliedTo(List(head, tail)))
    Implicits.search(TypeRepr.of[Fields].appliedTo(tuple)) match
      case found: ImplicitSearchSuccess   => '{ ${ found.tree.asExpr }.asInstanceOf[Fields[T]] }
      case missing: ImplicitSearchFailure => report.errorAndAbort(missing.explanation)
  given Fields[EmptyTuple] with
    override def building: AvroCodec.Building[List[AvroSchema]]                          = State.pure(Nil)
    override def encode(values: EmptyTuple, depth: Int): Either[String, List[AvroValue]] = Right(Nil)
    override def decode(values: List[AvroValue]): Either[String, EmptyTuple]             = Right(EmptyTuple)

  given [H, T <: Tuple](using head: Resolved[H], tail: Fields[T]): Fields[H *: T] with
    override def building: AvroCodec.Building[List[AvroSchema]]                      = (head.codec.building, tail.building).mapN(_ :: _)
    override def encode(values: H *: T, depth: Int): Either[String, List[AvroValue]] =
      (head.codec.encodeWithin(values.head, depth), tail.encode(values.tail, depth)).mapN(_ :: _)
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
