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

import cats.syntax.all.*
import xkafka.avro.{AvroBinary, AvroField, AvroSchema, AvroSchemaJson, AvroValue, FullName, LogicalType}

/** The Scala source of a named Avro type in `namespace`, whose package also holds the types named `neighbours`. */
private[codegen] final class AvroScala(namespace: String, neighbours: Set[String]):
  def source(schema: AvroSchema): Either[String, String] =
    schema match
      case record: AvroSchema.Record                                      => recordSource(record)
      case enumeration: AvroSchema.Enum                                   => enumSource(enumeration)
      case fixed: AvroSchema.Fixed                                        => Right(fixedSource(fixed))
      case AvroSchema.Logical(LogicalType.FixedDecimal(name, _, _, _, _)) =>
        Left(s"${name.value} is a decimal held in a fixed type, which xkafka has no codec for")
      case other => Left(s"${AvroSchemaJson.render(other)} is not a record, an enum or a fixed type")

  /** A Scala type and what its codec needs from the companion it is derived in. */
  private final case class Typed(scala: String, unions: List[List[String]], codecs: Map[String, String], decimals: Set[(Int, Int)])

  private object Typed:
    def plain(scala: String): Typed = Typed(scala, Nil, Map.empty, Set.empty)

  private def recordSource(record: AvroSchema.Record): Either[String, String] =
    val name        = record.name.value
    val definitions = AvroSchema.definitions(record)
    for
      fields <- record.fields.traverse(field => fieldSource(name, field, definitions).toValidatedNel).toEither.leftMap(_.toList.mkString("; "))
      _      <-
        fields.map(_._1).groupBy(identity).collectFirst { case (scala, copies) if copies.size > 1 => scala }.toLeft(())
          .leftMap(scala => s"$name has two fields named $scala in Scala")
      typed = fields.map(_._3)
      codecs <-
        typed.flatMap(_.codecs.toList).groupMap(_._1)(_._2).toList.sortBy(_._1).traverse:
          case (scala, List(one)) => Right(Option.when(one.nonEmpty)(s"private given _root_.xkafka.avro.AvroCodec[$scala] = $one"))
          case (scala, choices) if choices.distinct.size == 1 =>
            Right(Option.when(choices.head.nonEmpty)(s"private given _root_.xkafka.avro.AvroCodec[$scala] = ${choices.head}"))
          case (scala, _) =>
            Left(s"$name has fields of ${scala.replace("_root_.", "")} written with different precisions, which one record cannot hold")
      unions = typed.flatMap(_.unions).distinct
      _ <-
        unions.groupBy(_.toSet).collectFirst { case (_, orders) if orders.size > 1 => orders.head.mkString(" | ") }.toLeft(())
          .leftMap(types => s"$name has unions of ${types.replace("_root_.", "")} in different orders, which one record cannot hold")
    yield
      // Givens of union types are named, since Scala names them all alike.
      val givens =
        codecs.flatten ++ unions.zipWithIndex.map: (types, index) =>
          s"private given avroUnion${index + 1}: _root_.xkafka.avro.AvroCodec[${types.mkString(" | ")}] = _root_.xkafka.avro.AvroCodec.union[(${types
              .mkString(", ")})]"
      val scala      = Names.typeName(record.name.name)
      val parameters = if fields.isEmpty then "()" else fields.map(_._2).mkString("(\n    ", ",\n    ", "\n)")
      header(record.name) + annotations(record.doc, record.aliases) + s"final case class $scala$parameters\n\n" + companion(scala, record, givens)

  /** A field's Scala name, its parameter, and its type. */
  private def fieldSource(record: String, field: AvroField, definitions: Map[FullName, AvroSchema]): Either[String, (String, String, Typed)] =
    val where = s"$record.${field.name}"
    for
      typed   <- typeOf(field.schema, where)
      decimal <-
        typed.decimals.toList match
          case Nil        => Right(None)
          case one :: Nil => Right(Some(one))
          case _          => Left(s"$where holds decimals of different precisions, which one field cannot")
      _ <- unionOrder(field, where)
      // xkafka's codec leaves out a default it could not write in the field's own schema, which would then not be the file's.
      default <-
        field.default.traverse: value =>
          val placed =
            Either.cond(
              AvroBinary.placeable(field.schema, value, definitions),
              (),
              "it holds a value of a union's branch other than its first, the one xkafka writes a default in"
            )
          (placed *> literal(field.schema, value, definitions)).leftMap(reason => s"$where has a default xkafka cannot write: $reason")
    yield
      val (scala, renamed) = Names.field(field.name)
      val marks            =
        renamed.map(original => s"@_root_.xkafka.schemaregistry.SchemaName(${Names.quote(original)})").toList ++
          field.doc.map(doc => s"@_root_.xkafka.schemaregistry.SchemaDoc(${Names.quote(doc)})") ++
          field.aliases.map(alias => s"@_root_.xkafka.schemaregistry.SchemaAlias(${Names.quote(alias)})") ++
          decimal.map((precision, scale) => s"@_root_.xkafka.schemaregistry.SchemaDecimal($precision, $scale)")
      (scala, (marks :+ s"$scala${Names.colon(scala)}${typed.scala}${default.fold("")(value => s" = $value")}").mkString(" "), typed)

  /** Fails where xkafka would write a field's union in another order than the file's: with null first, or with the branch its default is of. */
  private def unionOrder(field: AvroField, where: String): Either[String, Unit] =
    field.schema match
      case AvroSchema.Union(branches) if branches.contains(AvroSchema.Null) =>
        val others   = branches.filterNot(_ == AvroSchema.Null)
        val expected =
          field.default.filterNot(_ == AvroValue.Null).fold(AvroSchema.Null :: others)(_ => others.head :: AvroSchema.Null :: others.tail)
        Either
          .cond(expected == branches, (), s"$where has null at another place in its union than xkafka writes it, first or after the default's type")
          .flatMap(_ => nestedUnions(field.schema, where))
      case schema => nestedUnions(schema, where)

  /** Fails where a union inside a field's lists or maps has null other than first, the place xkafka writes it in a union with no default. */
  private def nestedUnions(schema: AvroSchema, where: String): Either[String, Unit] =
    def inner(schema: AvroSchema): Either[String, Unit] =
      schema match
        case AvroSchema.Union(branches) if branches.contains(AvroSchema.Null) && branches.head != AvroSchema.Null =>
          Left(s"$where has null at another place in a union inside it than xkafka writes it, first")
        case AvroSchema.Union(branches) => branches.traverse_(nestedUnions(_, where))
        case other                      => nestedUnions(other, where)
    schema match
      case AvroSchema.Array(items)    => inner(items)
      case AvroSchema.Map(values)     => inner(values)
      case AvroSchema.Union(branches) => branches.traverse_(nestedUnions(_, where))
      case _                          => Right(())

  private def typeOf(schema: AvroSchema, where: String): Either[String, Typed] =
    schema match
      case AvroSchema.Null                   => Left(s"$where is null alone, which has no Scala type")
      case AvroSchema.Boolean                => Right(Typed.plain(Names.builtin("Boolean", neighbours)))
      case AvroSchema.Int                    => Right(Typed.plain(Names.builtin("Int", neighbours)))
      case AvroSchema.Long                   => Right(Typed.plain(Names.builtin("Long", neighbours)))
      case AvroSchema.Float                  => Right(Typed.plain(Names.builtin("Float", neighbours)))
      case AvroSchema.Double                 => Right(Typed.plain(Names.builtin("Double", neighbours)))
      case AvroSchema.String                 => Right(Typed.plain(Names.builtin("String", neighbours)))
      case AvroSchema.Bytes                  => Right(Typed.plain(s"_root_.fs2.Chunk[${Names.builtin("Byte", neighbours)}]"))
      case AvroSchema.Record(name, _, _, _)  => Right(Typed.plain(qualified(name)))
      case AvroSchema.Enum(name, _, _, _, _) => Right(Typed.plain(qualified(name)))
      case AvroSchema.Fixed(name, _, _)      => Right(Typed.plain(qualified(name)))
      case AvroSchema.Named(name)            => Right(Typed.plain(qualified(name)))
      case AvroSchema.Array(items) => typeOf(items, where).map(inner => inner.copy(scala = s"${Names.builtin("List", neighbours)}[${inner.scala}]"))
      case AvroSchema.Map(values)  => typeOf(values, where)
          .map(inner => inner.copy(scala = s"${Names.builtin("Map", neighbours)}[${Names.builtin("String", neighbours)}, ${inner.scala}]"))
      case AvroSchema.Union(branches) =>
        val others = branches.filterNot(_ == AvroSchema.Null)
        val inner  =
          others match
            case Nil        => Left(s"$where is a union of null alone, which has no Scala type")
            case one :: Nil => typeOf(one, where)
            case several    =>
              if several.exists { case AvroSchema.Logical(LogicalType.Decimal(_, _)) => true; case _ => false } then
                Left(s"$where has a decimal in a union of several types, which xkafka cannot give a precision there")
              else
                several.traverse(typeOf(_, where)).map: types =>
                  Typed(
                    types.map(_.scala).mkString("(", " | ", ")"),
                    types.map(_.scala) :: types.flatMap(_.unions),
                    types.flatMap(_.codecs).toMap,
                    types.flatMap(_.decimals).toSet
                  )
        if others.size == branches.size then inner
        else inner.map(typed => typed.copy(scala = s"${Names.builtin("Option", neighbours)}[${typed.scala}]"))
      case AvroSchema.Logical(logicalType) => logical(logicalType, where)

  /** A logical type's Scala type, and the codec it needs given where it is not the one xkafka gives that type. */
  private def logical(logicalType: LogicalType, where: String): Either[String, Typed] =
    def needing(scala: String, codec: String) = Right(Typed(scala, Nil, Map(scala -> codec), Set.empty))
    logicalType match
      case LogicalType.Decimal(precision, scale)   => Right(Typed(Names.builtin("BigDecimal", neighbours), Nil, Map.empty, Set((precision, scale))))
      case LogicalType.FixedDecimal(_, _, _, _, _) => Left(s"$where is a decimal held in a fixed type, which xkafka has no codec for")
      case LogicalType.Uuid                        => Right(Typed.plain("_root_.java.util.UUID"))
      case LogicalType.Date                        => Right(Typed.plain("_root_.java.time.LocalDate"))
      case LogicalType.TimeMillis                  => needing("_root_.java.time.LocalTime", "_root_.xkafka.avro.AvroCodec.localTimeMillis")
      case LogicalType.TimeMicros                  => needing("_root_.java.time.LocalTime", "")
      case LogicalType.TimestampMillis             => needing("_root_.java.time.Instant", "")
      case LogicalType.TimestampMicros             => needing("_root_.java.time.Instant", "_root_.xkafka.avro.AvroCodec.instantMicros")
      case LogicalType.TimestampNanos              => needing("_root_.java.time.Instant", "_root_.xkafka.avro.AvroCodec.instantNanos")
      case LogicalType.LocalTimestampMillis        => needing("_root_.java.time.LocalDateTime", "_root_.xkafka.avro.AvroCodec.localDateTimeMillis")
      case LogicalType.LocalTimestampMicros        => needing("_root_.java.time.LocalDateTime", "")
      case LogicalType.LocalTimestampNanos         => Left(s"$where is a local-timestamp-nanos, which xkafka has no codec for")

  /** The Scala expression of `value`, a default of `schema`. */
  private def literal(schema: AvroSchema, value: AvroValue, definitions: Map[FullName, AvroSchema]): Either[String, String] =
    def bytes(chunk: fs2.Chunk[Byte]) = chunk.toList.mkString(s"_root_.fs2.Chunk[${Names.builtin("Byte", neighbours)}](", ", ", ")")
    (schema, value) match
      case (AvroSchema.Named(name), _) => definitions.get(name).toRight(s"${name.value} is not defined").flatMap(literal(_, value, definitions))
      case (AvroSchema.Union(branches), AvroValue.Null) if branches.contains(AvroSchema.Null) => Right(Names.builtin("None", neighbours))
      case (AvroSchema.Union(branches), _) => branches.find(_ != AvroSchema.Null).toRight("a union without a type")
          .flatMap(literal(_, value, definitions)).map: inner =>
            if branches.contains(AvroSchema.Null) then s"${Names.builtin("Some", neighbours)}($inner)" else inner
      case (AvroSchema.Boolean, AvroValue.Boolean(boolean))                 => Right(boolean.toString)
      case (AvroSchema.Int, AvroValue.Int(int))                             => Right(int.toString)
      case (AvroSchema.Long, AvroValue.Long(long))                          => Right(s"${long}L")
      case (AvroSchema.Float, AvroValue.Float(float)) if float.isFinite     => Right(s"${float}f")
      case (AvroSchema.Double, AvroValue.Double(double)) if double.isFinite => Right(double.toString)
      case (AvroSchema.String, AvroValue.String(text))                      => Right(Names.quote(text))
      case (AvroSchema.Bytes, AvroValue.Bytes(chunk))                       => Right(bytes(chunk))
      case (AvroSchema.Array(items), AvroValue.Array(values))               => values.toList.traverse(literal(items, _, definitions))
          .map(_.mkString(s"${Names.builtin("List", neighbours)}(", ", ", ")"))
      case (AvroSchema.Map(inner), AvroValue.Map(entries)) => entries.toList
          .traverse((key, item) => literal(inner, item, definitions).map(item => s"${Names.quote(key)} -> $item"))
          .map(_.mkString(s"${Names.builtin("Map", neighbours)}(", ", ", ")"))
      case (AvroSchema.Enum(name, _, _, _, _), AvroValue.Enum(_, symbol))       => Right(s"${qualified(name)}.${symbolOf(symbol)._1}")
      case (AvroSchema.Fixed(name, _, _), AvroValue.Fixed(_, chunk))            => Right(s"${qualified(name)}(${bytes(chunk)})")
      case (AvroSchema.Record(name, fields, _, _), AvroValue.Record(_, values)) => values.toList.traverse: (field, item) =>
          fields.find(_.name == field).toRight(s"${name.value} has no field $field").flatMap(known => literal(known.schema, item, definitions))
            .map(item => s"${Names.field(field)._1} = $item")
        .map(_.mkString(s"${qualified(name)}(", ", ", ")"))
      case (AvroSchema.Logical(logicalType), _) => logicalLiteral(logicalType, value)
      case _                                    => Left(s"$value is not a value of ${AvroSchemaJson.render(schema)}")

  /** The milliseconds of a day, past which no time of day goes. */
  private val DayMillis = 86400000

  private def logicalLiteral(logicalType: LogicalType, value: AvroValue): Either[String, String] =
    val epoch = "_root_.java.time.Instant.EPOCH"
    (logicalType, value) match
      // Each default is made as the class is, so one that cannot be would fail each time it is taken.
      case (LogicalType.Decimal(_, _), AvroValue.Bytes(chunk)) if chunk.isEmpty => Left("a decimal of no bytes is no number")
      case (LogicalType.Decimal(_, scale), AvroValue.Bytes(chunk))              =>
        val digits = chunk.toList.mkString(s"${Names.builtin("Array", neighbours)}[${Names.builtin("Byte", neighbours)}](", ", ", ")")
        Right(s"${Names.builtin("BigDecimal", neighbours)}(${Names.builtin("BigInt", neighbours)}($digits), $scale)")
      case (LogicalType.Uuid, AvroValue.String(text)) => Either.cond(
          scala.util.Try(java.util.UUID.fromString(text)).isSuccess,
          s"_root_.java.util.UUID.fromString(${Names.quote(text)})",
          s"'$text' is no UUID"
        )
      case (LogicalType.Date, AvroValue.Int(days)) => Right(s"_root_.java.time.LocalDate.ofEpochDay(${days}L)")
      // A default past a day would make a time the class cannot hold, failing each time the default is taken.
      case (LogicalType.TimeMillis, AvroValue.Int(millis)) => Either.cond(
          millis >= 0 && millis < DayMillis,
          s"_root_.java.time.LocalTime.ofNanoOfDay(${millis.toLong * 1000000L}L)",
          s"$millis is no time of day in milliseconds"
        )
      case (LogicalType.TimeMicros, AvroValue.Long(micros)) => Either.cond(
          micros >= 0L && micros < DayMillis * 1000L,
          s"_root_.java.time.LocalTime.ofNanoOfDay(${micros * 1000L}L)",
          s"$micros is no time of day in microseconds"
        )
      case (LogicalType.TimestampMillis, AvroValue.Long(millis)) => Right(s"_root_.java.time.Instant.ofEpochMilli(${millis}L)")
      case (LogicalType.TimestampMicros, AvroValue.Long(micros)) => Right(s"$epoch.plus(${micros}L, _root_.java.time.temporal.ChronoUnit.MICROS)")
      case (LogicalType.TimestampNanos, AvroValue.Long(nanos))   => Right(s"$epoch.plus(${nanos}L, _root_.java.time.temporal.ChronoUnit.NANOS)")
      case (LogicalType.LocalTimestampMillis, AvroValue.Long(millis)) =>
        Right(s"_root_.java.time.LocalDateTime.ofInstant(_root_.java.time.Instant.ofEpochMilli(${millis}L), _root_.java.time.ZoneOffset.UTC)")
      case (LogicalType.LocalTimestampMicros, AvroValue.Long(micros)) => Right(
          s"_root_.java.time.LocalDateTime.ofInstant($epoch.plus(${micros}L, _root_.java.time.temporal.ChronoUnit.MICROS), _root_.java.time.ZoneOffset.UTC)"
        )
      case _ => Left(s"$value is not a value of the logical type ${logicalType.name}")

  private def enumSource(enumeration: AvroSchema.Enum): Either[String, String] =
    val cases = enumeration.symbols.map(symbolOf)
    cases.map(_._1).groupBy(identity).collectFirst { case (scala, copies) if copies.size > 1 => scala } match
      case Some(scala) => Left(s"${enumeration.name.value} has two symbols named $scala in Scala")
      case None        =>
        val scala = Names.typeName(enumeration.name.name)
        val lines =
          cases.map((name, renamed) =>
            renamed.fold(s"  case $name")(original => s"  @_root_.xkafka.schemaregistry.SchemaName(${Names.quote(original)}) case $name")
          )
        Right(
          header(enumeration.name) + annotations(enumeration.doc, enumeration.aliases) +
            enumeration.default.fold("")(symbol => s"@_root_.xkafka.avro.AvroEnumDefault(${Names.quote(symbol)})\n") +
            s"enum $scala${Names.opening(scala)}\n${lines.mkString("\n")}\n\n" + companion(scala, enumeration, Nil)
        )

  private def fixedSource(fixed: AvroSchema.Fixed): String =
    val scala = Names.typeName(fixed.name.name)
    header(fixed.name) + annotations(None, fixed.aliases) + s"@_root_.xkafka.avro.AvroFixed(${fixed.size})\n" +
      s"final case class $scala(bytes: _root_.fs2.Chunk[${Names.builtin("Byte", neighbours)}])\n\n" + companion(scala, fixed, Nil)

  private def header(name: FullName): String =
    s"""// Generated by xkafka-codegen from the Avro schema ${name.value}. Changes made here are lost when it is generated again.
       |
       |package ${namespace.split('.').map(Names.term).mkString(".")}
       |
       |""".stripMargin

  private def annotations(doc: Option[String], aliases: List[FullName]): String =
    (doc.map(text => s"@_root_.xkafka.schemaregistry.SchemaDoc(${Names.quote(text)})\n").toList ++
      aliases.map(alias => s"@_root_.xkafka.schemaregistry.SchemaAlias(${Names.quote(alias.value)})\n")).mkString

  /** A symbol's case, renamed where it would clash with the enum's members or its companion's schema, with the name the schema gives it. */
  private def symbolOf(symbol: String): (String, Option[String]) =
    if symbol == "avroSchema" then ("avroSchema_", Some(symbol)) else Names.symbol(symbol)

  private def companion(scala: String, schema: AvroSchema, givens: List[String]): String =
    val extra = givens.map(line => s"  $line\n").mkString
    s"""object $scala${Names.opening(scala)}
       |  /** The Avro schema $scala was generated from, which its codec must have. */
       |  val avroSchema: _root_.scala.Predef.String = ${Names.text(AvroSchemaJson.render(schema))}
       |
       |$extra${
        if extra.isEmpty then "" else "\n"
      }  given _root_.xkafka.avro.AvroCodec[$scala] = _root_.xkafka.avro.AvroCodec.derived[$scala].matching(avroSchema)
       |""".stripMargin

  /** How code in this type's package names the type `name`. */
  private def qualified(name: FullName): String =
    if name.namespace.contains(namespace) then Names.typeName(name.name)
    // From the root, since a package inside this one, as example.audit in example, would hide the package audit.
    else ("_root_" :: name.namespace.toList.flatMap(_.split('.')).map(Names.term) ++ List(Names.typeName(name.name))).mkString(".")
