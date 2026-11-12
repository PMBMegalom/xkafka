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

import java.nio.file.Path

import scala.jdk.CollectionConverters.*

import cats.data.NonEmptyList
import cats.syntax.all.*
import org.apache.avro.{Schema as JavaSchema, SchemaParser}
import org.apache.avro.idl.IdlReader
import xkafka.avro.{AvroSchemaJson, LogicalType}

/** A Scala source file, at `path` under the directory generated sources go in. */
final case class GeneratedFile(path: Path, text: String)

/** Scala sources generated from Avro schemas: a case class for each record, an enum for each enum, and a case class marked `AvroFixed` for each fixed
  * type, each in the package of its namespace, with its codec in its companion. Each codec must have the schema its type was generated from.
  */
object AvroGenerator:
  /** The sources of the named types that `files`, .avsc and .avdl files, declare, or why they cannot be generated. */
  def generate(files: List[Path]): Either[NonEmptyList[String], List[GeneratedFile]] =
    read(files).leftMap(NonEmptyList.one).flatMap: schemas =>
      val packages = schemas.groupMap(schema => Option(schema.getNamespace).filter(_.nonEmpty))(_.getName).view.mapValues(_.toSet).toMap
      schemas.traverse(schema => file(schema, packages).toValidatedNel).toEither

  private def read(files: List[Path]): Either[String, List[JavaSchema]] =
    val (idl, json) = files.partition(_.toString.endsWith(".avdl"))
    json.find(!_.toString.endsWith(".avsc")) match
      case Some(other) => Left(s"$other is neither an .avsc nor an .avdl file")
      case None        =>
        for
          read    <- parsed(Nil, json)
          fromIdl <-
            idl.flatTraverse(path => Either.catchNonFatal(IdlReader().parse(path).getNamedSchemas.asScala.values.toList).leftMap(failed(path)))
          schemas <- (read ++ fromIdl).groupBy(_.getFullName).toList.sortBy(_._1).traverse:
            case (name, first :: others) => if others.forall(_ == first) then Right(first) else Left(s"$name is declared differently in two files")
            case (name, Nil)             => Left(s"$name is declared nowhere")
        yield schemas

  /** The named types of `done` and `pending`, .avsc files, where `done` are read in their order. Avro checks a default as it reads it, so a file with
    * a default of a type another file declares is read after that file: each round reads the files that can be, until none are left.
    */
  @annotation.tailrec
  private def parsed(done: List[Path], pending: List[Path]): Either[String, List[JavaSchema]] =
    def parse(files: List[Path]) =
      Either.catchNonFatal:
        val parser = SchemaParser()
        files.foreach(parser.parse)
        parser.getParsedNamedSchemas.asScala.toList
    val readable = pending.filter(file => parse(done :+ file).isRight)
    pending.diff(readable) match
      case Nil                       => parse(done ++ readable).leftMap(message)
      case rest if readable.nonEmpty => parsed(done ++ readable, rest)
      case first :: _                => Left(parse(done :+ first).fold(failed(first), _ => s"$first cannot be read"))

  private def failed(path: Path)(error: Throwable): String = s"$path: ${message(error)}"

  private def message(error: Throwable): String = Option(error.getMessage).getOrElse(error.toString)

  private def file(schema: JavaSchema, packages: Map[Option[String], Set[String]]): Either[String, GeneratedFile] =
    val name = schema.getFullName
    for
      namespace <- Option(schema.getNamespace).filter(_.nonEmpty).toRight(s"$name has no namespace, which its Scala package is made from")
      _         <-
        unsupported(name, schema, top = true) match
          case Nil      => Right(())
          case problems => Left(problems.mkString("; "))
      avro <- AvroSchemaJson.parse(schema.toString).leftMap(reason => s"$name cannot be read: $reason")
      text <- AvroScala(namespace, packages.getOrElse(Some(namespace), Set.empty)).source(avro)
    yield GeneratedFile(Path.of(namespace.replace('.', '/'), s"${schema.getName}.scala"), text)

  private val logicalNames: Set[String] =
    Set(LogicalType.Decimal(1, 0), LogicalType.Uuid, LogicalType.Date, LogicalType.TimeMillis, LogicalType.TimeMicros, LogicalType.TimestampMillis)
      .map(_.name) ++ Set(LogicalType.TimestampMicros, LogicalType.TimestampNanos, LogicalType.LocalTimestampMillis, LogicalType.LocalTimestampMicros)
      .map(_.name) + LogicalType.LocalTimestampNanos.name

  /** What `schema`, at `where`, declares that xkafka's schemas do not keep, so the schema registered would not be the file's. Named types inside a
    * record are generated, and looked at, on their own.
    */
  private def unsupported(where: String, schema: JavaSchema, top: Boolean): List[String] =
    val logical = Option(schema.getProp("logicalType"))
    val kept    = if logical.isDefined then Set("logicalType", "precision", "scale") else Set.empty[String]
    val props   = schema.getObjectProps.asScala.keys.toList.filterNot(kept).sorted
    val own     =
      (if props.isEmpty then Nil else List(s"$where has the properties ${props.mkString(", ")}, which xkafka does not keep")) ++
        logical.filter(name => !logicalNames(name) || schema.getLogicalType == null)
          .map(name => s"$where has the logical type $name, which xkafka does not know").toList
    schema.getType match
      case JavaSchema.Type.RECORD if top =>
        own ++ schema.getFields.asScala.toList.flatMap: field =>
          val at    = s"$where.${field.name}"
          val order =
            Option
              .when(field.order != JavaSchema.Field.Order.ASCENDING)(s"$at is sorted ${field.order.toString.toLowerCase}, which xkafka does not keep")
          val fields = field.getObjectProps.asScala.keys.toList.sorted
          order.toList ++ (if fields.isEmpty then Nil else List(s"$at has the properties ${fields.mkString(", ")}, which xkafka does not keep")) ++
            unsupported(at, field.schema, top = false)
      case JavaSchema.Type.FIXED if top && Option(schema.getDoc).nonEmpty =>
        own :+ s"$where is a fixed type with documentation, which xkafka does not keep"
      case JavaSchema.Type.RECORD | JavaSchema.Type.ENUM | JavaSchema.Type.FIXED if !top => Nil
      case JavaSchema.Type.ARRAY => own ++ unsupported(where, schema.getElementType, top = false)
      case JavaSchema.Type.MAP   => own ++ unsupported(where, schema.getValueType, top = false)
      case JavaSchema.Type.UNION => schema.getTypes.asScala.toList.flatMap(unsupported(where, _, top = false))
      case _                     => own
