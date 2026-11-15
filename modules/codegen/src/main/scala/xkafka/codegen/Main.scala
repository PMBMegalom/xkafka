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

import java.nio.file.{Files, Path}

import scala.jdk.CollectionConverters.*

import cats.data.NonEmptyList
import cats.effect.{ExitCode, IO, IOApp, Resource}
import cats.syntax.all.*

/** Writes the Scala sources of Avro and JSON Schema files: the first argument is the directory they go in, the others .avsc, .avdl and .json files,
  * or directories whose such files, at any depth, are read. They may be preceded by `--json-package`, then the package of the types of JSON Schema
  * files whose titles name none, and by `--json-big-decimals`, for JSON Schema's numbers to be `BigDecimal`.
  */
object Main extends IOApp:
  private val extensions = List(".avsc", ".avdl", ".json")

  override def run(arguments: List[String]): IO[ExitCode] =
    options(arguments, JsonSchemaOptions()) match
      case Some((json, output :: inputs)) if inputs.nonEmpty =>
        inputs.flatTraverse(input => schemas(Path.of(input))).flatMap(files => IO.blocking(generate(files, json))).flatMap:
          case Left(problems) => problems.traverse_(problem => IO.consoleForIO.errorln(problem)).as(ExitCode.Error)
          case Right(sources) => sources.traverse_(source => write(Path.of(output).resolve(source.path), source.text)).as(ExitCode.Success)
      case _ => IO.consoleForIO.errorln(
          "usage: xkafka-codegen [--json-package <package>] [--json-big-decimals] <output directory> <.avsc, .avdl and .json files, or directories of them>"
        ).as(ExitCode.Error)

  private def options(arguments: List[String], json: JsonSchemaOptions): Option[(JsonSchemaOptions, List[String])] =
    arguments match
      case "--json-package" :: name :: rest       => options(rest, json.copy(packageName = Some(name)))
      case "--json-big-decimals" :: rest          => options(rest, json.copy(bigDecimals = true))
      case option :: _ if option.startsWith("--") => None
      case rest                                   => Some((json, rest))

  private[codegen] def generate(files: List[Path], json: JsonSchemaOptions): Either[NonEmptyList[String], List[GeneratedFile]] =
    val (schemas, avro) = files.partition(_.toString.endsWith(".json"))
    // A type of either format hides a Scala type of its name from the other's code in its package, so each is told the other's names.
    val fromAvro = if avro.isEmpty then Right(Nil) else AvroGenerator.generate(avro, JsonSchemaGenerator.names(schemas, json))
    val fromJson = JsonSchemaGenerator.generate(schemas, json, if avro.isEmpty then Map.empty else AvroGenerator.names(avro))
    (fromAvro.toValidated, fromJson.toValidated).mapN(_ ++ _).toEither.flatMap(apart)

  /** The files, unless two would be one: one type's would replace the other's, as where an Avro and a JSON Schema type have one name, or where two
    * names differ only in case, which macOS and Windows take to be one file.
    */
  private def apart(files: List[GeneratedFile]): Either[NonEmptyList[String], List[GeneratedFile]] =
    val clashes =
      files.groupBy(_.path.toString.toLowerCase).values.filter(_.size > 1).toList.map: group =>
        group.map(_.path.toString).distinct.sorted match
          case List(one) => s"${group.size} types would be written to $one"
          case paths     => s"${group.size} types would be written to ${paths.mkString(" and ")}, which are one file where case is ignored"
    NonEmptyList.fromList(clashes.sorted).toLeft(files)

  /** `input`, a file, or the schema files in `input`, a directory, in the order of their paths. */
  private def schemas(input: Path): IO[List[Path]] =
    IO.blocking(Files.isDirectory(input)).ifM(
      Resource.fromAutoCloseable(IO.blocking(Files.walk(input))).use: paths =>
        IO.blocking(paths.iterator.asScala.filter(path => extensions.exists(path.toString.endsWith)).toList.sorted),
      IO.pure(List(input))
    )

  private def write(path: Path, text: String): IO[Unit] =
    IO.blocking(Files.createDirectories(path.getParent)) *> IO.blocking(Files.writeString(path, text)).void
