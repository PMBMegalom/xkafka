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

import cats.data.NonEmptyList
import cats.syntax.all.*
import io.circe.{jawn, Json}

/** How classes are generated from JSON Schema files: in `packageName` where a schema's title names no package, and with JSON Schema's numbers as
  * `BigDecimal` where `bigDecimals`, and as `Double` where not.
  */
final case class JsonSchemaOptions(packageName: Option[String] = None, bigDecimals: Boolean = false)

/** Scala sources generated from JSON Schema files: a case class for each object, an enum for each enum of text, and a sealed trait for a file that is
  * a `oneOf` or `anyOf` of objects, with codecs that register each file's schema as it is.
  */
object JsonSchemaGenerator:
  /** The sources of the types that `files`, JSON Schema files, describe, or why they cannot be generated. */
  def generate(files: List[Path], options: JsonSchemaOptions): Either[NonEmptyList[String], List[GeneratedFile]] =
    files.traverse(file => root(file, options).toValidatedNel).toEither.flatMap: roots =>
      roots.groupBy(found => s"${found.packageName}.${found.name}").toList.sortBy(_._1).collectFirst {
        case (name, copies) if copies.size > 1 => s"$name is generated from both ${copies.map(_.file).mkString(" and ")}"
      } match
        case Some(problem) => Left(NonEmptyList.one(problem))
        case None          =>
          val packages = roots.groupMap(_.packageName)(_.name).view.mapValues(_.toSet).toMap
          roots.traverse: found =>
            JsonScala(found, options.bigDecimals, packages.getOrElse(found.packageName, Set.empty)).source
              .bimap(reason => s"${found.file}: $reason", GeneratedFile(Path.of(found.packageName.replace('.', '/'), s"${found.name}.scala"), _))
              .toValidatedNel
          .toEither

  /** A file's schema, and the package and name of the type generated from it. */
  private[codegen] final case class Root(file: Path, packageName: String, name: String, schema: Json)

  private def qualified(name: String): Boolean = name.split('.').toList.forall(Names.isIdentifier) && !name.endsWith(".")

  private def root(file: Path, options: JsonSchemaOptions): Either[String, Root] =
    for
      text   <- Either.catchNonFatal(Files.readString(file)).leftMap(error => s"$file cannot be read: $error")
      schema <- jawn.parse(text).leftMap(error => s"$file is not JSON: ${error.message}")
      title = schema.hcursor.get[String]("title").toOption
      named <-
        title.filter(name => name.contains('.') && qualified(name)) match
          case Some(full) => Right((full.substring(0, full.lastIndexOf('.')), full.substring(full.lastIndexOf('.') + 1)))
          case None       =>
            for
              packageName <-
                options.packageName.toRight(s"$file names no package: give the schema a title such as com.example.Order, or the generator a package")
              _ <- Either.cond(qualified(packageName), (), s"$packageName is not a package name")
              base = file.getFileName.toString.stripSuffix(".json").stripSuffix(".schema")
              name <- title.filter(Names.isIdentifier).orElse(Names.pascal(base)).toRight(s"$file has no title, and its name makes no Scala name")
            yield (packageName, name)
    yield Root(file, named._1, named._2, schema)
