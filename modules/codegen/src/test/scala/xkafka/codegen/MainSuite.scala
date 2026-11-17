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

import munit.FunSuite

/** What the program generates from files of both formats together. */
class MainSuite extends FunSuite:
  private val directory = FunFixture[Path](_ => Files.createTempDirectory("xkafka-codegen"), _ => ())

  private def files(in: Path, texts: (String, String)*): List[Path] = texts.toList.map((name, text) => Files.writeString(in.resolve(name), text))

  private def avro(name: String): String = s"""{"type": "record", "name": "$name", "namespace": "example", "fields": []}"""

  directory.test("a type of one format named as a Scala type does not hide that type from the other format's code in the same package"): in =>
    val optional = """{"title": "example.Order", "type": "object", "properties": {"note": {"type": "string"}}}"""
    val named    = """{"type": "record", "name": "Holder", "namespace": "example", "fields": [{"name": "kept", "type": ["null", "string"]}]}"""
    val written  = Main.generate(files(in, "option.avsc" -> avro("Option"), "order.json" -> optional, "holder.avsc" -> named), JsonSchemaOptions())
    val order    = written.toOption.flatMap(_.find(_.path.toString == "example/Order.scala")).map(_.text)
    assert(order.exists(_.contains("_root_.scala.Option[")), order)

  directory.test("types of either format that would be written to one file are refused, as are files whose names differ only in case"): in =>
    val json = """{"title": "example.Same", "type": "object", "properties": {"a": {"type": "string"}}}"""
    assertEquals(
      Main.generate(files(in, "same.avsc" -> avro("Same"), "same.json" -> json), JsonSchemaOptions()).left.map(_.toList),
      Left(List("2 types would be written to example/Same.scala"))
    )
    assertEquals(
      Main.generate(files(in, "upper.avsc" -> avro("Same"), "lower.avsc" -> avro("same")), JsonSchemaOptions()).left.map(_.toList),
      Left(List("2 types would be written to example/Same.scala and example/same.scala, which are one file where case is ignored"))
    )
