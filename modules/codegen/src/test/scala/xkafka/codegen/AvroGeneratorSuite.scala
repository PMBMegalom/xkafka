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

/** What the generator writes from Avro files, and what it refuses, saying why. */
class AvroGeneratorSuite extends FunSuite:
  private val directory = FunFixture[Path](_ => Files.createTempDirectory("xkafka-codegen"), _ => ())

  private def files(in: Path, texts: (String, String)*): List[Path] = texts.toList.map((name, text) => Files.writeString(in.resolve(name), text))

  private def refused(in: Path, texts: (String, String)*): List[String] =
    AvroGenerator.generate(files(in, texts*)).fold(_.toList, sources => fail(s"expected a refusal, got ${sources.map(_.path)}"))

  private def record(fields: String, extra: String = ""): String =
    s"""{"type": "record", "name": "R", "namespace": "example"$extra, "fields": [$fields]}"""

  directory.test("each named type is a file in the directory of its namespace, and files may name each other's types in any order"): in =>
    val generated =
      AvroGenerator.generate(files(
        in,
        "a.avsc" -> record("""{"name": "kind", "type": "example.more.Kind", "default": "B"}"""),
        "b.avsc" -> """{"type": "enum", "name": "Kind", "namespace": "example.more", "symbols": ["A", "B"]}"""
      ))
    assertEquals(generated.map(_.map(_.path.toString).sorted), Right(List("example/R.scala", "example/more/Kind.scala")))
    assert(generated.exists(_.exists(_.text.contains("kind: example.more.Kind = example.more.Kind.B"))), generated.toString)

  directory.test("a time's default that is no time of day is refused, since the class could not hold it"): in =>
    def time(logical: String, base: String, default: Long) =
      record(s"""{"name": "t", "type": {"type": "$base", "logicalType": "$logical"}, "default": $default}""")
    assertEquals(
      refused(in, "a.avsc" -> time("time-millis", "int", -1)),
      List("example.R.t has a default xkafka cannot write: -1 is no time of day in milliseconds")
    )
    assertEquals(
      refused(in, "a.avsc" -> time("time-millis", "int", 86400000)),
      List("example.R.t has a default xkafka cannot write: 86400000 is no time of day in milliseconds")
    )
    assertEquals(
      refused(in, "a.avsc" -> time("time-micros", "long", 86400000000L)),
      List("example.R.t has a default xkafka cannot write: 86400000000 is no time of day in microseconds")
    )
    assert(AvroGenerator.generate(files(in, "a.avsc" -> time("time-millis", "int", 86399999))).isRight)

  directory.test("a type without a namespace is refused, since its package is made from it"): in =>
    assertEquals(
      refused(in, "a.avsc" -> """{"type": "record", "name": "R", "fields": []}"""),
      List("R has no namespace, which its Scala package is made from")
    )

  directory.test("what xkafka's schemas do not keep is refused: sort orders, properties, unknown logical types and a fixed type's documentation"):
    in =>
      assertEquals(
        refused(
          in,
          "a.avsc" -> record(
            """{"name": "a", "type": "int", "order": "descending"}, {"name": "b", "type": "int", "colour": "red"},
            |{"name": "c", "type": {"type": "string", "java-class": "Thing"}}, {"name": "d", "type": {"type": "int", "logicalType": "tenths"}}"""
              .stripMargin,
            """, "owner": "me""""
          ),
          "b.avsc" -> """{"type": "fixed", "name": "F", "namespace": "example", "size": 2, "doc": "two bytes"}"""
        ),
        List(
          "example.F is a fixed type with documentation, which xkafka does not keep",
          "example.R has the properties owner, which xkafka does not keep; example.R.a is sorted descending, which xkafka does not keep; " +
            "example.R.b has the properties colour, which xkafka does not keep; example.R.c has the properties java-class, which xkafka does not keep; " +
            "example.R.d has the logical type tenths, which xkafka does not know"
        )
      )

  directory.test("a union with null elsewhere than xkafka writes it is refused"): in =>
    assertEquals(
      refused(
        in,
        "a.avsc" -> record("""{"name": "a", "type": ["string", "null"]}, {"name": "b", "type": ["int", "string", "null"], "default": 1}""")
      ),
      List(
        "example.R.a has null at another place in its union than xkafka writes it, first or after the default's type; " +
          "example.R.b has null at another place in its union than xkafka writes it, first or after the default's type"
      )
    )

  directory.test("types xkafka has no codec for, and fields that would need two codecs of one Scala type, are refused"): in =>
    val problems =
      refused(
        in,
        "a.avsc" -> record(
          """{"name": "a", "type": {"type": "long", "logicalType": "local-timestamp-nanos"}}""" +
            """, {"name": "b", "type": ["int", {"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 1}]}""" +
            """, {"name": "c", "type": {"type": "fixed", "name": "Money", "size": 8, "logicalType": "decimal", "precision": 10, "scale": 2}}"""
        ),
        "b.avsc" -> """{"type": "record", "name": "T", "namespace": "example", "fields": [
                    |  {"name": "a", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                    |  {"name": "b", "type": {"type": "long", "logicalType": "timestamp-micros"}}]}""".stripMargin,
        "c.avsc" -> """{"type": "record", "name": "U", "namespace": "example", "fields": [
                    |  {"name": "a", "type": ["int", "string"]}, {"name": "b", "type": ["string", "int"]}]}""".stripMargin
      )
    assertEquals(
      problems,
      List(
        "example.Money is a decimal held in a fixed type, which xkafka has no codec for",
        "example.R.a is a local-timestamp-nanos, which xkafka has no codec for; " +
          "example.R.b has a decimal in a union of several types, which xkafka cannot give a precision there; " +
          "example.R.c is a decimal held in a fixed type, which xkafka has no codec for",
        "example.T has fields of java.time.Instant written with different precisions, which one record cannot hold",
        "example.U has unions of Int | String in different orders, which one record cannot hold"
      )
    )

  directory.test("files that are not .avsc or .avdl, that cannot be read, or that declare one type differently, are refused"): in =>
    assertEquals(refused(in, "a.json" -> "{}"), List(s"${in.resolve("a.json")} is neither an .avsc nor an .avdl file"))
    val broken = refused(in, "b.avsc" -> """{"type": "record", "name": "R", "namespace": "example"}""")
    assert(broken.head.startsWith(s"${in.resolve("b.avsc")}: "), broken.toString)
    assertEquals(
      refused(
        in,
        "c.avsc" -> """{"type": "enum", "name": "E", "namespace": "example", "symbols": ["A"]}""",
        "d.avdl" -> """@namespace("example") protocol P { enum E { A, B } }"""
      ),
      List("example.E is declared differently in two files")
    )
