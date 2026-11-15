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

/** What the generator writes from JSON Schema files, and what it refuses, saying why. */
class JsonSchemaGeneratorSuite extends FunSuite:
  private val directory = FunFixture[Path](_ => Files.createTempDirectory("xkafka-codegen"), _ => ())

  private def files(in: Path, texts: (String, String)*): List[Path] = texts.toList.map((name, text) => Files.writeString(in.resolve(name), text))

  private def generated(in: Path, options: JsonSchemaOptions, texts: (String, String)*): List[GeneratedFile] =
    JsonSchemaGenerator.generate(files(in, texts*), options).fold(problems => fail(problems.toList.mkString("\n")), identity)

  private def refused(in: Path, texts: (String, String)*): List[String] =
    JsonSchemaGenerator.generate(files(in, texts*), JsonSchemaOptions(Some("example")))
      .fold(_.toList.map(_.stripPrefix(s"$in/")), sources => fail(s"expected a refusal, got ${sources.map(_.path)}"))

  private def record(properties: String, extra: String = ""): String =
    s"""{"title": "example.R", "type": "object", "properties": {$properties}$extra}"""

  directory.test("a title with a package names the type and its package; otherwise the type takes its title or file name, in the package given"):
    in =>
      val sources =
        generated(
          in,
          JsonSchemaOptions(Some("example.given")),
          "a.json"             -> record(""),
          "b.json"             -> """{"title": "Named", "type": "object", "properties": {}}""",
          "line-item.json"     -> """{"title": "A line", "type": "object", "properties": {}}""",
          "status.schema.json" -> """{"enum": ["a"]}"""
        )
      assertEquals(
        sources.map(_.path.toString).sorted,
        List("example/R.scala", "example/given/LineItem.scala", "example/given/Named.scala", "example/given/Status.scala")
      )

  directory.test("a file whose title names no package is refused where the generator is given none, as are two files of one type"): in =>
    val paths = files(in, "a.json" -> """{"title": "R", "type": "object"}""")
    assertEquals(
      JsonSchemaGenerator.generate(paths, JsonSchemaOptions()).left.map(_.toList),
      Left(List(s"$in/a.json names no package: give the schema a title such as com.example.Order, or the generator a package"))
    )
    assertEquals(
      JsonSchemaGenerator.generate(paths, JsonSchemaOptions(Some("not a.package"))).left.map(_.toList),
      Left(List("not a.package is not a package name"))
    )
    assertEquals(refused(in, "b.json" -> record(""), "c.json" -> record("")), List(s"example.R is generated from both $in/b.json and $in/c.json"))

  directory.test("numbers are Double, or BigDecimal where the generator is told so, and keywords that only check values are ignored"): in =>
    val schema =
      record(
        """"n": {"type": "number", "minimum": 0}, "s": {"type": "string", "pattern": "^a", "maxLength": 3}, "i": {"type": "integer", "enum": [1, 2]}"""
      )
    val doubles = generated(in, JsonSchemaOptions(), "a.json" -> schema).head.text
    assert(
      doubles.contains("n: Option[Double] = None") && doubles.contains("s: Option[String] = None") && doubles.contains("i: Option[Long] = None"),
      doubles
    )
    val decimals = generated(in, JsonSchemaOptions(bigDecimals = true), "a.json" -> schema).head.text
    assert(decimals.contains("n: Option[BigDecimal] = None"), decimals)

  directory.test("a reference to a definition that may be null is an Option of the definition's type"): in =>
    val text =
      generated(
        in,
        JsonSchemaOptions(),
        "a.json" -> record(
          """"d": {"$ref": "#/definitions/D"}""",
          ""","required": ["d"], "definitions": {"D": {"type": ["object", "null"], "properties": {}}}"""
        )
      ).head.text
    assert(text.contains("d: Option[R.D]"), text)

  directory.test("a required name no property declares is refused, since no value the class writes would have it"): in =>
    assertEquals(
      refused(in, "r.json" -> record(""""id": {"type": "string"}""", """, "required": ["id", "tenant"]""")),
      List("r.json: # requires tenant, which it declares no property of")
    )

  directory.test("keywords that combine schemas, and references to other files, are refused"): in =>
    assertEquals(
      refused(
        in,
        "a.json" -> record(
          """"a": {"allOf": [{"type": "string"}]}, "b": {"$ref": "other.json#/definitions/B"}, "c": {"$ref": "#/definitions/C", "type": "object"}"""
        ),
        "b.json" -> """{"title": "example.S", "type": "object", "patternProperties": {"^x": {"type": "string"}}}""",
        "c.json" ->
          """{"title": "example.T", "type": "object", "properties": {"a": {"type": "array", "items": [{"type": "string"}]}}, "additionalProperties": {"type": "string"}}"""
      ),
      List(
        "a.json: #/properties/a has allOf, which the generator does not read",
        "b.json: # has patternProperties, which the generator does not read",
        "c.json: # has both properties and a schema for others, which the generator does not read"
      )
    )
    assertEquals(
      refused(in, "d.json" -> record(""""b": {"$ref": "other.json#/definitions/B"}""")),
      List(
        "d.json: #/properties/b refers to other.json#/definitions/B, which is not a definition of the same file; the generator reads no other references yet"
      )
    )
    assertEquals(
      refused(in, "e.json" -> record(""""c": {"$ref": "#/definitions/C", "type": "object"}""")),
      List("e.json: #/properties/c has type beside its $ref, which the generator does not read")
    )
    assertEquals(
      refused(in, "f.json" -> record(""""a": {"type": "array", "items": [{"type": "string"}]}""")),
      List("f.json: #/properties/a has a list of items, one schema for each place, which the generator does not read")
    )

  directory.test("unions whose branches a written value could not be told apart by are refused"): in =>
    assertEquals(
      refused(
        in,
        "a.json" -> record(""""a": {"oneOf": [{"type": "string"}, {"type": "string", "format": "email"}]}"""),
        "b.json" ->
          """{"title": "example.S", "type": "object", "properties": {"a": {"anyOf": [{"type": "array", "items": {"type": "string"}}, {"type": "array", "items": {"type": "integer"}}]}}}""",
        "c.json" -> """{"title": "example.T", "type": "object", "properties": {"a": {"oneOf": [{}, {"type": "string"}]}}}""",
        "d.json" ->
          """{"title": "example.U", "type": "object", "properties": {"a": {"oneOf": [{"type": "integer"}, {"type": "number"}]}, "b": {"anyOf": [{"type": "integer"}, {"type": "number"}]}}}"""
      ),
      List(
        "a.json: #/properties/a has two branches of the same Scala class, which a value written could not be told to be of",
        "b.json: #/properties/a has two branches of the same Scala class, which a value written could not be told to be of",
        "c.json: #/properties/a has a branch of any value, which no other branch can be told from",
        "d.json: example.U has (Long | Double) both as a oneOf and as an anyOf, which one type cannot hold"
      )
    )

  directory
    .test("constants other than required text, files that are not objects, enums or unions of objects, and defaults of other types are refused"):
      in =>
        assertEquals(
          refused(
            in,
            "a.json" -> record(""""a": {"const": "x"}"""),
            "b.json" -> """{"title": "example.S", "type": "string"}""",
            "c.json" -> """{"title": "example.T", "enum": ["a", null]}""",
            "d.json" -> """{"title": "example.U", "oneOf": [{"type": "object"}, {"type": "string"}]}""",
            "e.json" -> """{"title": "example.V", "type": "object", "properties": {"n": {"type": "integer", "default": "one"}}}""",
            "f.json" -> """{"title": "example.W", "type": "object", "properties": {"e": {"enum": [1, "a"]}}}"""
          ),
          List(
            "a.json: #/properties/a is a constant that is not text, or not required, which the generator does not read",
            "b.json: the schema is not an object, an enum of text, or a oneOf or anyOf of objects, which the generator makes types of",
            "c.json: the schema is not an object, an enum of text, or a oneOf or anyOf of objects, which the generator makes types of",
            "d.json: #/oneOf has a branch that is not an object, which a sealed trait cannot have as a case",
            "e.json: n has a default the generator cannot write: \"one\" is not an integer",
            "f.json: #/properties/e is an enum of values other than text, without a type"
          )
        )

  directory.test("definitions that hold themselves without a type of their own, and types that would share a name, are refused"): in =>
    assertEquals(
      refused(
        in,
        "a.json" ->
          """{"title": "example.R", "type": "object", "properties": {"a": {"$ref": "#/definitions/A"}}, "definitions": {"A": {"type": "array", "items": {"$ref": "#/definitions/A"}}}}""",
        "b.json" ->
          """{"title": "example.S", "type": "object", "properties": {"line": {"type": "object", "properties": {}}, "d": {"$ref": "#/definitions/Line"}}, "definitions": {"Line": {"type": "object", "properties": {"x": {}}}}}""",
        "c.json" -> """{"title": "example.T", "type": "object", "properties": {"t": {"type": "object", "properties": {}}}}"""
      ),
      List(
        "a.json: #/definitions/A/items refers to #/definitions/A, which holds itself without a type of its own",
        "b.json: two types would be named example.S.Line",
        "c.json: example.T.T would be named as the schema's own type"
      )
    )
