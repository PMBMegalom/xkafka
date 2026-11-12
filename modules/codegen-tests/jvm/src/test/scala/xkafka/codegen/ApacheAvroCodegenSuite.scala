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

import example.shop.*
import fs2.Chunk
import munit.FunSuite
import org.apache.avro.{Schema, SchemaParser}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.idl.IdlReader
import org.apache.avro.io.DecoderFactory
import xkafka.avro.{AvroCodec, AvroFormat}

/** The generated classes' schemas and records, as Apache Avro reads them against the files they were generated from. */
class ApacheAvroCodegenSuite extends FunSuite:
  private val sources = Path.of("modules/codegen-tests/shared/src/main/avro")

  // Apache Avro's reading of every named type the files declare, by full name.
  private lazy val declared: Map[String, Schema] =
    val parser = SchemaParser()
    // Read so that the types the order's defaults are of come first.
    List("line.avsc", "status.avsc", "chain.avsc", "clash.avsc", "times.avsc", "order.avsc").map(sources.resolve).foreach(parser.parse)
    val idl = IdlReader().parse(sources.resolve("audit.avdl")).getNamedSchemas.asScala.values
    (parser.getParsedNamedSchemas.asScala ++ idl).map(schema => schema.getFullName -> schema).toMap

  private def same[A](name: String)(using AvroCodec[A]): Unit =
    assertEquals(SchemaParser.parseSingle(AvroFormat[A].schema.text).toString, declared(name).toString, name)

  test("the schema each generated class registers is the file's, as Apache Avro reads both"):
    same[Order]("example.shop.Order")
    same[Status]("example.shop.Status")
    same[Line]("example.shop.Line")
    same[Hash]("example.shop.Hash")
    same[example.chain.Link]("example.chain.Link")
    same[example.`type`.List]("example.type.List")
    same[example.audit.Entry]("example.audit.Entry")
    same[example.audit.Level]("example.audit.Level")
    same[example.times.Defaults]("example.times.Defaults")
    same[example.times.Nanos]("example.times.Nanos")

  test("Apache Avro reads a generated class's record with the file's schema"):
    val order =
      Order(id = "o-1", lines = List(Line("apple", 3)), hash = Hash(Chunk[Byte](1, 2, 3, 4)), payment = Card("4111"), status = Status.values_)
    val bytes  = AvroFormat[Order].writer(AvroFormat[Order].schema).flatMap(_(order)).toOption.get.toArray
    val reader = GenericDatumReader[GenericRecord](declared("example.shop.Order"))
    val read   = reader.read(null, DecoderFactory.get.binaryDecoder(bytes, null))
    assertEquals(read.get("id").toString, "o-1")
    assertEquals(read.get("status").toString, "values")
    assertEquals(read.get("payment").asInstanceOf[GenericRecord].get("number").toString, "4111")
    assertEquals(read.get("copy").toString, "2")
