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

package example

import munit.FunSuite
import xkafka.avro.{AvroCodec, AvroSchemaJson}
import xkafka.schemaregistry.{SchemaDoc, SchemaName}

@SchemaName("Visit")
final case class Visited(
    page: String,
    @SchemaDoc("How long it took")
    millis: Long = 0,
    referrer: Option[String] = None
) derives AvroCodec

sealed trait Shape derives AvroCodec
final case class Circle(radius: Double) extends Shape
case object Dot                         extends Shape

/** Derivation as a program outside xkafka's packages does it, where only xkafka's public API is visible. */
class OutsideSuite extends FunSuite:
  test("codecs derive outside xkafka's packages, with defaults, annotations and sealed traits"):
    assertEquals(
      AvroSchemaJson.render(AvroCodec[Visited].schema),
      """{"type":"record","name":"Visit","namespace":"example","fields":[{"name":"page","type":"string"},""" +
        """{"name":"millis","type":"long","doc":"How long it took","default":0},{"name":"referrer","type":["null","string"],"default":null}]}"""
    )
    assertEquals(AvroCodec[Shape].encode(Dot).flatMap(AvroCodec[Shape].decode), Right(Dot))
