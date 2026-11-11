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

import cats.data.NonEmptyList
import cats.effect.IO
import munit.CatsEffectSuite
import xkafka.{Headers, Topic}
import xkafka.schemaregistry.{FakeRegistry, Part, RegistryException, RegistrySettings, Schema, SchemaDoc, SchemaNamespace, SchemaRegistry, SchemaType}

@SchemaNamespace("example.web") @SchemaDoc("A visit")
final case class Visit(
    @SchemaDoc("The visit's id")
    id: String,
    pages: Int = 1
) derives AvroCodec

/** Codecs that must have the schema of the file their type was generated from. */
class AvroMatchingSuite extends CatsEffectSuite:
  private val base     = "http://registry:8081"
  private val settings = RegistrySettings.from(NonEmptyList.one(base)).toOption.get
  private val topic    = Topic.from("visits").toOption.get

  private def registry: IO[SchemaRegistry[IO]] = FakeRegistry().flatMap(fake => SchemaRegistry.of(settings, fake.transport(base)))

  private val file =
    """{"type": "record", "name": "Visit", "namespace": "example.web", "doc": "A visit",
      | "fields": [{"name": "id", "type": "string", "doc": "The visit's id"}, {"name": "pages", "type": "int", "default": 1}]}""".stripMargin

  private def noSchema[A](outcome: Either[Throwable, A]): String =
    outcome match
      case Left(error: RegistryException.NoSchema) => error.reason
      case other                                   => fail(s"expected no schema, got $other")

  test("a codec with the schema it must match writes and reads, and registers that schema"):
    given AvroCodec[Visit] = AvroCodec.derived[Visit].matching(file)
    for
      client  <- registry
      serde   <- AvroSerde.of[IO, Visit](client, Part.Value)
      written <- serde.serializer.serialize(topic, Headers.empty, Visit("v-1", 3))
      read    <- serde.deserializer.deserialize(topic, Headers.empty, written)
    yield
      assertEquals(read, Visit("v-1", 3))
      assertEquals(AvroFormat[Visit].schema, Schema(AvroSchemaJson.parse(file).map(AvroSchemaJson.render).toOption.get, SchemaType.Avro))

  test("a codec whose schema is another fails to make serdes, saying how the two differ"):
    given AvroCodec[Visit] = AvroCodec.derived[Visit].matching(file.replace("\"default\": 1", "\"default\": 2"))
    val expected           =
      """the codec's schema is {"type":"record","name":"Visit","namespace":"example.web","doc":"A visit","fields":[{"name":"id","type":"string","doc":"The visit's id"},{"name":"pages","type":"int","default":1}]}, """ +
        """not {"type":"record","name":"Visit","namespace":"example.web","doc":"A visit","fields":[{"name":"id","type":"string","doc":"The visit's id"},{"name":"pages","type":"int","default":2}]}"""
    for outcome <- registry.flatMap(AvroSerde.of[IO, Visit](_, Part.Value)).attempt yield
      assertEquals(noSchema(outcome), expected)
      assertEquals(AvroFormat[Visit].writer(AvroFormat[Visit].schema).map(_ => ()), Left(expected))
      assertEquals(AvroFormat[Visit].reader(AvroFormat[Visit].schema).map(_ => ()), Left(expected))

  test("a codec whose schema to match cannot be read fails to make serdes"):
    given AvroCodec[Visit] = AvroCodec.derived[Visit].matching("{")
    for outcome <- registry.flatMap(AvroSerde.of[IO, Visit](_, Part.Value)).attempt
    yield assert(noSchema(outcome).startsWith("the schema to match cannot be read: "), noSchema(outcome))
