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
import fs2.Chunk
import munit.CatsEffectSuite
import xkafka.{Headers, Topic}
import xkafka.schemaregistry.{FakeRegistry, Part, RegistryException, RegistrySettings, Schema, SchemaDoc, SchemaNamespace, SchemaRegistry, SchemaType}

@SchemaNamespace("example.web") @SchemaDoc("A visit")
final case class Visit(
    @SchemaDoc("The visit's id")
    id: String,
    pages: Int = 1
) derives AvroCodec

final case class Link(value: Int, next: Option[Link])

object Link:
  val avroSchema: String =
    """{"type": "record", "name": "Link", "namespace": "xkafka.avro",
      | "fields": [{"name": "value", "type": "int"}, {"name": "next", "type": ["null", "Link"]}]}""".stripMargin

  given AvroCodec[Link] = AvroCodec.derived[Link].matching(avroSchema)

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

  test("a codec matches a file that names a type again where it uses it again"):
    final case class Stop(city: String) derives AvroCodec
    final case class Trip(from: Stop, to: Stop, via: Option[Stop]) derives AvroCodec
    val trip =
      """{"type": "record", "name": "Trip", "namespace": "xkafka.avro", "fields": [
        | {"name": "from", "type": {"type": "record", "name": "Stop", "fields": [{"name": "city", "type": "string"}]}},
        | {"name": "to", "type": "Stop"}, {"name": "via", "type": ["null", "xkafka.avro.Stop"]}]}""".stripMargin
    assertEquals(AvroCodec[Trip].matching(trip).mismatch, None)

  test("a codec matches a file with a fixed"):
    @AvroFixed(16)
    final case class Id(bytes: Chunk[Byte]) derives AvroCodec
    final case class Session(id: Id) derives AvroCodec
    val session =
      """{"type": "record", "name": "Session", "namespace": "xkafka.avro",
        | "fields": [{"name": "id", "type": {"type": "fixed", "name": "Id", "size": 16}}]}""".stripMargin
    assertEquals(AvroCodec[Session].matching(session).mismatch, None)

  test("a codec matches a file with an enum's default"):
    @AvroEnumDefault("NEW")
    enum State derives AvroCodec:
      case NEW, DONE
    val state = """{"type": "enum", "name": "State", "namespace": "xkafka.avro", "symbols": ["NEW", "DONE"], "default": "NEW"}"""
    assertEquals(AvroCodec[State].matching(state).mismatch, None)

  test("a codec matches a file with a union of records"):
    final case class Cash(cents: Long) derives AvroCodec
    final case class Card(number: String) derives AvroCodec
    given AvroCodec[Cash | Card] = AvroCodec.union[(Cash, Card)]
    final case class Paid(by: Cash | Card) derives AvroCodec
    val paid =
      """{"type": "record", "name": "Paid", "namespace": "xkafka.avro", "fields": [{"name": "by", "type": [
        | {"type": "record", "name": "Cash", "fields": [{"name": "cents", "type": "long"}]},
        | {"type": "record", "name": "Card", "fields": [{"name": "number", "type": "string"}]}]}]}""".stripMargin
    assertEquals(AvroCodec[Paid].matching(paid).mismatch, None)

  test("a type that holds itself matches its file, with its codec given in its companion as generated code gives it"):
    for
      client  <- registry
      serde   <- AvroSerde.of[IO, Link](client, Part.Value)
      written <- serde.serializer.serialize(topic, Headers.empty, Link(1, Some(Link(2, None))))
      read    <- serde.deserializer.deserialize(topic, Headers.empty, written)
    yield assertEquals(read, Link(1, Some(Link(2, None))))

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
