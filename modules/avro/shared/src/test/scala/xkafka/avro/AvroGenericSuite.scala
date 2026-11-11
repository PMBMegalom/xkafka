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
import xkafka.schemaregistry.{FakeRegistry, Part, RegistryException, RegistrySettings, SchemaRegistry}

class AvroGenericSuite extends CatsEffectSuite:
  private val base     = "http://registry:8081"
  private val settings = RegistrySettings.from(NonEmptyList.one(base)).toOption.get
  private val topic    = Topic.from("lines").toOption.get
  private val name     = FullName(Some("xkafka.avro"), "Line")

  private def registry: IO[SchemaRegistry[IO]] = FakeRegistry().flatMap(fake => SchemaRegistry.of(settings, fake.transport(base)))

  private def line(sku: String, quantity: Int): AvroValue =
    AvroValue.Record(name, Vector("sku" -> AvroValue.String(sku), "quantity" -> AvroValue.Int(quantity)))

  test("a value of a schema known only as the program runs is written and read back"):
    for
      client  <- registry
      serde   <- AvroSerde.generic[IO](client, AvroCodec[Line].schema, Part.Value)
      written <- serde.serializer.serialize(topic, Headers.empty, line("apple", 3))
      read    <- serde.deserializer.deserialize(topic, Headers.empty, written)
      typed   <- AvroSerde.of[IO, Line](client, Part.Value).flatMap(_.deserializer.deserialize(topic, Headers.empty, written))
    yield
      assertEquals(read, line("apple", 3))
      assertEquals(typed, Line("apple", 3), "a typed serde reads what the generic one wrote")

  test("a generic serde reads a record written with another schema as its own schema describes it"):
    for
      client  <- registry
      written <- AvroSerde.of[IO, LineBefore](client, Part.Value).flatMap(_.serializer.serialize(topic, Headers.empty, LineBefore("pear")))
      read    <- AvroSerde.generic[IO](client, AvroCodec[Line].schema, Part.Value).flatMap(_.deserializer.deserialize(topic, Headers.empty, written))
    yield assertEquals(read, line("pear", 1))

  test("a value that does not fit the schema is refused"):
    for
      client  <- registry
      serde   <- AvroSerde.generic[IO](client, AvroCodec[Line].schema, Part.Value)
      refused <- serde.serializer.serialize(topic, Headers.empty, AvroValue.Record(name, Vector("sku" -> AvroValue.Int(1)))).attempt
    yield refused match
      case Left(error: RegistryException.Unserializable) => assert(error.reason.contains("does not fit"), error.reason)
      case other                                         => fail(s"expected unserializable, got $other")

  test("records read as written carry the schema each was written with"):
    for
      client  <- registry
      older   <- AvroSerde.of[IO, LineBefore](client, Part.Value).flatMap(_.serializer.serialize(topic, Headers.empty, LineBefore("fig")))
      newer   <- AvroSerde.of[IO, Line](client, Part.Value).flatMap(_.serializer.serialize(topic, Headers.empty, Line("kiwi", 2)))
      reading <- AvroSerde.asWritten[IO](client)
      first   <- reading.deserialize(topic, Headers.empty, older)
      second  <- reading.deserialize(topic, Headers.empty, newer)
    yield
      assertEquals(first, AvroWritten(AvroCodec[LineBefore].schema, AvroValue.Record(name, Vector("sku" -> AvroValue.String("fig")))))
      assertEquals(second, AvroWritten(AvroCodec[Line].schema, line("kiwi", 2)))
