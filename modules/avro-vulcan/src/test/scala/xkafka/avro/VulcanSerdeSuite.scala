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

import cats.effect.IO
import cats.syntax.all.*
import fs2.Chunk
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import munit.CatsEffectSuite
import org.apache.avro.generic.GenericRecord
import xkafka.{Headers, Topic}
import xkafka.schemaregistry.{Part, RegistryException, SchemaFormat, Writing}

final case class Visit(id: String, pages: Int = 1, referrer: Option[String] = None)

object Visit:
  given vulcan.Codec[Visit] =
    vulcan.Codec.record("Visit", "example.web"): field =>
      (field("id", _.id), field("pages", _.pages, default = Some(1)), field("referrer", _.referrer, default = Some(None))).mapN(Visit(_, _, _))

/** An older version of the visit, without its pages and referrer. */
final case class VisitBefore(id: String)

object VisitBefore:
  given vulcan.Codec[VisitBefore] = vulcan.Codec.record("Visit", "example.web")(_("id", _.id).map(VisitBefore(_)))

/** A newer version of the visit, whose pages are a long, and which has a channel. */
final case class VisitAfter(id: String, pages: Long, referrer: Option[String], channel: String = "web")

object VisitAfter:
  given vulcan.Codec[VisitAfter] =
    vulcan.Codec.record("Visit", "example.web"): field =>
      (field("id", _.id), field("pages", _.pages), field("referrer", _.referrer), field("channel", _.channel, default = Some("web")))
        .mapN(VisitAfter(_, _, _, _))

/** A channel, as two versions of its enum know channels: the older without television, which takes the web in its place. */
enum Channel:
  case Web, App, Tv

object Channel:
  val all: vulcan.Codec[Channel] =
    vulcan.Codec.enumeration[Channel]("Channel", "example.web", List("Web", "App", "Tv"), _.toString, name => Right(Channel.valueOf(name)))
  val known: vulcan.Codec[Channel] =
    vulcan.Codec.enumeration[Channel](
      "Channel",
      "example.web",
      List("Web", "App"),
      _.toString,
      name => Right(Channel.valueOf(name)),
      default = Some(Channel.Web)
    )

/** Values of vulcan's codecs written and read with the registry's schemas, against Confluent's serializers. */
class VulcanSerdeSuite extends CatsEffectSuite:
  private val topic = Topic.from("visits").toOption.get
  private val visit = Visit("v-1", 3, Some("search"))

  private def format[A: vulcan.Codec]: SchemaFormat[A] = VulcanFormat[A].fold(fail(_), identity)

  test("a codec's format registers the codec's schema, named by its record"):
    assertEquals(format[Visit].schema.text, vulcan.Codec[Visit].schema.map(_.toString).fold(error => fail(error.message), identity))
    assertEquals(format[Visit].recordName, Some("example.web.Visit"))
    assertEquals(format[String].recordName, None)

  test("values read back as they were written"):
    val read =
      for
        write <- format[Visit].writer(format[Visit].schema)
        bytes <- write(visit)
        read  <- format[Visit].reader(format[Visit].schema)
        back  <- read(bytes)
      yield back
    assertEquals(read, Right(visit))

  test("a record with bytes left after its value is refused, as xkafka's own Avro refuses it"):
    val read =
      for
        write <- format[Visit].writer(format[Visit].schema)
        bytes <- write(visit)
        read  <- format[Visit].reader(format[Visit].schema)
        back  <- read(bytes ++ Chunk[Byte](0, 0))
      yield back
    assertEquals(read, Left("2 bytes are left after the value"))

  test("the serde writes the bytes Confluent's serializer writes, and each reads what the other writes"):
    val mock   = new MockSchemaRegistryClient()
    val record = vulcan.Codec[Visit].encode(visit).fold(error => fail(error.message), identity)
    for
      serde  <- VulcanSerde.of[IO, Visit](ConfluentRegistry.bridged(mock), Part.Value)
      ours   <- serde.serializer.serialize(topic, Headers.empty, visit).map(_.get)
      theirs <- IO.blocking(Chunk.array(ConfluentRegistry.serializer(mock).serialize("visits", record)))
      read   <- IO.blocking(ConfluentRegistry.deserializer(mock).deserialize("visits", ours.toArray))
      back   <- serde.deserializer.deserialize(topic, Headers.empty, Some(theirs))
    yield
      assertEquals(ours, theirs)
      read match
        case written: GenericRecord => assertEquals(List("id", "pages", "referrer").map(written.get(_).toString), List("v-1", "3", "search"))
        case other                  => fail(s"expected a record, got $other")
      assertEquals(back, visit)

  test("a record written with an older schema is read with the newer codec's defaults"):
    val registry = ConfluentRegistry.bridged(new MockSchemaRegistryClient())
    for
      older <- VulcanSerde.of[IO, VisitBefore](registry, Part.Value)
      bytes <- older.serializer.serialize(topic, Headers.empty, VisitBefore("v-2")).map(_.get)
      newer <- VulcanSerde.of[IO, Visit](registry, Part.Value)
      read  <- newer.deserializer.deserialize(topic, Headers.empty, Some(bytes))
    yield assertEquals(read, Visit("v-2"))

  test("a value is written with the subject's latest schema where told so, leaving out the fields it lacks"):
    val registry = ConfluentRegistry.bridged(new MockSchemaRegistryClient())
    for
      older  <- VulcanSerde.of[IO, VisitBefore](registry, Part.Value)
      first  <- older.serializer.serialize(topic, Headers.empty, VisitBefore("v-0")).map(_.get)
      newer  <- VulcanSerde.of[IO, Visit](registry, Part.Value, writing = Writing.Latest)
      bytes  <- newer.serializer.serialize(topic, Headers.empty, visit).map(_.get)
      before <- older.deserializer.deserialize(topic, Headers.empty, Some(bytes))
    yield
      assertEquals(bytes.take(5), first.take(5), "the latest schema's id")
      assertEquals(before, VisitBefore("v-1"))

  test("a record is read as Apache Avro resolves the schema it was written with to the codec's, promoting an int to a long"):
    val registry = ConfluentRegistry.bridged(new MockSchemaRegistryClient())
    for
      older <- VulcanSerde.of[IO, Visit](registry, Part.Value)
      bytes <- older.serializer.serialize(topic, Headers.empty, visit).map(_.get)
      newer <- VulcanSerde.of[IO, VisitAfter](registry, Part.Value)
      read  <- newer.deserializer.deserialize(topic, Headers.empty, Some(bytes))
    yield assertEquals(read, VisitAfter("v-1", 3L, Some("search")))

  test("a value written with a schema that has fields the codec lacks takes their defaults"):
    val registry = ConfluentRegistry.bridged(new MockSchemaRegistryClient())
    for
      newer <- VulcanSerde.of[IO, VisitAfter](registry, Part.Value)
      _     <- newer.serializer.serialize(topic, Headers.empty, VisitAfter("v-0", 1L, None))
      older <- VulcanSerde.of[IO, Visit](registry, Part.Value, writing = Writing.Latest)
      bytes <- older.serializer.serialize(topic, Headers.empty, visit).map(_.get)
      read  <- newer.deserializer.deserialize(topic, Headers.empty, Some(bytes))
    yield assertEquals(read, VisitAfter("v-1", 3L, Some("search"), "web"))

  test("an enum's symbol the codec's schema lacks is read as its default, as Apache Avro resolves enums"):
    val registry = ConfluentRegistry.bridged(new MockSchemaRegistryClient())
    for
      newer <- VulcanSerde.of[IO, Channel](registry, Part.Value)(using Channel.all, summon)
      bytes <- newer.serializer.serialize(topic, Headers.empty, Channel.Tv).map(_.get)
      older <- VulcanSerde.of[IO, Channel](registry, Part.Value)(using Channel.known, summon)
      read  <- older.deserializer.deserialize(topic, Headers.empty, Some(bytes))
    yield assertEquals(read, Channel.Web)

  test("a codec without a schema is refused"):
    given broken: vulcan.Codec[Visit] = vulcan.Codec.record[Visit]("not a name", "example")(_("id", _.id).map(Visit(_)))
    assert(VulcanFormat[Visit](using broken).isLeft)
    VulcanSerde.of[IO, Visit](ConfluentRegistry.bridged(new MockSchemaRegistryClient()), Part.Value).attempt.map: result =>
      assert(result.left.exists(_.isInstanceOf[RegistryException.NoSchema]), result)
