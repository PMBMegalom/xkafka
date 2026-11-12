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
import com.sksamuel.avro4s.{AvroName, AvroNamespace, AvroSchema, Decoder, Encoder, SchemaFor}
import fs2.Chunk
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import munit.CatsEffectSuite
import org.apache.avro.{Schema as ApacheSchema, SchemaBuilder}
import org.apache.avro.generic.GenericRecord
import xkafka.{Headers, Topic}
import xkafka.schemaregistry.{Part, RegistryException, SchemaFormat, Writing}

@AvroNamespace("example.web")
final case class Visit(id: String, pages: Int = 1, referrer: Option[String] = None)

/** An older version of the visit, without its pages and referrer. */
@AvroNamespace("example.web") @AvroName("Visit")
final case class VisitBefore(id: String)

/** A newer version of the visit, whose pages are a long, and which has a channel. */
@AvroNamespace("example.web") @AvroName("Visit")
final case class VisitAfter(id: String, pages: Long, referrer: Option[String], channel: String = "web")

/** A channel, as two versions of its enum know channels: the older without television, which takes the web in its place. */
@AvroNamespace("example.web") @AvroName("Channel")
enum Channel:
  case Web, App, Tv

enum Known:
  case Web, App

object Known:
  // avro4s gives Scala 3 enums no default, so the schema is written out.
  given SchemaFor[Known] = SchemaFor(SchemaBuilder.enumeration("Channel").namespace("example.web").defaultSymbol("Web").symbols("Web", "App"))

/** Values of avro4s's encoders and decoders written and read with the registry's schemas, against Confluent's serializers. */
class Avro4sSerdeSuite extends CatsEffectSuite:
  private val topic = Topic.from("visits").toOption.get
  private val visit = Visit("v-1", 3, Some("search"))

  private def format[A: SchemaFor: Encoder: Decoder]: SchemaFormat[A] = Avro4sFormat[A].fold(fail(_), identity)

  test("the format registers avro4s's schema, named by its record"):
    assertEquals(format[Visit].schema.text, AvroSchema[Visit].toString)
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

  test("the serde writes the bytes Confluent's serializer writes, and each reads what the other writes"):
    val mock   = new MockSchemaRegistryClient()
    val record = Encoder[Visit].encode(AvroSchema[Visit])(visit)
    for
      serde  <- Avro4sSerde.of[IO, Visit](ConfluentRegistry.bridged(mock), Part.Value)
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

  /** What `writer`'s format writes of `value` with the schema `target`, as read by `reader`'s format. */
  private def across[A: SchemaFor: Encoder: Decoder, B: SchemaFor: Encoder: Decoder](value: A, target: SchemaFormat[?]): Either[String, B] =
    for
      write <- format[A].writer(target.schema)
      bytes <- write(value)
      read  <- format[B].reader(target.schema)
      back  <- read(bytes)
    yield back

  test("a record written with an older schema is read with the newer type's defaults"):
    assertEquals(across[VisitBefore, Visit](VisitBefore("v-2"), format[VisitBefore]), Right(Visit("v-2")))

  test("a record is read as Apache Avro resolves the schema it was written with to avro4s's, promoting an int to a long"):
    assertEquals(across[Visit, VisitAfter](visit, format[Visit]), Right(VisitAfter("v-1", 3L, Some("search"))))

  test("an enum's symbol avro4s's schema lacks is read as its default"):
    assertEquals(across[Channel, Known](Channel.Tv, format[Channel]), Right(Known.Web))

  test("a value written with another schema, as the subject's latest, takes the defaults of the fields avro4s's schema lacks"):
    assertEquals(across[Visit, VisitAfter](visit, format[VisitAfter]), Right(VisitAfter("v-1", 3L, Some("search"), "web")))

  test("the serde writes with the subject's latest schema where told so"):
    val registry = ConfluentRegistry.bridged(new MockSchemaRegistryClient())
    for
      newer <- Avro4sSerde.of[IO, VisitAfter](registry, Part.Value)
      first <- newer.serializer.serialize(topic, Headers.empty, VisitAfter("v-0", 1L, None)).map(_.get)
      older <- Avro4sSerde.of[IO, Visit](registry, Part.Value, writing = Writing.Latest)
      bytes <- older.serializer.serialize(topic, Headers.empty, visit).map(_.get)
      read  <- newer.deserializer.deserialize(topic, Headers.empty, Some(bytes))
    yield
      assertEquals(bytes.take(5), first.take(5), "the latest schema's id")
      assertEquals(read, VisitAfter("v-1", 3L, Some("search"), "web"))

  test("a type avro4s gives no schema is refused"):
    given broken: SchemaFor[Visit] =
      new SchemaFor[Visit]:
        override def schema: ApacheSchema = throw new IllegalStateException("no schema")
    assert(Avro4sFormat[Visit].left.exists(_.contains("no schema")))
    Avro4sSerde.of[IO, Visit](ConfluentRegistry.bridged(new MockSchemaRegistryClient()), Part.Value).attempt.map: result =>
      assert(result.left.exists(_.isInstanceOf[RegistryException.NoSchema]), result)
