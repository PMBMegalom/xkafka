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

package xkafka.schemaregistry

import java.nio.charset.StandardCharsets

import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all.*
import fs2.Chunk
import munit.CatsEffectSuite
import xkafka.{Deserializer, Headers, Serde, Topic}

class RegistrySerdeSuite extends CatsEffectSuite:
  private val base     = "http://registry:8081"
  private val settings = RegistrySettings.from(NonEmptyList.one(base)).toOption.get
  private val orders   = Topic.from("orders").toOption.get
  private val plain    = Schema("""{"type":"string","name":"Text"}""", SchemaType.Avro)
  private val loud     = Schema("""{"type":"string","name":"Loud"}""", SchemaType.Avro)

  /** Writes text as UTF-8, in capitals with the schema `loud`, refusing empty text, and reads back what was written with `loud` marked as such. */
  private final class Text(val recordName: Option[String]) extends SchemaFormat[String]:
    override val schema: Schema = plain

    override def writer(target: Schema): Either[String, String => Either[String, Chunk[Byte]]] =
      def utf8(text: String) = Either.cond(text.nonEmpty, Chunk.array(text.getBytes(StandardCharsets.UTF_8)), "empty text")
      if target == plain then Right(utf8)
      else if target == loud then Right(text => utf8(text.toUpperCase))
      else Left(s"cannot write with ${target.text}")

    override def reader(writer: Schema): Either[String, Chunk[Byte] => Either[String, String]] =
      Either.cond(
        writer == plain || writer == loud,
        bytes => Right(s"${if writer == loud then "loud " else ""}${new String(bytes.toArray, StandardCharsets.UTF_8)}"),
        s"cannot read ${writer.text}"
      )

  private def text(recordName: Option[String] = Some("com.example.Text")): IO[Text] = IO.pure(Text(recordName))

  private def serde(
      fake: FakeRegistry,
      format: SchemaFormat[String],
      part: Part = Part.Value,
      naming: SubjectNaming = SubjectNaming.TopicName,
      writing: Writing = Writing.Register
  ): IO[Serde[IO, String]] = SchemaRegistry.of(settings, fake.transport(base)).flatMap(RegistrySerde.of(_, format, part, naming, writing))

  private def write(serde: Serde[IO, String], value: String): IO[Chunk[Byte]] = serde.serializer.serialize(orders, Headers.empty, value).map(_.get)

  private def read(serde: Serde[IO, String], bytes: Chunk[Byte]): IO[String] = serde.deserializer.deserialize(orders, Headers.empty, Some(bytes))

  private def subjects(fake: FakeRegistry): IO[List[String]] = fake.requests.get.map(_.toList.map(_.url.drop(base.length)))

  test("a value is written as the zero byte, the schema's id in four big-endian bytes and the value, and read back"):
    for
      fake    <- FakeRegistry()
      _       <- (1 to 299).toList.traverse_(n => fake.registered(s"filler-$n", s"\"$n\""))
      format  <- text()
      written <- serde(fake, format).flatMap(write(_, "hello"))
      back    <- serde(fake, format).flatMap(read(_, written))
    yield
      assertEquals(written.toList, List[Byte](0, 0, 0, 1, 44) ++ "hello".getBytes(StandardCharsets.UTF_8).toList)
      assertEquals(back, "hello")

  test("subjects are named by topic and part, by record, or by topic and record"):
    def subjectOf(part: Part, naming: SubjectNaming) =
      for
        fake   <- FakeRegistry()
        format <- text()
        _      <- serde(fake, format, part, naming).flatMap(write(_, "x"))
        asked  <- subjects(fake)
      yield asked
    for
      value  <- subjectOf(Part.Value, SubjectNaming.TopicName)
      key    <- subjectOf(Part.Key, SubjectNaming.TopicName)
      record <- subjectOf(Part.Value, SubjectNaming.RecordName)
      both   <- subjectOf(Part.Key, SubjectNaming.TopicRecordName)
    yield
      assertEquals(value, List("/subjects/orders-value/versions"))
      assertEquals(key, List("/subjects/orders-key/versions"))
      assertEquals(record, List("/subjects/com.example.Text/versions"))
      assertEquals(both, List("/subjects/orders-com.example.Text/versions"))

  test("naming subjects by record refuses a format that names no record"):
    for
      fake    <- FakeRegistry()
      format  <- text(recordName = None)
      written <- serde(fake, format, naming = SubjectNaming.RecordName).flatMap(write(_, "x")).attempt
    yield written match
      case Left(error: RegistryException.Unserializable) =>
        assertEquals(error.reason, "the format names no record, which naming subjects by record needs")
      case other => fail(s"expected unserializable, got $other")

  test("writing with an already registered schema refuses one that is not, and registers nothing"):
    for
      fake    <- FakeRegistry()
      format  <- text()
      refused <- serde(fake, format, writing = Writing.AlreadyRegistered).flatMap(write(_, "x")).attempt
      id      <- fake.registered("orders-value", plain.text)
      written <- serde(fake, format, writing = Writing.AlreadyRegistered).flatMap(write(_, "x"))
      asked   <- subjects(fake)
    yield
      assert(refused.left.exists(_.getMessage.contains("the format's schema is not registered under subject 'orders-value'")), refused.toString)
      assertEquals(written.take(5).toList, List[Byte](0, 0, 0, 0, id.toByte))
      assert(!asked.exists(_.endsWith("/versions")), asked.toString)

  test("writing with the latest version uses its id and writes with it, and a subject without one is refused"):
    for
      fake    <- FakeRegistry()
      format  <- text()
      refused <- serde(fake, format, writing = Writing.Latest).flatMap(write(_, "x")).attempt
      _       <- fake.registered("orders-value", plain.text)
      id      <- fake.registered("orders-value", loud.text)
      written <- serde(fake, format, writing = Writing.Latest).flatMap(write(_, "quiet"))
    yield
      assert(refused.left.exists(_.getMessage.contains("subject 'orders-value' has no versions")), refused.toString)
      assertEquals(written.toList, List[Byte](0, 0, 0, 0, id.toByte) ++ "QUIET".getBytes(StandardCharsets.UTF_8).toList)

  test("writing with a pinned id writes with that schema"):
    for
      fake    <- FakeRegistry()
      format  <- text()
      id      <- fake.registered("anything", loud.text)
      written <- serde(fake, format, writing = Writing.Pinned(SchemaId(id))).flatMap(write(_, "quiet"))
      back    <- serde(fake, format).flatMap(read(_, written))
    yield assertEquals(back, "loud QUIET")

  test("one serde reads each record with the schema it names"):
    for
      fake    <- FakeRegistry()
      format  <- text()
      id      <- fake.registered("anything", loud.text)
      quiet   <- serde(fake, format).flatMap(write(_, "quiet"))
      shouted <- serde(fake, format, writing = Writing.Pinned(SchemaId(id))).flatMap(write(_, "quiet"))
      reading <- serde(fake, format)
      back    <- List(quiet, shouted, quiet).traverse(read(reading, _))
    yield assertEquals(back, List("quiet", "loud QUIET", "quiet"))

  test("a value the format refuses fails as unserializable"):
    for
      fake    <- FakeRegistry()
      format  <- text()
      refused <- serde(fake, format).flatMap(write(_, "")).attempt
    yield refused match
      case Left(error: RegistryException.Unserializable) => assertEquals((error.topic, error.reason), (orders, "empty text"))
      case other                                         => fail(s"expected unserializable, got $other")

  test("a schema the format cannot write with is refused"):
    for
      fake    <- FakeRegistry()
      format  <- text()
      id      <- fake.registered("anything", "\"int\"")
      refused <- serde(fake, format, writing = Writing.Pinned(SchemaId(id))).flatMap(write(_, "x")).attempt
    yield assert(refused.left.exists(_.getMessage.contains("cannot write with \"int\"")), refused.toString)

  test("records that name no schema, or a schema the format cannot read, are refused, and an absent value fails as null"):
    for
      fake    <- FakeRegistry()
      format  <- text()
      id      <- fake.registered("anything", "\"int\"")
      reading <- serde(fake, format)
      empty   <- read(reading, Chunk.empty).attempt
      magic   <- read(reading, Chunk[Byte](1, 0, 0, 0, 1)).attempt
      short   <- read(reading, Chunk[Byte](0, 0, 0)).attempt
      other   <- read(reading, Chunk[Byte](0, 0, 0, 0, id.toByte)).attempt
      absent  <- reading.deserializer.deserialize(orders, Headers.empty, None).attempt
    yield
      assert(empty.left.exists(_.getMessage.contains("the record is empty, so it names no schema")), empty.toString)
      assert(magic.left.exists(_.getMessage.contains("the record starts with byte 1")), magic.toString)
      assert(short.left.exists(_.getMessage.contains("the record is 3 bytes, too short to name its schema")), short.toString)
      assert(other.left.exists(_.getMessage.contains("cannot read \"int\"")), other.toString)
      assert(absent.left.exists(_.isInstanceOf[Deserializer.NullValueException]), absent.toString)

  test("each schema is asked for once however many records are written or read with it"):
    for
      fake    <- FakeRegistry()
      format  <- text()
      writing <- serde(fake, format)
      written <- List("a", "b", "c").traverse(write(writing, _))
      reading <- serde(fake, format)
      back    <- written.traverse(read(reading, _))
      asked   <- subjects(fake)
    yield
      assertEquals(back, List("a", "b", "c"))
      assertEquals(asked, List("/subjects/orders-value/versions", "/schemas/ids/1"))
