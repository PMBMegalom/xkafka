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

import java.time.{Instant, LocalDate, LocalDateTime, LocalTime}
import java.util.UUID

import example.audit.{Entry, Level}
import example.chain.Link
import example.shop.*
import fs2.Chunk
import munit.FunSuite
import xkafka.avro.{AvroCodec, AvroFormat, AvroSchemaJson}

/** Values of the classes xkafka-codegen generated from the schemas in src/main/avro, written and read with the schemas they were generated from. */
class GeneratedAvroSuite extends FunSuite:
  private def roundTrip[A](value: A)(using AvroCodec[A]): Either[String, A] =
    val format = AvroFormat[A]
    for
      write <- format.writer(format.schema)
      bytes <- write(value)
      read  <- format.reader(format.schema)
      back  <- read(bytes)
    yield back

  private def registers[A](avroSchema: String)(using AvroCodec[A]): Unit =
    assertEquals(AvroFormat[A].writer(AvroFormat[A].schema).map(_ => ()), Right(()), "the codec has the schema it was generated from")
    assertEquals(Right(AvroFormat[A].schema.text), AvroSchemaJson.parse(avroSchema).map(AvroSchemaJson.render))

  private val order =
    Order(
      id = "o-1",
      lines = List(Line("apple", 3)),
      hash = Hash(Chunk[Byte](1, 2, 3, 4)),
      payment = Card("4111"),
      refund = Some(Cash(5)),
      code = "x",
      discount = Some(BigDecimal("1.5")),
      `type` = "kind",
      copy_ = 9
    )

  test("a schema longer than the JVM holds in one constant is generated, and its codec has it"):
    registers[Large](Large.avroSchema)
    assert(Large.avroSchema.length > 65535 / 3)

  test("enums whose symbols are named as packages are generated, and read back"):
    val scoped = Scoped(Scope.java, java.time.Instant.ofEpochMilli(1000L), fs2.Chunk[Byte](1, 2))
    assertEquals(roundTrip(scoped), Right(scoped))

  test("types whose names end with an underscore are generated, and read back"):
    assertEquals(roundTrip(Marked_(Kind_.MARKED)), Right(Marked_(Kind_.MARKED)))

  test("text holding half of a surrogate pair is generated as it is"):
    assertEquals(Lone().text, "\ud800 and \ud83d\ude00")

  test("every generated type's codec has the schema it was generated from"):
    registers[Order](Order.avroSchema)
    registers[Status](Status.avroSchema)
    registers[Line](Line.avroSchema)
    registers[Hash](Hash.avroSchema)
    registers[Cash](Cash.avroSchema)
    registers[Card](Card.avroSchema)
    registers[Link](Link.avroSchema)
    registers[example.`type`.List](example.`type`.List.avroSchema)
    registers[Entry](Entry.avroSchema)
    registers[Level](Level.avroSchema)

  test("a record's fields take the file's defaults"):
    val defaults = Order(id = "o", lines = Nil, hash = Hash(Chunk[Byte](0, 0, 0, 0)), payment = Cash(1))
    assertEquals((defaults.quantity, defaults.total, defaults.ratio, defaults.weight, defaults.paid), (1, -5L, 0.5f, 1.25, false))
    assertEquals(defaults.greeting, "say \"hi\"\né")
    assertEquals(defaults.blob, Chunk[Byte](0, -1))
    assertEquals((defaults.note, defaults.label, defaults.tags, defaults.counts), (None, Some("none"), List("a", "b"), Map("x" -> 1L)))
    assertEquals((defaults.status, defaults.line, defaults.previous, defaults.refund, defaults.code), (Status.NEW, Line("none", 0), None, None, 7))
    assertEquals(defaults.either, Some(3))
    assertEquals((defaults.price, defaults.discount), (BigDecimal("12.34"), None))
    assertEquals(defaults.ref, UUID.fromString("123e4567-e89b-12d3-a456-426614174000"))
    assertEquals((defaults.day, defaults.opens), (LocalDate.ofEpochDay(20000), LocalTime.of(9, 0)))
    assertEquals(defaults.placed, Instant.ofEpochSecond(1700000000L, 1000L))
    assertEquals(defaults.local, LocalDateTime.of(2023, 11, 14, 22, 13, 20))
    assertEquals((defaults.`type`, defaults.copy_), ("keyword", 2))

  test("times and timestamps of every precision are generated with the codec of their precision, and their defaults"):
    import example.times.{Defaults, Nanos}
    registers[Defaults](Defaults.avroSchema)
    registers[Nanos](Nanos.avroSchema)
    assertEquals(Defaults().time, LocalTime.ofNanoOfDay(1000001000L))
    assertEquals(Defaults().at, Instant.ofEpochMilli(1700000000001L))
    assertEquals(Defaults().local, LocalDateTime.of(2023, 11, 14, 22, 13, 20, 1000))
    assertEquals(Nanos().at, Instant.ofEpochSecond(1700000000L, 1L))
    assertEquals(roundTrip(Nanos(Instant.ofEpochSecond(5, 7))), Right(Nanos(Instant.ofEpochSecond(5, 7))))
    assertEquals(roundTrip(Defaults()), Right(Defaults()))

  test("records of the generated classes read back as they were written"):
    assertEquals(roundTrip(order), Right(order))
    assertEquals(roundTrip(Link(1, Some(Link(2, None)))), Right(Link(1, Some(Link(2, None)))))
    assertEquals(roundTrip(example.`type`.List(List("a"), Some("b"))), Right(example.`type`.List(List("a"), Some("b"))))
    assertEquals(roundTrip(Entry("me", Level.HIGH, Some(5L))), Right(Entry("me", Level.HIGH, Some(5L))))
    Status.values.foreach(status => assertEquals(roundTrip(status), Right(status)))

  test("a record named String and a symbol named as the schema's own value are generated, the symbol renamed"):
    import example.named.{Member, String as Named}
    assert(Member.avroSchema.contains("\"symbols\":[\"avroSchema\",\"other\"]"), Member.avroSchema)
    assertEquals(AvroCodec[Member].encode(Member.avroSchema_).map(_.toString), Right("Enum(FullName(Some(example.named),Member),avroSchema)"))
    assertEquals(roundTrip(Named(Member.avroSchema_)), Right(Named(Member.avroSchema_)))

  test("names Scala keeps for itself are renamed in Scala and keep their Avro names in the schema"):
    assert(Order.avroSchema.contains("\"name\":\"copy\""), Order.avroSchema)
    assert(Status.avroSchema.contains("\"symbols\":[\"NEW\",\"PAID\",\"type\",\"values\"]"), Status.avroSchema)
    assertEquals(Status.values.toList, List(Status.NEW, Status.PAID, Status.`type`, Status.values_))
    assertEquals(AvroCodec[Status].encode(Status.values_).map(_.toString), Right("Enum(FullName(Some(example.shop),Status),values)"))
