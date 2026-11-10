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

import java.time.{Instant, LocalDate, LocalDateTime, LocalTime}
import java.util.UUID

import fs2.Chunk
import munit.FunSuite
import xkafka.schemaregistry.{Schema, SchemaType}

final case class Line(sku: String, quantity: Int = 1) derives AvroCodec

enum Status derives AvroCodec:
  case Open, Paid, Shipped

sealed trait Payment derives AvroCodec
final case class Card(last4: String) extends Payment
case object Cash                     extends Payment
final case class Transfer(
    iban: String,
    @AvroDoc("When it cleared")
    cleared: Option[Instant] = None
) extends Payment

@AvroNamespace("com.example") @AvroDoc("An order") @AvroAlias("Purchase")
final case class Order(
    id: UUID,
    @AvroName("placed_at")
    placedAt: Instant,
    day: LocalDate,
    lines: List[Line],
    status: Status,
    payment: Payment,
    note: Option[String] = None,
    tags: Map[String, Long] = Map.empty,
    @AvroNoDefault
    retries: Int = 0
) derives AvroCodec

final case class Node(value: Int, next: Option[Node] = None) derives AvroCodec

final case class Box[A](value: A)

final case class Preferred(note: Option[String] = Some("none")) derives AvroCodec

final case class Maybe(note: Option[String]) derives AvroCodec

@AvroName("Line")
final case class LineBefore(sku: String) derives AvroCodec

class AvroCodecSuite extends FunSuite:
  private def json[A](codec: AvroCodec[A]): String = AvroSchemaJson.render(codec.schema)

  private def roundTrip[A](value: A)(using codec: AvroCodec[A]): Either[String, A] =
    val format = AvroFormat[A]
    for
      write <- format.writer(format.schema)
      bytes <- write(value)
      read  <- format.reader(format.schema)
      back  <- read(bytes)
    yield back

  private val order =
    Order(
      UUID.fromString("3f2b8c1e-9a4d-4e6f-8b2a-1c3d5e7f9a0b"),
      Instant.ofEpochMilli(1762761600123L),
      LocalDate.of(2026, 11, 10),
      List(Line("apple", 3), Line("pear")),
      Status.Paid,
      Transfer("DE00", Some(Instant.ofEpochMilli(5))),
      Some("ring twice"),
      Map("priority" -> 2L),
      1
    )

  test("a case class is a record of its fields, in its package, with its defaults"):
    assertEquals(
      json(AvroCodec[Line]),
      """{"type":"record","name":"Line","namespace":"xkafka.avro","fields":[{"name":"sku","type":"string"},{"name":"quantity","type":"int","default":1}]}"""
    )

  test("an enum of objects is an Avro enum of its cases' names"):
    assertEquals(json(AvroCodec[Status]), """{"type":"enum","name":"Status","namespace":"xkafka.avro","symbols":["Open","Paid","Shipped"]}""")

  test("a sealed trait is a union of its cases' records, an object's empty"):
    assertEquals(
      json(AvroCodec[Payment]),
      """[{"type":"record","name":"Card","namespace":"xkafka.avro","fields":[{"name":"last4","type":"string"}]},""" +
        """{"type":"record","name":"Cash","namespace":"xkafka.avro","fields":[]},""" +
        """{"type":"record","name":"Transfer","namespace":"xkafka.avro","fields":[{"name":"iban","type":"string"},""" +
        """{"name":"cleared","type":["null",{"type":"long","logicalType":"timestamp-millis"}],"doc":"When it cleared","default":null}]}]"""
    )

  test("annotations name, place, document and alias a record and its fields, and leave a default out"):
    AvroCodec[Order].schema match
      case AvroSchema.Record(name, fields, doc, aliases) =>
        assertEquals(name, FullName(Some("com.example"), "Order"))
        assertEquals(doc, Some("An order"))
        assertEquals(aliases, List(FullName(Some("com.example"), "Purchase")))
        assertEquals(fields.map(_.name), List("id", "placed_at", "day", "lines", "status", "payment", "note", "tags", "retries"))
        assertEquals(fields.map(_.default.isDefined), List(false, false, false, false, false, false, true, true, false))
        assertEquals(fields.find(_.name == "id").map(_.schema), Some(AvroSchema.Logical(LogicalType.Uuid)))
        assertEquals(fields.find(_.name == "day").map(_.schema), Some(AvroSchema.Logical(LogicalType.Date)))
      case other => fail(s"expected a record, got $other")

  test("values of every kind read back as they were written"):
    assertEquals(roundTrip(order), Right(order))
    assertEquals(
      roundTrip(order.copy(payment = Cash, note = None, status = Status.Open)),
      Right(order.copy(payment = Cash, note = None, status = Status.Open))
    )
    assertEquals(roundTrip[Payment](Card("4242")), Right(Card("4242")))
    assertEquals(roundTrip(Node(1, Some(Node(2, Some(Node(3)))))), Right(Node(1, Some(Node(2, Some(Node(3)))))))

  test("a record that holds itself names itself inside"):
    assertEquals(
      json(AvroCodec[Node]),
      """{"type":"record","name":"Node","namespace":"xkafka.avro","fields":[{"name":"value","type":"int"},{"name":"next","type":["null","Node"],"default":null}]}"""
    )

  test("a generic case class's name carries its type arguments"):
    assertEquals(AvroCodec.derived[Box[Int]].schema.fullName, Some(FullName(Some("xkafka.avro"), "Box__Int")))
    assertEquals(AvroCodec.derived[Box[String]].schema.fullName, Some(FullName(Some("xkafka.avro"), "Box__String")))

  test("an optional value is a union of null and its type, null first, as avro4s and Confluent's tools write it"):
    assertEquals(
      json(AvroCodec[Maybe]),
      """{"type":"record","name":"Maybe","namespace":"xkafka.avro","fields":[{"name":"note","type":["null","string"]}]}"""
    )

  test("an optional field with a value by default puts that branch first, as Avro takes a union's default from its first branch"):
    assertEquals(
      json(AvroCodec[Preferred]),
      """{"type":"record","name":"Preferred","namespace":"xkafka.avro","fields":[{"name":"note","type":["string","null"],"default":"none"}]}"""
    )
    assertEquals(roundTrip(Preferred(None)), Right(Preferred(None)))

  test("a record written before a field was added reads with the field's default"):
    val before  = AvroFormat[LineBefore]
    val written = before.writer(before.schema).flatMap(_(LineBefore("apple")))
    assertEquals(written.flatMap(bytes => AvroFormat[Line].reader(before.schema).flatMap(_(bytes))), Right(Line("apple", 1)))

  test("a value is written with another schema of its record, taking the defaults of fields it lacks and leaving out those it has not"):
    val after =
      Schema(
        """{"type":"record","name":"Line","namespace":"xkafka.avro","fields":[{"name":"sku","type":"string"},{"name":"colour","type":"string","default":"red"}]}""",
        SchemaType.Avro
      )
    val written = AvroFormat[Line].writer(after).flatMap(_(Line("apple", 3)))
    assertEquals(
      written.flatMap(AvroBinary.decode(AvroSchemaJson.parse(after.text).toOption.get, _)),
      Right(AvroValue.Record(FullName(Some("xkafka.avro"), "Line"), Vector("sku" -> AvroValue.String("apple"), "colour" -> AvroValue.String("red"))))
    )

  test("decimals keep their digits, and refuse a number with more than the schema allows rather than round it"):
    val money = AvroCodec.decimal(6, 2)
    assertEquals(money.encode(BigDecimal("1234.5")).flatMap(money.decode), Right(BigDecimal("1234.50")))
    assertEquals(money.encode(BigDecimal("-0.01")).flatMap(money.decode), Right(BigDecimal("-0.01")))
    assertEquals(money.encode(BigDecimal("1.005")), Left("1.005 has more than 2 digits after the point"))
    assertEquals(money.encode(BigDecimal("12345.67")), Left("12345.67 has more than 6 digits"))

  test("times keep what their logical type keeps"):
    val instant = Instant.parse("2026-11-10T07:00:00.123456789Z")
    assertEquals(AvroCodec[Instant].encode(instant), Right(AvroValue.Long(instant.toEpochMilli)))
    assertEquals(AvroCodec.instantMicros.encode(instant).flatMap(AvroCodec.instantMicros.decode), Right(Instant.parse("2026-11-10T07:00:00.123456Z")))
    assertEquals(AvroCodec.instantNanos.encode(instant).flatMap(AvroCodec.instantNanos.decode), Right(instant))
    assertEquals(AvroCodec.instantMicros.encode(Instant.parse("1969-12-31T23:59:59.999999Z")), Right(AvroValue.Long(-1)))
    assertEquals(AvroCodec.instantMicros.decode(AvroValue.Long(-1)), Right(Instant.parse("1969-12-31T23:59:59.999999Z")))
    assertEquals(AvroCodec.instantNanos.decode(AvroValue.Long(-1)), Right(Instant.parse("1969-12-31T23:59:59.999999999Z")))
    assertEquals(AvroCodec[LocalDate].encode(LocalDate.of(1970, 1, 2)), Right(AvroValue.Int(1)))
    assertEquals(AvroCodec[LocalTime].encode(LocalTime.of(0, 0, 1)), Right(AvroValue.Long(1000000)))
    assertEquals(AvroCodec.localTimeMillis.encode(LocalTime.of(0, 0, 1)), Right(AvroValue.Int(1000)))
    val local = LocalDateTime.of(2026, 11, 10, 7, 0, 0, 1000)
    assertEquals(AvroCodec[LocalDateTime].encode(local).flatMap(AvroCodec[LocalDateTime].decode), Right(local))
    assert(AvroCodec.instantNanos.encode(Instant.MAX).isLeft, "an instant past what nanoseconds since 1970 hold is refused")

  test("bytes, sets, vectors and maps read back as they were written"):
    final case class Kinds(raw: Chunk[Byte], set: Set[Int], vector: Vector[Double], map: Map[String, Option[Boolean]], float: Float) derives AvroCodec
    val kinds = Kinds(Chunk[Byte](0, -1, 7), Set(3, 1), Vector(1.5, -2.25), Map("a" -> Some(true), "b" -> None), 0.5f)
    assertEquals(roundTrip(kinds), Right(kinds))

  test("records written as one type are refused by a codec of another"):
    val bytes = AvroFormat[Line].writer(AvroFormat[Line].schema).flatMap(_(Line("x", 2)))
    val read  = bytes.flatMap(written => AvroFormat[Status].reader(AvroFormat[Line].schema).flatMap(_(written)))
    assert(read.left.exists(_.contains("cannot read what")), read.toString)
