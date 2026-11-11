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
import xkafka.schemaregistry.{Schema, SchemaAlias, SchemaDecimal, SchemaDoc, SchemaName, SchemaNamespace, SchemaNoDefault, SchemaType}

final case class Line(sku: String, quantity: Int = 1) derives AvroCodec

enum Status derives AvroCodec:
  case Open, Paid, Shipped

sealed trait Payment derives AvroCodec
final case class Card(last4: String) extends Payment
case object Cash                     extends Payment
final case class Transfer(
    iban: String,
    @SchemaDoc("When it cleared")
    cleared: Option[Instant] = None
) extends Payment

@SchemaNamespace("com.example") @SchemaDoc("An order") @SchemaAlias("Purchase")
final case class Order(
    id: UUID,
    @SchemaName("placed_at")
    placedAt: Instant,
    day: LocalDate,
    lines: List[Line],
    status: Status,
    payment: Payment,
    note: Option[String] = None,
    tags: Map[String, Long] = Map.empty,
    @SchemaNoDefault
    retries: Int = 0
) derives AvroCodec

final case class Node(value: Int, next: Option[Node] = None) derives AvroCodec

final case class Box[A](value: A)

final case class Preferred(note: Option[String] = Some("none")) derives AvroCodec

final case class Maybe(note: Option[String]) derives AvroCodec

@SchemaName("Line")
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

  test("fields marked SchemaDecimal are decimals of their own precision and scale, inside options and lists too, and with their defaults"):
    final case class Priced(
        @SchemaDecimal(10, 2)
        price: BigDecimal,
        @SchemaDecimal(precision = 5, scale = 1)
        discount: Option[BigDecimal] = Some(BigDecimal("1.5")),
        @SchemaDecimal(4, 0)
        counts: List[BigDecimal] = Nil
    ) derives AvroCodec
    assertEquals(
      AvroSchemaJson.render(AvroCodec[Priced].schema),
      """{"type":"record","name":"Priced","namespace":"xkafka.avro","fields":[""" +
        """{"name":"price","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}},""" +
        """{"name":"discount","type":[{"type":"bytes","logicalType":"decimal","precision":5,"scale":1},"null"],"default":"\u000f"},""" +
        """{"name":"counts","type":{"type":"array","items":{"type":"bytes","logicalType":"decimal","precision":4,"scale":0}},"default":[]}]}"""
    )
    val priced = Priced(BigDecimal("12.5"), None, List(BigDecimal(7)))
    assertEquals(roundTrip(priced), Right(Priced(BigDecimal("12.50"), None, List(BigDecimal(7)))))
    assertEquals(
      AvroCodec[Priced].encode(priced.copy(price = BigDecimal("1.005"))),
      Left("record 'xkafka.avro.Priced': 1.005 has more than 2 digits after the point")
    )

  test("SchemaDecimal takes the place of a decimal codec given for every BigDecimal, which unmarked fields take"):
    given AvroCodec[BigDecimal] = AvroCodec.decimal(6, 3)
    final case class Mixed(
        @SchemaDecimal(8, 1)
        marked: BigDecimal,
        unmarked: BigDecimal
    ) derives AvroCodec
    assertEquals(
      AvroCodec[Mixed].schema,
      AvroSchema.Record(
        FullName(Some("xkafka.avro"), "Mixed"),
        List(AvroField("marked", AvroSchema.Logical(LogicalType.Decimal(8, 1))), AvroField("unmarked", AvroSchema.Logical(LogicalType.Decimal(6, 3))))
      )
    )

  test("SchemaDecimal on a field without a BigDecimal, or with a precision and scale Avro does not allow, fails to compile"):
    assert(
      compileErrors("final case class Wrong(@SchemaDecimal(4, 1) text: String) derives AvroCodec")
        .contains("@SchemaDecimal on text, which holds no BigDecimal")
    )
    assert(
      compileErrors("final case class Wrong(@SchemaDecimal(2, 3) price: BigDecimal) derives AvroCodec")
        .contains("@SchemaDecimal(2, 3) on price: the precision must be at least 1 and the scale from 0 to the precision")
    )
    assert(
      compileErrors("final case class Wrong(@SchemaDecimal(0, 0) price: BigDecimal) derives AvroCodec")
        .contains("@SchemaDecimal(0, 0) on price: the precision must be at least 1")
    )
    assert(
      compileErrors("final case class Wrong(price: BigDecimal) derives AvroCodec").contains("xkafka.avro.AvroCodec[BigDecimal]"),
      "an unmarked BigDecimal still needs a codec"
    )

  test("a case class marked AvroFixed is a fixed of its size, written as its bytes alone, and named where it is used again"):
    @SchemaAlias("Digest") @AvroFixed(4)
    final case class Hash(bytes: Chunk[Byte]) derives AvroCodec
    @AvroFixed(size = 2)
    final case class Tag(bytes: Array[Byte]) derives AvroCodec
    final case class File(hash: Hash, previous: Option[Hash], tag: Tag) derives AvroCodec
    assertEquals(
      json(AvroCodec[File]),
      """{"type":"record","name":"File","namespace":"xkafka.avro","fields":[""" +
        """{"name":"hash","type":{"type":"fixed","name":"Hash","size":4,"aliases":["Digest"]}},""" +
        """{"name":"previous","type":["null","Hash"]},{"name":"tag","type":{"type":"fixed","name":"Tag","size":2}}]}"""
    )
    val file  = File(Hash(Chunk[Byte](1, 2, 3, 4)), None, Tag(Array[Byte](9, 8)))
    val bytes = AvroFormat[File].writer(AvroFormat[File].schema).flatMap(_(file))
    assertEquals(bytes, Right(Chunk[Byte](1, 2, 3, 4, 0, 9, 8)))
    assertEquals(roundTrip(file).map(read => (read.hash, read.previous, read.tag.bytes.toList)), Right((file.hash, None, List[Byte](9, 8))))
    assertEquals(AvroCodec[Hash].encode(Hash(Chunk[Byte](1, 2, 3))), Left("fixed 'xkafka.avro.Hash': 3 bytes, not 4"))

  test("AvroFixed on a case class without exactly one field of bytes, with a negative size, or documented, fails to compile"):
    assert(
      compileErrors("@AvroFixed(4) final case class Wrong(text: String) derives AvroCodec")
        .contains("@AvroFixed on Wrong, which must have one field, of fs2.Chunk[Byte] or Array[Byte]")
    )
    assert(
      compileErrors("@AvroFixed(4) final case class Wrong(a: Chunk[Byte], b: Chunk[Byte]) derives AvroCodec")
        .contains("@AvroFixed on Wrong, which must have one field")
    )
    assert(
      compileErrors("@AvroFixed(-1) final case class Wrong(a: Chunk[Byte]) derives AvroCodec")
        .contains("@AvroFixed(-1) on Wrong: the size must not be negative")
    )
    assert(
      compileErrors("@SchemaDoc(\"a hash\") @AvroFixed(4) final case class Wrong(a: Chunk[Byte]) derives AvroCodec")
        .contains("@SchemaDoc on Wrong, which @AvroFixed makes a fixed: an Avro fixed has no documentation")
    )

  test("an enum marked AvroEnumDefault declares its default, which a reader takes for a symbol it does not know"):
    @AvroEnumDefault("LOW")
    enum Level derives AvroCodec:
      case LOW, HIGH
    assertEquals(json(AvroCodec[Level]), """{"type":"enum","name":"Level","namespace":"xkafka.avro","symbols":["LOW","HIGH"],"default":"LOW"}""")
    val writer  = Schema(AvroSchemaJson.render(AvroSchema.Enum(FullName(Some("xkafka.avro"), "Level"), List("LOW", "MID", "HIGH"))), SchemaType.Avro)
    val written =
      AvroFormat(using AvroCodec.generic(AvroSchemaJson.parse(writer.text).toOption.get)).writer(writer)
        .flatMap(_(AvroValue.Enum(FullName(Some("xkafka.avro"), "Level"), "MID")))
    assertEquals(written.flatMap(bytes => AvroFormat[Level].reader(writer).flatMap(_(bytes))), Right(Level.LOW))

  test("a sealed trait of objects marked AvroEnumDefault takes a case's name for the schema, as SchemaName gives it"):
    @AvroEnumDefault("FAST")
    sealed trait Speed derives AvroCodec
    @SchemaName("FAST")
    case object Quick extends Speed
    case object Slow  extends Speed
    assertEquals(json(AvroCodec[Speed]), """{"type":"enum","name":"Speed","namespace":"xkafka.avro","symbols":["FAST","Slow"],"default":"FAST"}""")

  test("AvroEnumDefault naming no case, or on a type whose cases are not all objects, fails to compile"):
    assert(
      compileErrors("@AvroEnumDefault(\"MID\") enum Wrong derives AvroCodec { case LOW, HIGH }")
        .contains("@AvroEnumDefault(\"MID\") on Wrong, which has no case MID: LOW, HIGH")
    )
    assert(
      compileErrors("@AvroEnumDefault(\"A\") enum Wrong derives AvroCodec { case A; case B(x: Int) }")
        .contains("@AvroEnumDefault on Wrong, whose cases are not all objects, so it is not an Avro enum")
    )

  test("a union type is a union of its types in the order given, written as the type a value is of and read back by the record's name"):
    final case class Cash(cents: Long) derives AvroCodec
    final case class Refund(cents: Long) derives AvroCodec
    final case class Card(number: String) derives AvroCodec
    given AvroCodec[Cash | Refund | Card] = AvroCodec.union[(Cash, Refund, Card)]
    final case class Payment(method: Cash | Refund | Card, previous: Option[Cash | Refund | Card] = None) derives AvroCodec
    assertEquals(
      json(AvroCodec[Payment]),
      """{"type":"record","name":"Payment","namespace":"xkafka.avro","fields":[{"name":"method","type":[""" +
        """{"type":"record","name":"Cash","fields":[{"name":"cents","type":"long"}]},""" +
        """{"type":"record","name":"Refund","fields":[{"name":"cents","type":"long"}]},""" +
        """{"type":"record","name":"Card","fields":[{"name":"number","type":"string"}]}]},""" +
        """{"name":"previous","type":["null","Cash","Refund","Card"],"default":null}]}"""
    )
    List(Payment(Refund(5)), Payment(Cash(5), Some(Refund(2))), Payment(Card("4111"), Some(Card("0")))).foreach: payment =>
      assertEquals(roundTrip(payment), Right(payment))

  test("a union of primitive types tells them apart by the value's class"):
    given AvroCodec[Int | Long | String] = AvroCodec.union[(Int, Long, String)]
    assertEquals(json(AvroCodec[Int | Long | String]), """["int","long","string"]""")
    List[Int | Long | String](7, 7L, "7").foreach: value =>
      assertEquals(AvroCodec[Int | Long | String].encode(value).flatMap(AvroCodec[Int | Long | String].decode), Right(value))
    assertEquals(AvroCodec[Int | Long | String].encode(7L), Right(AvroValue.Long(7L)))

  test("bytes, sets, vectors and maps read back as they were written"):
    final case class Kinds(raw: Chunk[Byte], set: Set[Int], vector: Vector[Double], map: Map[String, Option[Boolean]], float: Float) derives AvroCodec
    val kinds = Kinds(Chunk[Byte](0, -1, 7), Set(3, 1), Vector(1.5, -2.25), Map("a" -> Some(true), "b" -> None), 0.5f)
    assertEquals(roundTrip(kinds), Right(kinds))

  test("byte arrays are written as bytes, and what was written does not change with the array"):
    final case class Blob(raw: Array[Byte], default: Array[Byte] = Array[Byte](1, 2)) derives AvroCodec
    val raw     = Array[Byte](0, -1, 7)
    val written = AvroCodec[Blob].encode(Blob(raw))
    raw(0) = 9
    val schema = AvroSchemaJson.render(AvroCodec[Blob].schema)
    assert(
      schema.contains("\"fields\":[{\"name\":\"raw\",\"type\":\"bytes\"},{\"name\":\"default\",\"type\":\"bytes\",\"default\":\"\\u0001\\u0002\"}]"),
      schema
    )
    assertEquals(
      written.flatMap(AvroCodec[Blob].decode).map(blob => (blob.raw.toList, blob.default.toList)),
      Right((List[Byte](0, -1, 7), List[Byte](1, 2)))
    )

  test("records written as one type are refused by a codec of another"):
    val bytes = AvroFormat[Line].writer(AvroFormat[Line].schema).flatMap(_(Line("x", 2)))
    val read  = bytes.flatMap(written => AvroFormat[Status].reader(AvroFormat[Line].schema).flatMap(_(written)))
    assert(read.left.exists(_.contains("cannot read what")), read.toString)
