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

package xkafka.protobuf

import java.util.UUID

import example.proto.*
import fs2.Chunk
import munit.FunSuite
import smithy4s.{Blob, Document, Schema as SmithySchema}
import smithy4s.time.{LocalDate, LocalTime, OffsetDateTime, Timestamp}

/** Values of the types smithy4s generates from src/test/smithy, as the Protobuf tests write them. */
object Till:
  val least: Payment = Payment(UUID.fromString("123e4567-e89b-12d3-a456-426614174000"), Timestamp(1700000000L, 5))

  val most: Payment =
    least.copy(
      amount = Some(10.5),
      cents = Some(1050L),
      delta = Some(-5L),
      hash = Some(-1),
      count = Some(7),
      small = Some((-2).toByte),
      short = Some(300.toShort),
      rate = Some(0.25f),
      paid = Some(true),
      note = Some("thanks"),
      big = Some(BigDecimal("12345678901234567.89")),
      huge = Some(BigInt("123456789012345678901234567890")),
      receipt = Some(Blob("hi")),
      day = Some(LocalDate.parseUnsafe("2024-01-02")),
      extra = Some(Document.obj("a" -> Document.fromBoolean(true), "b" -> Document.array(Document.fromString("c")))),
      tip = Some(0L),
      method = Some(Method.card(Card("4242"))),
      tender = Some(Tender(Some("till"), Some(Detail.code(9)))),
      status = Some(Status.SETTLED),
      tags = Some(List("x", "y")),
      totals = Some(Map("z" -> 3L)),
      lines = Some(List(Line("a"), Line("b", 2))),
      previous = Some(least),
      offset = Some(-3L),
      opens = Some(LocalTime.parseUnsafe("09:30:00")),
      when = Some(OffsetDateTime.parseUnsafe("2024-01-02T03:04:05+02:00")),
      _wait = Some(scala.concurrent.duration.Duration(90, "seconds"))
    )

/** Types smithy4s generates, written and read with the .proto file of the messages smithy4s writes. */
class Smithy4sProtobufSuite extends FunSuite:
  import Till.*

  private def file[A](using schema: SmithySchema[A]): ProtobufSchema.File = Smithy4sProtobufSchema.of(schema).fold(fail(_), identity)

  private def roundTrip[A: SmithySchema](value: A): Either[String, A] =
    for
      format <- Smithy4sProtobufFormat[A]
      write  <- format.writer(format.schema)
      bytes  <- write(value)
      read   <- format.reader(format.schema)
      back   <- read(bytes)
    yield back

  /** What `value` holds that its file does not declare. */
  private def unknown(value: ProtobufValue): Vector[UnknownField] =
    value match
      case ProtobufValue.Message(_, fields, unknown) => unknown ++ fields.flatMap(field => this.unknown(field._2))
      case ProtobufValue.List(values)                => values.flatMap(this.unknown)
      case ProtobufValue.Map(entries)                => entries.flatMap(entry => this.unknown(entry._2))
      case _                                         => Vector.empty

  /** The message smithy4s writes of `value`, as xkafka reads it with the file made of its schema. */
  private def read[A](value: A)(using schema: SmithySchema[A]): ProtobufValue.Message =
    val bytes = Chunk.array(smithy4s.protobuf.Protobuf.codecs.fromSchema(schema).writeBlob(value).toArray)
    Pool.of(file[A], Map.empty).flatMap(pool => new Dynamic(pool).decode(s"example.proto.${schema.shapeId.name}", bytes)).fold(fail(_), identity)

  test("a structure's file declares its message, the messages and enums it holds, and the well-known types it uses"):
    assertEquals(
      ProtobufSchemaText.render(file[Payment]),
      """syntax = "proto3";
        |
        |package example.proto;
        |
        |import "google/protobuf/duration.proto";
        |import "google/protobuf/struct.proto";
        |import "google/protobuf/timestamp.proto";
        |import "google/protobuf/wrappers.proto";
        |
        |message Payment {
        |  string id = 1;
        |  .google.protobuf.Timestamp at = 2;
        |  double amount = 3;
        |  int64 cents = 4;
        |  sint64 delta = 5;
        |  fixed32 hash = 6;
        |  uint32 count = 7;
        |  int32 small = 8;
        |  int32 short = 9;
        |  float rate = 10;
        |  bool paid = 11;
        |  string note = 12;
        |  string big = 13;
        |  string huge = 14;
        |  bytes receipt = 15;
        |  string day = 16;
        |  .google.protobuf.Value extra = 17;
        |  .google.protobuf.Int64Value tip = 18;
        |  Method method = 19;
        |  Tender tender = 20;
        |  Status status = 21;
        |  repeated string tags = 22;
        |  map<string, int64> totals = 23;
        |  repeated Line lines = 24;
        |  Payment previous = 25;
        |  sfixed64 offset = 26;
        |  string opens = 27;
        |  string when = 28;
        |  .google.protobuf.Duration wait = 29;
        |}
        |
        |message Card {
        |  string last4 = 1;
        |}
        |
        |message Cash {
        |  double given = 1;
        |}
        |
        |message Line {
        |  string sku = 1;
        |  int32 quantity = 2;
        |}
        |
        |message Method {
        |  oneof definition {
        |    Cash cash = 1;
        |    Card card = 2;
        |  }
        |}
        |
        |message Tender {
        |  string label = 1;
        |  oneof detail {
        |    string text = 3;
        |    int32 code = 4;
        |  }
        |}
        |
        |enum Status {
        |  PENDING = 0;
        |  SETTLED = 1;
        |  REFUSED = 2;
        |}
        |""".stripMargin
    )

  test("members and enum values numbered with @protoIndex keep their numbers"):
    val receipt = file[Receipt]
    assertEquals(
      receipt.messages.map(message => message.name -> message.fields.map(field => field.name -> field.number)),
      List("Receipt" -> List("number" -> 3, "kind" -> 7))
    )
    assertEquals(receipt.enums.map(found => found.name -> found.values), List("Kind" -> List("UNKNOWN" -> 0, "COPY" -> 5)))

  test("an enum whose value numbered 0 is not its first declares that value first, as proto3 needs"):
    assertEquals(file[Ticket].enums.map(found => found.name -> found.values), List("Grade" -> List("NONE" -> 0, "HIGH" -> 4)))

  test("every field smithy4s writes is declared by the file, as the type it is written as"):
    val message = read(most)
    assertEquals(unknown(message), Vector.empty)
    assertEquals(message.field("delta"), Some(ProtobufValue.Int64(-5L)))
    assertEquals(message.field("hash"), Some(ProtobufValue.UInt32(4294967295L)))
    assertEquals(message.field("count"), Some(ProtobufValue.UInt32(7L)))
    assertEquals(message.field("offset"), Some(ProtobufValue.Int64(-3L)))
    assertEquals(
      (message.field("opens"), message.field("when")),
      (Some(ProtobufValue.String("09:30:00")), Some(ProtobufValue.String("2024-01-02T03:04:05+02:00")))
    )
    assertEquals(
      message.field("wait"),
      Some(ProtobufValue.Message("google.protobuf.Duration", Vector("seconds" -> ProtobufValue.Int64(90L), "nanos" -> ProtobufValue.Int32(0))))
    )
    assertEquals((message.field("small"), message.field("short")), (Some(ProtobufValue.Int32(-2)), Some(ProtobufValue.Int32(300))))
    assertEquals(message.field("rate"), Some(ProtobufValue.Float(0.25f)))
    assertEquals(message.field("big"), Some(ProtobufValue.String("12345678901234567.89")))
    assertEquals(message.field("tip"), Some(ProtobufValue.Message("google.protobuf.Int64Value", Vector("value" -> ProtobufValue.Int64(0L)))))
    assertEquals(message.field("tender").flatMap(_.field("code")), Some(ProtobufValue.Int32(9)))
    assertEquals(message.field("status"), Some(ProtobufValue.Enum("SETTLED")))
    assertEquals(
      message.field("at"),
      Some(
        ProtobufValue.Message("google.protobuf.Timestamp", Vector("seconds" -> ProtobufValue.Int64(1700000000L), "nanos" -> ProtobufValue.Int32(5)))
      )
    )
    assertEquals(message.field("totals"), Some(ProtobufValue.Map(Vector(ProtobufValue.String("z") -> ProtobufValue.Int64(3L)))))
    assertEquals(
      message.field("method"),
      Some(ProtobufValue.Message(
        "example.proto.Method",
        Vector("card" -> ProtobufValue.Message("example.proto.Card", Vector("last4" -> ProtobufValue.String("4242"))))
      ))
    )
    assertEquals(message.field("extra").flatMap(_.field("struct_value")).map(unknown), Some(Vector.empty))
    assertEquals(unknown(read(Receipt("r-1", Some(Kind.COPY)))), Vector.empty)

  test("values are written as smithy4s writes them, after the message's place in the file, and read back"):
    val written = Smithy4sProtobufFormat[Payment].flatMap(format => format.writer(format.schema).flatMap(_(most)))
    assertEquals(
      written,
      Right(ProtobufWire.indexes(List(0)) ++ Chunk.array(smithy4s.protobuf.Protobuf.codecs.fromSchema(Payment.schema).writeBlob(most).toArray))
    )
    assertEquals(roundTrip(most), Right(most))
    assertEquals(roundTrip(least), Right(least))
    val other = most.copy(tender = Some(Tender(None, Some(Detail.text("t")))), method = Some(Method.cash(Cash(1))))
    assertEquals(roundTrip(other), Right(other))

  test("the format names the message by its full name"):
    assertEquals(Smithy4sProtobufFormat[Payment].map(_.recordName), Right(Some("example.proto.Payment")))

  test("what a .proto file cannot declare is refused, saying why"):
    assertEquals(
      Smithy4sProtobufSchema.of(Clash.schema).map(ProtobufSchemaText.render),
      Left("example.proto#Clash numbers two fields 1, as where an inlined union's members are not numbered apart from its own")
    )
    assertEquals(
      Smithy4sProtobufSchema.of(example.shop.Order.schema).map(ProtobufSchemaText.render),
      Left("example.shop#Order$sparse is a list of lists, maps, inlined unions or nulls, which a repeated field cannot hold")
    )
    assertEquals(
      Smithy4sProtobufSchema.of(Status.schema).map(ProtobufSchemaText.render),
      Left("example.proto#Status is neither a structure nor a union, which a message is made of")
    )
    def refused[A](schema: SmithySchema[A]) =
      Smithy4sProtobufSchema.of(schema).map(ProtobufSchemaText.render).left.getOrElse(fail(s"${schema.shapeId} is declared"))
    assertEquals(
      refused(Lists.schema),
      "example.proto#Lists$lists is a list of lists, maps, inlined unions or nulls, which a repeated field cannot hold"
    )
    assertEquals(refused(MapOfLists.schema), "example.proto#MapOfLists$byName is a map whose keys or values a Protobuf map cannot hold")
    assertEquals(refused(UnionOfList.schema), "example.proto#HoldsTags$tags is a list, a map or a union inlined, which a oneof cannot hold")
    assertEquals(refused(Compact.schema), "example.proto#Compact$id is a UUID with @protoCompactUUID, which xkafka does not declare yet")
    assertEquals(refused(SameValues.schema), "two enums have the value SAME, which one package cannot declare twice")
    assertEquals(refused(SameNames.schema), "two shapes are named Card, which one file cannot declare")
    assertEquals(refused(CompactDay.schema), "example.proto#CompactDay$day is a date with @protoCompactLocalDate, which xkafka does not declare yet")
    assertEquals(
      refused(CompactTime.schema),
      "example.proto#CompactTime$opens is a time with @protoCompactLocalTime, which xkafka does not declare yet"
    )
    assertEquals(refused(Millis.schema), "example.proto#Millis$at is a timestamp in epoch milliseconds, which xkafka does not declare yet")
    assertEquals(
      refused(ProtobufOffset.schema),
      "example.proto#ProtobufOffset$when is a date and time in the Protobuf format, which xkafka does not declare yet"
    )
    assertEquals(
      refused(WrappedTime.schema),
      "example.proto#WrappedTime$at has @protoWrapped on a type without a well-known wrapper, which xkafka does not declare yet"
    )
    assertEquals(refused(WrappedList.schema), "example.proto#WrappedList$tags is a list with @protoWrapped, which xkafka does not declare yet")
