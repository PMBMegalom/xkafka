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

import java.time.{Instant, LocalDate}
import java.util.UUID

import cats.data.NonEmptyList
import cats.effect.IO
import fs2.Chunk
import munit.CatsEffectSuite
import xkafka.{Headers, Topic}
import xkafka.protobuf.ProtobufSchema.{Enum, Field, FieldType, File, Label, Message, Syntax}
import xkafka.schemaregistry.*

class ProtobufGenericSuite extends CatsEffectSuite:
  private def parsed(text: String): File = ProtobufSchemaText.parse(text).fold(fail(_), identity)

  private val shop =
    """// A shop's orders
      |syntax = "proto3";
      |package com.example.shop;
      |
      |import "google/protobuf/timestamp.proto";
      |option java_package = "com.example";
      |
      |/* An order, with { braces } in a comment */
      |message Order {
      |  option deprecated = false;
      |  reserved 9, 10 to 12;
      |  reserved "old";
      |  string id = 1;
      |  google.protobuf.Timestamp placed = 2;
      |  repeated Line lines = 3;
      |  map<string, int64> tags = 4;
      |  optional string note = 5;
      |  repeated int32 counts = 6 [packed = false, deprecated = true, (custom.option) = { a: 1 }];
      |  oneof payment {
      |    Card card = 7;
      |    string voucher = 8;
      |  }
      |  Status status = 13;
      |  repeated Status history = 14;
      |  uint32 weight = 15;
      |  sint64 balance = 16;
      |  fixed64 code = 17;
      |  message Line {
      |    string sku = 1;
      |    int32 quantity = 2;
      |  }
      |  message Card { string last4 = 1; }
      |}
      |
      |enum Status {
      |  option allow_alias = true;
      |  STATUS_OPEN = 0;
      |  STATUS_PAID = 1;
      |  STATUS_OLD = -1;
      |}
      |
      |service Orders { rpc Place (Order) returns (Order) { option idempotency_level = IDEMPOTENT; } }
      |""".stripMargin

  private val shopFile =
    File(
      Syntax.Proto3,
      Some("com.example.shop"),
      List("google/protobuf/timestamp.proto"),
      List(Message(
        "Order",
        List(
          Field("id", 1, FieldType.String, Label.Implicit),
          Field("placed", 2, FieldType.Named("google.protobuf.Timestamp"), Label.Implicit),
          Field("lines", 3, FieldType.Named("Line"), Label.Repeated),
          Field("tags", 4, FieldType.Map(FieldType.String, FieldType.Int64), Label.Repeated),
          Field("note", 5, FieldType.String, Label.Optional),
          Field("counts", 6, FieldType.Int32, Label.Repeated, packed = Some(false)),
          Field("card", 7, FieldType.Named("Card"), Label.Optional, oneof = Some("payment")),
          Field("voucher", 8, FieldType.String, Label.Optional, oneof = Some("payment")),
          Field("status", 13, FieldType.Named("Status"), Label.Implicit),
          Field("history", 14, FieldType.Named("Status"), Label.Repeated),
          Field("weight", 15, FieldType.UInt32, Label.Implicit),
          Field("balance", 16, FieldType.SInt64, Label.Implicit),
          Field("code", 17, FieldType.Fixed64, Label.Implicit)
        ),
        List(
          Message("Line", List(Field("sku", 1, FieldType.String, Label.Implicit), Field("quantity", 2, FieldType.Int32, Label.Implicit))),
          Message("Card", List(Field("last4", 1, FieldType.String, Label.Implicit)))
        )
      )),
      List(Enum("Status", List("STATUS_OPEN" -> 0, "STATUS_PAID" -> 1, "STATUS_OLD" -> -1)))
    )

  private val legacy =
    """syntax = "proto2";
      |package legacy;
      |message Item {
      |  required string id = 1;
      |  optional int32 count = 2 [default = -5];
      |  optional string label = 3 [default = "none \"yet\""];
      |  repeated int32 sizes = 4 [packed = true];
      |  repeated int32 marks = 5;
      |}
      |""".stripMargin

  test("a proto3 file is read with its messages, nested types, oneofs, maps and enums, past comments, options, reservations and services"):
    assertEquals(parsed(shop), shopFile)

  test("a proto2 file is read with its labels, defaults as written, and packing"):
    assertEquals(
      parsed(legacy),
      File(
        Syntax.Proto2,
        Some("legacy"),
        Nil,
        List(Message(
          "Item",
          List(
            Field("id", 1, FieldType.String, Label.Required),
            Field("count", 2, FieldType.Int32, Label.Optional, default = Some("-5")),
            Field("label", 3, FieldType.String, Label.Optional, default = Some("\"none \\\"yet\\\"\"")),
            Field("sizes", 4, FieldType.Int32, Label.Repeated, packed = Some(true)),
            Field("marks", 5, FieldType.Int32, Label.Repeated)
          )
        )),
        Nil
      )
    )

  test("a file written out reads back as the same file"):
    List(shopFile, parsed(legacy), parsed(ProtobufCodec[Order].schema.fold(fail(_), identity))).foreach: file =>
      assertEquals(ProtobufSchemaText.parse(ProtobufSchemaText.render(file)), Right(file))

  test("groups, extensions and editions are refused, and a malformed file says where"):
    assertEquals(
      ProtobufSchemaText.parse("syntax = \"proto2\"; message A { optional group G = 1 { optional int32 a = 2; } }"),
      Left("the file declares a group, which xkafka does not read")
    )
    assertEquals(
      ProtobufSchemaText.parse("message A { extensions 100 to 200; } extend A { optional int32 b = 100; }"),
      Left("the file extends a message, and xkafka does not read extensions")
    )
    assertEquals(ProtobufSchemaText.parse("edition = \"2023\";"), Left("the file is written in an edition of Protobuf, which xkafka does not read"))
    assertEquals(ProtobufSchemaText.parse("message A { int32 a 1; }"), Left("expected '=' at token 6, found '1'"))

  test("the well-known types' files are all read"):
    assertEquals(WellKnown.files.keySet, WellKnown.texts.keySet)

  private val order =
    Order(
      UUID.fromString("3f2b8c1e-9a4d-4e6f-8b2a-1c3d5e7f9a0b"),
      Instant.parse("2026-11-10T07:00:00.123Z"),
      LocalDate.of(2026, 11, 10),
      List(Line("apple", 3), Line("pear")),
      Status.Paid,
      Transfer("DE00", Some(Instant.parse("2026-11-11T00:00:00Z"))),
      Some("ring twice"),
      Map("priority" -> 2L),
      1,
      Chunk[Byte](1, 2, 3),
      List(1, 2, 300),
      BigDecimal("12.50")
    )

  private def dynamic(file: File): Dynamic = new Dynamic(Pool.of(file, Map.empty).fold(fail(_), identity))

  test("a derived message read as a value is written back as the same bytes"):
    val codecs = dynamic(parsed(ProtobufCodec[Order].schema.fold(fail(_), identity)))
    val bytes  = ProtobufCodec[Order].encode(order).fold(fail(_), identity)
    val value  = codecs.decode("com.example.Order", bytes).fold(fail(_), identity)
    assertEquals(codecs.encode("com.example.Order", value), Right(bytes))
    assertEquals(
      value.field("lines"),
      Some(ProtobufValue.List(Vector(
        ProtobufValue.Message("com.example.Line", Vector("sku" -> ProtobufValue.String("apple"), "quantity" -> ProtobufValue.Int32(3))),
        ProtobufValue.Message("com.example.Line", Vector("sku" -> ProtobufValue.String("pear"), "quantity" -> ProtobufValue.Int32(1)))
      )))
    )
    assertEquals(value.field("status"), Some(ProtobufValue.Enum("STATUS_PAID")))
    assertEquals(value.field("tags"), Some(ProtobufValue.Map(Vector(ProtobufValue.String("priority") -> ProtobufValue.Int64(2)))))
    assertEquals(value.field("counts"), Some(ProtobufValue.List(Vector(1, 2, 300).map(ProtobufValue.Int32(_)))))

  test("fields without presence read as zero where absent, others are left out, and fields of unknown numbers are kept"):
    val codecs = dynamic(shopFile)
    val bytes  = Chunk[Byte](0x0a, 1, 'a', 0x92.toByte, 0x06, 2, 'h', 'i')
    val value  = codecs.decode("com.example.shop.Order", bytes).fold(fail(_), identity)
    assertEquals(
      value,
      ProtobufValue.Message(
        "com.example.shop.Order",
        Vector(
          "id"      -> ProtobufValue.String("a"),
          "lines"   -> ProtobufValue.List(Vector.empty),
          "tags"    -> ProtobufValue.Map(Vector.empty),
          "counts"  -> ProtobufValue.List(Vector.empty),
          "status"  -> ProtobufValue.Enum("STATUS_OPEN"),
          "history" -> ProtobufValue.List(Vector.empty),
          "weight"  -> ProtobufValue.UInt32(0),
          "balance" -> ProtobufValue.Int64(0),
          "code"    -> ProtobufValue.UInt64(0)
        ),
        Vector(UnknownField(98, Chunk[Byte](0x92.toByte, 0x06, 2, 'h', 'i')))
      )
    )
    assertEquals(codecs.encode("com.example.shop.Order", value), Right(bytes))

  test("numbers of every width, enums the file does not name, oneofs and unpacked lists are written and read back"):
    val codecs                       = dynamic(shopFile)
    val value: ProtobufValue.Message =
      ProtobufValue.Message(
        "com.example.shop.Order",
        Vector(
          "id"    -> ProtobufValue.String("b"),
          "lines" -> ProtobufValue.List(Vector(
            ProtobufValue.Message("com.example.shop.Order.Line", Vector("sku" -> ProtobufValue.String("fig"), "quantity" -> ProtobufValue.Int32(-2)))
          )),
          "tags"    -> ProtobufValue.Map(Vector(ProtobufValue.String("a") -> ProtobufValue.Int64(Long.MinValue))),
          "note"    -> ProtobufValue.String(""),
          "counts"  -> ProtobufValue.List(Vector(ProtobufValue.Int32(1), ProtobufValue.Int32(2))),
          "voucher" -> ProtobufValue.String("V1"),
          "status"  -> ProtobufValue.EnumNumber(7),
          "history" -> ProtobufValue.List(Vector(ProtobufValue.Enum("STATUS_PAID"), ProtobufValue.Enum("STATUS_OLD"))),
          "weight"  -> ProtobufValue.UInt32(4294967295L),
          "balance" -> ProtobufValue.Int64(-3),
          "code"    -> ProtobufValue.UInt64(BigInt("18446744073709551615"))
        )
      )
    val written = codecs.encode("com.example.shop.Order", value).fold(fail(_), identity)
    assertEquals(codecs.decode("com.example.shop.Order", written), Right(value))
    assert(written.toList.containsSlice(List[Byte](0x30, 1, 0x30, 2)), "counts are not packed, as the file says")

  test("of two map entries with one key, the later is read"):
    val entry = (value: Int) => Chunk[Byte](0x22, 5, 0x0a, 1, 'k', 0x10, value.toByte)
    val read  = dynamic(shopFile).decode("com.example.shop.Order", entry(1) ++ entry(2)).fold(fail(_), identity)
    assertEquals(read.field("tags"), Some(ProtobufValue.Map(Vector(ProtobufValue.String("k") -> ProtobufValue.Int64(2)))))

  test("a value that does not fit its message is refused, saying why"):
    val codecs                                                                 = dynamic(shopFile)
    def refused(fields: (String, ProtobufValue)*): Either[String, Chunk[Byte]] =
      codecs.encode("com.example.shop.Order", ProtobufValue.Message("com.example.shop.Order", fields.toVector))
    assertEquals(refused("price" -> ProtobufValue.Int32(1)), Left("com.example.shop.Order has no field price"))
    assertEquals(refused("id" -> ProtobufValue.Int32(1)).left.map(_.take(52)), Left("field id of com.example.shop.Order: expected a value"))
    assertEquals(
      refused("card" -> ProtobufValue.Message("com.example.shop.Order.Card", Vector.empty), "voucher" -> ProtobufValue.String("v")),
      Left("more than one field of oneof payment of com.example.shop.Order is given")
    )
    assertEquals(refused("weight" -> ProtobufValue.UInt32(-1)), Left("field weight of com.example.shop.Order: -1 is not an unsigned 32-bit number"))
    assertEquals(
      refused("status" -> ProtobufValue.Enum("CLOSED")),
      Left("field status of com.example.shop.Order: CLOSED is not a value of com.example.shop.Status")
    )
    assertEquals(
      codecs.encode("com.example.shop.Order", ProtobufValue.Message("com.example.shop.Other", Vector.empty)),
      Left("a message com.example.shop.Other is given where com.example.shop.Order is written")
    )
    assertEquals(
      dynamic(parsed(legacy)).encode("legacy.Item", ProtobufValue.Message("legacy.Item", Vector.empty)),
      Left("required field id of legacy.Item is not given")
    )

  test("a message is found by its path through the file's messages"):
    assertEquals(shopFile.message("com.example.shop.Order.Card").map(_._2), Some(List(0, 1)))
    assertEquals(shopFile.messageAt(List(0, 1)), Some("com.example.shop.Order.Card"))
    assertEquals(shopFile.messageAt(List(1)), None)

  private val settings = RegistrySettings.from(NonEmptyList.one("http://registry:8081")).toOption.get
  private val topic    = Topic.from("lines").toOption.get

  private def client: IO[SchemaRegistry[IO]] = FakeRegistry().flatMap(fake => SchemaRegistry.of(settings, fake.transport("http://registry:8081")))

  test("a generic serde writes values of a file it holds, which a derived serde reads, and reads what one writes"):
    val file = parsed(ProtobufCodec[Line].schema.fold(fail(_), identity))
    val line = ProtobufValue.Message("xkafka.protobuf.Line", Vector("sku" -> ProtobufValue.String("kiwi"), "quantity" -> ProtobufValue.Int32(4)))
    for
      registry <- client
      generic  <- ProtobufSerde.generic[IO](registry, file, "xkafka.protobuf.Line", Part.Value)
      written  <- generic.serializer.serialize(topic, Headers.empty, line)
      typed    <- ProtobufSerde.of[IO, Line](registry, Part.Value)
      read     <- typed.deserializer.deserialize(topic, Headers.empty, written)
      back     <- typed.serializer.serialize(topic, Headers.empty, Line("lime", 0)).flatMap(generic.deserializer.deserialize(topic, Headers.empty, _))
      refused  <-
        generic.serializer.serialize(topic, Headers.empty, ProtobufValue.Message("xkafka.protobuf.Line", Vector("size" -> ProtobufValue.Int32(1))))
          .attempt
    yield
      assertEquals(read, Line("kiwi", 4))
      assertEquals(back.field("quantity"), Some(ProtobufValue.Int32(0)))
      refused match
        case Left(error: RegistryException.Unserializable) => assertEquals(error.reason, "xkafka.protobuf.Line has no field size")
        case other                                         => fail(s"expected unserializable, got $other")

  test("a generic serde of a file that imports other files, or lacks the message, fails, saying why"):
    for
      registry <- client
      imports  <- ProtobufSerde.generic[IO](registry, parsed("syntax = \"proto3\"; import \"money.proto\"; message A {}"), "A", Part.Value).attempt
      missing  <- ProtobufSerde.generic[IO](registry, shopFile, "com.example.shop.Missing", Part.Value).attempt
    yield
      imports match
        case Left(error: RegistryException.NoSchema) => assert(error.reason.startsWith("the schema imports money.proto"), error.reason)
        case other                                   => fail(s"expected no schema, got $other")
      missing match
        case Left(error: RegistryException.NoSchema) => assertEquals(error.reason, "the schema declares no message com.example.shop.Missing")
        case other                                   => fail(s"expected no schema, got $other")

  test("records read as written carry their file and message, with the well-known types and the files the schema's references name"):
    val money = Schema("syntax = \"proto3\";\npackage bank;\nmessage Money { int64 cents = 1; }\n", SchemaType.Protobuf)
    val text  =
      "syntax = \"proto3\";\npackage shop;\nimport \"money.proto\";\nmessage Header {}\nmessage Price { string sku = 1; bank.Money amount = 2; message Note { string text = 1; } }\n"
    for
      registry <- client
      _        <- registry.register(Subject.from("money").toOption.get, money)
      id       <-
        registry.register(
          Subject.from("prices").toOption.get,
          Schema(text, SchemaType.Protobuf, List(SchemaReference("money.proto", Subject.from("money").toOption.get, 1)))
        )
      pool  = Pool.of(parsed(text), Map("money.proto" -> parsed(money.text))).fold(fail(_), identity)
      price =
        new ProtobufValue.Message(
          "shop.Price",
          Vector("sku" -> ProtobufValue.String("fig"), "amount" -> ProtobufValue.Message("bank.Money", Vector("cents" -> ProtobufValue.Int64(250))))
        )
      payload = new Dynamic(pool).encode("shop.Price", price).fold(fail(_), identity)
      prefix  = Chunk[Byte](0, (id.value >>> 24).toByte, (id.value >>> 16).toByte, (id.value >>> 8).toByte, id.value.toByte)
      reading <- ProtobufSerde.asWritten[IO](registry)
      read    <- reading.deserialize(topic, Headers.empty, Some(prefix ++ ProtobufWire.indexes(List(1)) ++ payload))
      note    <- reading.deserialize(topic, Headers.empty, Some(prefix ++ ProtobufWire.indexes(List(1, 0)) ++ Chunk[Byte](0x0a, 2, 'h', 'i')))
      derived <- ProtobufSerde.of[IO, Order](registry, Part.Value).flatMap(_.serializer.serialize(topic, Headers.empty, order))
      ordered <- reading.deserialize(topic, Headers.empty, derived)
    yield
      assertEquals(read, ProtobufWritten(parsed(text), price))
      assertEquals(note.value, ProtobufValue.Message("shop.Price.Note", Vector("text" -> ProtobufValue.String("hi"))))
      assertEquals(ordered.value.name, "com.example.Order")
      assertEquals(
        ordered.value.field("placed_at"),
        Some(
          ProtobufValue
            .Message("google.protobuf.Timestamp", Vector("seconds" -> ProtobufValue.Int64(1794294000L), "nanos" -> ProtobufValue.Int32(123000000)))
        )
      )
