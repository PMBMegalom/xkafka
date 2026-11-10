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

import scala.compiletime.testing.typeCheckErrors

import cats.data.NonEmptyList
import cats.effect.IO
import fs2.Chunk
import munit.CatsEffectSuite
import xkafka.{Headers, Topic}
import xkafka.schemaregistry.*

final case class Line(
    @ProtobufField(1)
    sku: String,
    @ProtobufField(2)
    quantity: Int = 1
) derives ProtobufCodec

@SchemaName("Line")
final case class LineBefore(
    @ProtobufField(1)
    sku: String
) derives ProtobufCodec

enum Status derives ProtobufCodec:
  @ProtobufField(0)
  case Open
  @ProtobufField(1)
  case Paid

sealed trait Payment derives ProtobufCodec

@ProtobufField(1)
final case class Card(
    @ProtobufField(1)
    last4: String
) extends Payment

@ProtobufField(2)
case object Cash extends Payment

@ProtobufField(3)
final case class Transfer(
    @ProtobufField(1)
    iban: String,
    @SchemaDoc("When it cleared") @ProtobufField(2)
    cleared: Option[Instant] = None
) extends Payment

@SchemaNamespace("com.example") @SchemaDoc("An order")
final case class Order(
    @ProtobufField(1)
    id: UUID,
    @SchemaName("placed_at") @ProtobufField(2)
    placedAt: Instant,
    @ProtobufField(3)
    day: LocalDate,
    @ProtobufField(4)
    lines: List[Line],
    @ProtobufField(5)
    status: Status,
    @ProtobufField(6)
    payment: Payment,
    @ProtobufField(7)
    note: Option[String] = None,
    @ProtobufField(8)
    tags: Map[String, Long] = Map.empty,
    @ProtobufField(9)
    retries: Int = 0,
    @ProtobufField(10)
    raw: Chunk[Byte] = Chunk.empty,
    @ProtobufField(11)
    counts: List[Int] = Nil,
    @ProtobufField(12)
    total: BigDecimal = BigDecimal(0)
) derives ProtobufCodec

final case class Node(
    @ProtobufField(1)
    value: Int,
    @ProtobufField(2)
    next: Option[Node] = None
) derives ProtobufCodec

final case class Plain(
    @ProtobufField(1)
    count: Int,
    @ProtobufField(2)
    name: String
) derives ProtobufCodec

final case class Held(
    @ProtobufField(1)
    status: Status
) derives ProtobufCodec

final case class Defaulted(
    @ProtobufField(1)
    tags: List[String] = List("a"),
    @ProtobufField(2)
    note: Option[String] = Some("x"),
    @ProtobufField(3)
    counts: Map[String, Int] = Map("a" -> 1)
) derives ProtobufCodec

final case class Maybes(
    @ProtobufField(1)
    values: List[Option[Int]]
) derives ProtobufCodec

final case class MaybeList(
    @ProtobufField(1)
    values: Option[List[Int]]
) derives ProtobufCodec

final case class Counted(
    @ProtobufField(1) @SchemaNoDefault
    count: Int = 5
) derives ProtobufCodec

final case class Nested(
    @ProtobufField(1)
    rows: List[List[Int]]
) derives ProtobufCodec

@SchemaNamespace("com.other") @SchemaName("Line")
final case class OtherLine(
    @ProtobufField(1)
    code: Int
) derives ProtobufCodec

final case class Both(
    @ProtobufField(1)
    line: Line,
    @ProtobufField(2)
    other: OtherLine
) derives ProtobufCodec

object Zigzag:
  given ProtobufCodec[Int] = ProtobufCodec.sint32

  final case class Delta(
      @ProtobufField(1)
      change: Int
  ) derives ProtobufCodec

class ProtobufCodecSuite extends CatsEffectSuite:
  private def bytes(values: Int*): Chunk[Byte] = Chunk.from(values.map(_.toByte))

  private def schema[A](codec: ProtobufCodec[A]): String = codec.schema.fold(fail(_), identity)

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

  test("a case class is a message of its numbered fields, those with a Scala default optional"):
    assertEquals(
      schema(ProtobufCodec[Line]),
      """syntax = "proto3";
        |
        |package xkafka.protobuf;
        |
        |message Line {
        |  string sku = 1;
        |  optional int32 quantity = 2;
        |}
        |""".stripMargin
    )

  test("fields are written in order of their numbers, one with a default even where it is zero, and one without left out where it is zero"):
    assertEquals(ProtobufCodec[Line].encode(Line("apple", 3)), Right(bytes(0x0a, 5, 'a', 'p', 'p', 'l', 'e', 0x10, 3)))
    assertEquals(ProtobufCodec[Line].encode(Line("", 0)), Right(bytes(0x10, 0)))
    assertEquals(ProtobufCodec[Plain].encode(Plain(0, "")), Right(Chunk.empty))
    assertEquals(ProtobufCodec[Plain].decode(Chunk.empty), Right(Plain(0, "")))
    assertEquals(ProtobufCodec[Plain].encode(Plain(-1, "")), Right(bytes(0x08, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01)))
    final case class Swapped(
        @ProtobufField(2)
        count: Int,
        @ProtobufField(1)
        name: String
    ) derives ProtobufCodec
    assertEquals(ProtobufCodec[Swapped].encode(Swapped(1, "a")), Right(bytes(0x0a, 1, 'a', 0x10, 1)))

  test("a field whose default is left out with SchemaNoDefault is written and read as a field without one"):
    assert(schema(ProtobufCodec[Counted]).contains("  int32 count = 1;"))
    assertEquals(ProtobufCodec[Counted].encode(Counted(0)), Right(Chunk.empty))
    assertEquals(ProtobufCodec[Counted].decode(Chunk.empty), Right(Counted(0)))

  test("lists, maps and options do not use their defaults: a record lacking one reads it as empty or none"):
    assertEquals(ProtobufCodec[Defaulted].decode(Chunk.empty), Right(Defaulted(Nil, None, Map.empty)))
    val text = schema(ProtobufCodec[Defaulted])
    assert(text.contains("  repeated string tags = 1;\n  optional string note = 2;\n  map<string, int32> counts = 3;"), text)

  test("a record lacking a field with a default reads the default, and fields the codec does not know are skipped"):
    val before = ProtobufCodec[LineBefore].encode(LineBefore("apple")).fold(fail(_), identity)
    assertEquals(ProtobufCodec[Line].decode(before), Right(Line("apple", 1)))
    assertEquals(ProtobufCodec[Line].encode(Line("pear", 4)).flatMap(ProtobufCodec[LineBefore].decode), Right(LineBefore("pear")))

  test("an enum of objects is a Protobuf enum whose values carry its name, the one numbered zero read where the field is absent"):
    val text = schema(ProtobufCodec[Held])
    assert(text.contains("enum Status {\n  STATUS_OPEN = 0;\n  STATUS_PAID = 1;\n}"), text)
    assertEquals(ProtobufCodec[Held].encode(Held(Status.Paid)), Right(bytes(0x08, 1)))
    assertEquals(ProtobufCodec[Held].encode(Held(Status.Open)), Right(Chunk.empty))
    assertEquals(ProtobufCodec[Held].decode(Chunk.empty), Right(Held(Status.Open)))
    assertEquals(ProtobufCodec[Held].decode(bytes(0x08, 7)), Left("xkafka.protobuf.Held: 7 is not a case of xkafka.protobuf.Status"))

  test("a sealed trait is a message of a oneof of its cases, each under its number"):
    val text = schema(ProtobufCodec[Payment])
    assert(
      text.contains(
        "message Payment {\n  oneof payment {\n    Card card = 1;\n    Cash cash = 2;\n    Transfer transfer = 3;\n  }\n}\n\nmessage Card {\n  string last4 = 1;\n}\n\nmessage Cash {\n}"
      ),
      text
    )
    assertEquals(ProtobufCodec[Payment].encode(Card("4242")), Right(bytes(0x0a, 6, 0x0a, 4, '4', '2', '4', '2')))
    assertEquals(ProtobufCodec[Payment].encode(Cash), Right(bytes(0x12, 0)))
    assertEquals(ProtobufCodec[Payment].decode(bytes(0x12, 0)), Right(Cash))
    assertEquals(ProtobufCodec[Payment].decode(bytes(0x0a, 0, 0x12, 0)), Right(Cash), "of two cases written, the last is the one set")
    assertEquals(ProtobufCodec[Payment].decode(Chunk.empty), Left("no case of xkafka.protobuf.Payment is set"))

  test("a message holding others declares them all in its package, with comments of their documentation"):
    assertEquals(
      schema(ProtobufCodec[Order]),
      """syntax = "proto3";
        |
        |package com.example;
        |
        |import "google/protobuf/timestamp.proto";
        |
        |// An order
        |message Order {
        |  string id = 1;
        |  google.protobuf.Timestamp placed_at = 2;
        |  string day = 3;
        |  repeated Line lines = 4;
        |  Status status = 5;
        |  Payment payment = 6;
        |  optional string note = 7;
        |  map<string, int64> tags = 8;
        |  optional int32 retries = 9;
        |  optional bytes raw = 10;
        |  repeated int32 counts = 11;
        |  optional string total = 12;
        |}
        |
        |message Line {
        |  string sku = 1;
        |  optional int32 quantity = 2;
        |}
        |
        |enum Status {
        |  STATUS_OPEN = 0;
        |  STATUS_PAID = 1;
        |}
        |
        |message Payment {
        |  oneof payment {
        |    Card card = 1;
        |    Cash cash = 2;
        |    Transfer transfer = 3;
        |  }
        |}
        |
        |message Card {
        |  string last4 = 1;
        |}
        |
        |message Cash {
        |}
        |
        |message Transfer {
        |  string iban = 1;
        |  // When it cleared
        |  google.protobuf.Timestamp cleared = 2;
        |}
        |""".stripMargin
    )

  test("values of every kind read back as they were written"):
    val written = ProtobufCodec[Order].encode(order)
    assertEquals(written.flatMap(ProtobufCodec[Order].decode), Right(order))
    val plain = order.copy(payment = Cash, note = None, lines = Nil, tags = Map.empty, counts = Nil, placedAt = Instant.EPOCH)
    assertEquals(ProtobufCodec[Order].encode(plain).flatMap(ProtobufCodec[Order].decode), Right(plain))

  test("numbers in a list are packed, and read packed or not"):
    final case class Counts(
        @ProtobufField(1)
        counts: List[Int]
    ) derives ProtobufCodec
    assertEquals(ProtobufCodec[Counts].encode(Counts(List(1, 2, 300))), Right(bytes(0x0a, 4, 1, 2, 0xac, 0x02)))
    assertEquals(ProtobufCodec[Counts].decode(bytes(0x08, 1, 0x08, 2)), Right(Counts(List(1, 2))))

  test("a codec given in place of the default one is the one a derived message takes"):
    assert(schema(ProtobufCodec[Zigzag.Delta]).contains("sint32 change = 1;"))
    assertEquals(ProtobufCodec[Zigzag.Delta].encode(Zigzag.Delta(-1)), Right(bytes(0x08, 1)))
    assertEquals(ProtobufCodec[Zigzag.Delta].decode(bytes(0x08, 3)), Right(Zigzag.Delta(-2)))

  test("a type that holds itself is a message whose field names it"):
    assert(schema(ProtobufCodec[Node]).contains("message Node {\n  int32 value = 1;\n  Node next = 2;\n}"))
    val node = Node(1, Some(Node(2, Some(Node(3)))))
    assertEquals(ProtobufCodec[Node].encode(node).flatMap(ProtobufCodec[Node].decode), Right(node))

  test("types Protobuf cannot declare have no schema, saying why"):
    assertEquals(
      ProtobufCodec[Nested].schema,
      Left("field rows of xkafka.protobuf.Nested: a list holding lists cannot be declared, as Protobuf has no field that holds it")
    )
    assertEquals(
      ProtobufCodec[Maybes].schema,
      Left("field values of xkafka.protobuf.Maybes: a list holding options cannot be declared, as Protobuf has no field that holds it")
    )
    assertEquals(
      ProtobufCodec[MaybeList].schema,
      Left("field values of xkafka.protobuf.MaybeList: an option holding lists cannot be declared, as Protobuf has no field that holds it")
    )
    assertEquals(ProtobufCodec[Int].schema, Left("int32 is not a message, and a Protobuf record is one"))
    assertEquals(ProtobufCodec[Both].schema, Left("xkafka.protobuf.Line and com.other.Line would both be declared as Line"))

  test("an instant outside the years a Timestamp holds is refused"):
    final case class When(
        @ProtobufField(1)
        at: Instant
    ) derives ProtobufCodec
    assert(ProtobufCodec[When].encode(When(Instant.MAX)).isLeft)

  test("a field or case without a number, two sharing one, a number Protobuf does not allow, and an enum without a zero do not compile"):
    assertEquals(
      typeCheckErrors("final case class Unnumbered(@ProtobufField(1) a: Int, b: Int) derives ProtobufCodec").map(_.message),
      List("field b of Unnumbered has no @ProtobufField number, which Protobuf writes it under")
    )
    assertEquals(
      typeCheckErrors("final case class Shared(@ProtobufField(1) a: Int, @ProtobufField(1) b: Int) derives ProtobufCodec").map(_.message),
      List("fields a and b of Shared share number 1")
    )
    assertEquals(
      typeCheckErrors("final case class Reserved(@ProtobufField(19500) a: Int) derives ProtobufCodec").map(_.message),
      List("field a of Reserved is numbered 19500, and Protobuf numbers run from 1 to 536870911, leaving out 19000 to 19999")
    )
    assertEquals(
      typeCheckErrors("enum Light derives ProtobufCodec { @ProtobufField(1) case Red; @ProtobufField(2) case Green }").map(_.message),
      List("enum Light has no case numbered 0, which Protobuf reads where a field of it is absent")
    )

  test("the messages a file declares outside any other are found, past comments and quoted text"):
    val text =
      """syntax = "proto3"; // message Commented {
        |/* message Hidden { */
        |option java_package = "message.Quoted";
        |message First { message Inner { int32 a = 1; } Inner inner = 1; }
        |enum Kind { KIND_A = 0; }
        |message Second {}
        |""".stripMargin
    assertEquals(ProtobufFormat.messages(text), List("First", "Second"))

  private val settings = RegistrySettings.from(NonEmptyList.one("http://registry:8081")).toOption.get
  private val topic    = Topic.from("lines").toOption.get

  test("serdes write the schema's id, the message's index and the message, and read records written before a field was added"):
    for
      fake    <- FakeRegistry()
      client  <- SchemaRegistry.of(settings, fake.transport("http://registry:8081"))
      before  <- ProtobufSerde.of[IO, LineBefore](client, Part.Value).flatMap(_.serializer.serialize(topic, Headers.empty, LineBefore("apple")))
      read    <- ProtobufSerde.of[IO, Line](client, Part.Value).flatMap(_.deserializer.deserialize(topic, Headers.empty, before))
      subject <- client.latest(Subject.from("lines-value").toOption.get)
    yield
      assertEquals(before.map(_.drop(5)), Some(bytes(0, 0x0a, 5, 'a', 'p', 'p', 'l', 'e')))
      assertEquals(read, Line("apple", 1))
      assertEquals(subject.map(_.schema.schemaType), Some(SchemaType.Protobuf))

  test("writing with a schema whose first message is another names the message by its place"):
    val text =
      "syntax = \"proto3\";\npackage xkafka.protobuf;\nmessage Header { string id = 1; }\nmessage Line { string sku = 1; optional int32 quantity = 2; }\n"
    for
      fake    <- FakeRegistry()
      _       <- fake.registered("lines-value", text, "PROTOBUF")
      client  <- SchemaRegistry.of(settings, fake.transport("http://registry:8081"))
      serde   <- ProtobufSerde.of[IO, Line](client, Part.Value, writing = Writing.Latest)
      written <- serde.serializer.serialize(topic, Headers.empty, Line("fig", 2))
      read    <- serde.deserializer.deserialize(topic, Headers.empty, written)
    yield
      assertEquals(written.map(_.drop(5).take(2)), Some(bytes(2, 2)))
      assertEquals(read, Line("fig", 2))

  test("writing with a schema that declares no message of the type's name is refused"):
    val text = "syntax = \"proto3\";\nmessage Header { string id = 1; }\n"
    for
      fake    <- FakeRegistry()
      id      <- fake.registered("other", text, "PROTOBUF")
      client  <- SchemaRegistry.of(settings, fake.transport("http://registry:8081"))
      serde   <- ProtobufSerde.of[IO, Line](client, Part.Value, writing = Writing.Pinned(SchemaId(id)))
      refused <- serde.serializer.serialize(topic, Headers.empty, Line("fig")).attempt
    yield refused match
      case Left(error: RegistryException.Unserializable) => assertEquals(error.reason, "the schema declares no message Line")
      case other                                         => fail(s"expected unserializable, got $other")

  test("records whose schema names schemas registered under other subjects are written and read"):
    val money = Schema("syntax = \"proto3\";\nmessage Money { int64 cents = 1; }\n", SchemaType.Protobuf)
    for
      fake   <- FakeRegistry()
      client <- SchemaRegistry.of(settings, fake.transport("http://registry:8081"))
      _      <- client.register(Subject.from("money").toOption.get, money)
      uses =
        Schema(
          "syntax = \"proto3\";\npackage xkafka.protobuf;\nimport \"money.proto\";\nmessage Line { string sku = 1; optional int32 quantity = 2; Money price = 3; }\n",
          SchemaType.Protobuf,
          List(SchemaReference("money.proto", Subject.from("money").toOption.get, 1))
        )
      id      <- client.register(Subject.from("lines-value").toOption.get, uses)
      serde   <- ProtobufSerde.of[IO, Line](client, Part.Value, writing = Writing.Pinned(id))
      written <- serde.serializer.serialize(topic, Headers.empty, Line("fig", 2))
      read    <- serde.deserializer.deserialize(topic, Headers.empty, written)
    yield assertEquals(read, Line("fig", 2))

  test("a serde of a type Protobuf cannot declare fails, saying why"):
    for
      fake   <- FakeRegistry()
      client <- SchemaRegistry.of(settings, fake.transport("http://registry:8081"))
      serde  <- ProtobufSerde.of[IO, Nested](client, Part.Value).attempt
      asked  <- fake.requests.get
    yield
      serde match
        case Left(error: RegistryException.NoSchema) => assert(error.reason.contains("a list holding lists"), error.reason)
        case other                                   => fail(s"expected no schema, got $other")
      assertEquals(asked, Vector.empty, "nothing is registered")
