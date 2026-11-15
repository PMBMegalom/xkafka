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

import scala.jdk.CollectionConverters.*

import cats.effect.IO
import com.google.protobuf.DynamicMessage
import com.google.protobuf.util.JsonFormat
import fs2.Chunk
import io.circe.Json
import io.circe.jawn.parse
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.schemaregistry.protobuf.{ProtobufSchema, ProtobufSchemaProvider}
import io.confluent.kafka.serializers.protobuf.{KafkaProtobufDeserializer, KafkaProtobufSerializer}
import munit.CatsEffectSuite
import xkafka.{Headers, Topic}
import xkafka.schemaregistry.{Part, SchemaName}

@SchemaName("Line")
final case class Grown(
    @ProtobufField(1)
    sku: String,
    @ProtobufField(2)
    quantity: Int = 1,
    @ProtobufField(3)
    colour: Option[String] = None
) derives ProtobufCodec

@SchemaName("Line")
final case class Retyped(
    @ProtobufField(1)
    sku: Int,
    @ProtobufField(2)
    quantity: Int = 1
) derives ProtobufCodec

/** xkafka's Protobuf schemas, records and serdes against Confluent's Protobuf support and protobuf-java. */
/** A message in a package with a `google` part, where a name starting `google.` would be looked for in that part. */
@xkafka.schemaregistry.SchemaNamespace("com.acme.google.ads")
final case class Placed(
    @ProtobufField(1)
    at: Instant
) derives ProtobufCodec

/** A message that holds itself, a sealed trait whose case holds it, and a map of it, for how deep protobuf-java reads each. */
final case class Deep(
    @ProtobufField(1)
    next: Option[Deep]
) derives ProtobufCodec

sealed trait Chain derives ProtobufCodec
@ProtobufField(1)
final case class Link(
    @ProtobufField(1)
    next: Chain
) extends Chain
@ProtobufField(2)
final case class End() extends Chain

final case class Kids(
    @ProtobufField(1)
    kids: Map[String, Kids]
) derives ProtobufCodec

class ConfluentProtobufSuite extends CatsEffectSuite:
  private val topic = Topic.from("orders").toOption.get

  private def confluent[A](codec: ProtobufCodec[A]): ProtobufSchema = new ProtobufSchema(codec.schema.fold(fail(_), identity))

  private def json(text: String): Json = parse(text).fold(error => fail(error.message), identity)

  /** The message protobuf-java reads from what xkafka writes of `value`, with the descriptor Confluent makes of xkafka's schema. */
  private def java[A](codec: ProtobufCodec[A], value: A): (Chunk[Byte], DynamicMessage) =
    val written = codec.encode(value).fold(fail(_), identity)
    (written, DynamicMessage.parseFrom(confluent(codec).toDescriptor, written.toArray))

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

  test("protobuf-java reads what xkafka writes as xkafka means it, and writes the same bytes again"):
    val (written, message) = java(ProtobufCodec[Order], order)
    assertEquals(Chunk.array(message.toByteArray), written)
    assertEquals(
      json(JsonFormat.printer().print(message)),
      json("""{"id":"3f2b8c1e-9a4d-4e6f-8b2a-1c3d5e7f9a0b","placedAt":"2026-11-10T07:00:00.123Z","day":"2026-11-10",
          |"lines":[{"sku":"apple","quantity":3},{"sku":"pear","quantity":1}],"status":"STATUS_PAID",
          |"payment":{"transfer":{"iban":"DE00","cleared":"2026-11-11T00:00:00Z"}},"note":"ring twice","tags":{"priority":"2"},"retries":1,
          |"raw":"AQID","counts":[1,2,300],"total":"12.50"}""".stripMargin)
    )
    val (zeros, plain) = java(ProtobufCodec[Plain], Plain(0, ""))
    assertEquals((zeros, plain.getAllFields.isEmpty), (Chunk.empty, true))
    val (cash, paid) = java(ProtobufCodec[Payment], Cash)
    assertEquals((Chunk.array(paid.toByteArray), json(JsonFormat.printer().print(paid))), (cash, json("""{"cash":{}}""")))
    val (node, nested) = java(ProtobufCodec[Node], Node(1, Some(Node(2))))
    assertEquals(Chunk.array(nested.toByteArray), node)

  test("records are read and written as deep as protobuf-java reads them, a oneof's message and a map's entries counted as it counts them"):
    def message(number: Int, inner: Chunk[Byte]) = ProtobufWire.field(number, Raw.Bytes(inner))
    def deep(levels: Int)                        = (0 until levels).foldLeft(Chunk.empty[Byte])((inner, _) => message(1, inner))
    def chain(links: Int)                        = (0 until links).foldLeft(message(2, Chunk.empty))((inner, _) => message(1, message(1, inner)))
    def kids(levels: Int)                        = (0 until levels)
      .foldLeft(Chunk.empty[Byte])((inner, _) => message(1, message(1, Chunk.array("k".getBytes)) ++ message(2, inner)))
    def agrees[A](codec: ProtobufCodec[A], full: String, bytes: Chunk[Byte], label: String): Unit =
      val schema    = codec.schema.fold(fail(_), identity)
      val javaReads = scala.util.Try(DynamicMessage.parseFrom(new ProtobufSchema(schema).toDescriptor, bytes.toArray)).isSuccess
      val dynamic   = new Dynamic(ProtobufSchemaText.parse(schema).flatMap(Pool.of(_, Map.empty)).fold(fail(_), identity))
      val read      = codec.decode(bytes)
      assertEquals(read.isRight, javaReads, s"$label: read")
      assertEquals(dynamic.decode(full, bytes).isRight, javaReads, s"$label: read as a value")
      read.foreach(value => assertEquals(codec.encode(value), Right(bytes), s"$label: written"))
    List(99, 100, 101).foreach(levels => agrees(ProtobufCodec[Deep], "xkafka.protobuf.Deep", deep(levels), s"$levels nested"))
    List(48, 49, 50).foreach(links => agrees(ProtobufCodec[Chain], "xkafka.protobuf.Chain", chain(links), s"$links links"))
    List(49, 50, 51).foreach(levels => agrees(ProtobufCodec[Kids], "xkafka.protobuf.Kids", kids(levels), s"$levels levels of entries"))
    def links(count: Int): Chain = (0 until count).foldLeft[Chain](End())((inner, _) => Link(inner))
    assert(ProtobufCodec[Chain].encode(links(50)).isLeft, "a chain protobuf-java refuses to read is refused when written")

  test("xkafka reads what protobuf-java writes"):
    val descriptor = confluent(ProtobufCodec[Order]).toDescriptor
    val builder    = DynamicMessage.newBuilder(descriptor)
    JsonFormat.parser().merge(
      """{"id":"3f2b8c1e-9a4d-4e6f-8b2a-1c3d5e7f9a0b","placedAt":"2026-11-10T07:00:00.123Z","day":"2026-11-10","lines":[{"sku":"fig"}],
        |"status":"STATUS_OPEN","payment":{"card":{"last4":"1234"}},"counts":[7]}""".stripMargin,
      builder
    )
    assertEquals(
      ProtobufCodec[Order].decode(Chunk.array(builder.build.toByteArray)),
      Right(Order(
        UUID.fromString("3f2b8c1e-9a4d-4e6f-8b2a-1c3d5e7f9a0b"),
        Instant.parse("2026-11-10T07:00:00.123Z"),
        LocalDate.of(2026, 11, 10),
        List(Line("fig", 1)),
        Status.Open,
        Card("1234"),
        counts = List(7)
      ))
    )

  test("a well-known type is named from the root, so a package with a google part does not hide it from protobuf-java"):
    val descriptor = confluent(ProtobufCodec[Placed]).toDescriptor
    assertEquals(descriptor.findFieldByName("at").getMessageType.getFullName, "google.protobuf.Timestamp")

  test("adding a field is a compatible change under Confluent's rules, and changing a field's type is not"):
    val older = confluent(ProtobufCodec[Line])
    assertEquals(confluent(ProtobufCodec[Grown]).isBackwardCompatible(older).asScala.toList, Nil)
    assert(confluent(ProtobufCodec[Retyped]).isBackwardCompatible(older).asScala.nonEmpty, "changing a field's type")

  private val shop =
    """syntax = "proto3";
      |package com.example.shop;
      |import "google/protobuf/timestamp.proto";
      |message Order {
      |  string id = 1;
      |  google.protobuf.Timestamp placed = 2;
      |  repeated Line lines = 3;
      |  map<string, int64> tags = 4;
      |  optional string note = 5;
      |  repeated int32 counts = 6 [packed = false];
      |  oneof payment {
      |    Card card = 7;
      |    string voucher = 8;
      |  }
      |  Status status = 13;
      |  repeated Status history = 14;
      |  uint32 weight = 15;
      |  sint64 balance = 16;
      |  fixed64 code = 17;
      |  double ratio = 18;
      |  bytes raw = 19;
      |  message Line { string sku = 1; int32 quantity = 2; }
      |  message Card { string last4 = 1; }
      |}
      |enum Status { STATUS_OPEN = 0; STATUS_PAID = 1; STATUS_OLD = -1; }
      |""".stripMargin

  private val legacy =
    """syntax = "proto2";
      |package legacy;
      |message Item {
      |  required string id = 1;
      |  optional int32 count = 2 [default = -5];
      |  repeated int32 sizes = 4 [packed = true];
      |  repeated int32 marks = 5;
      |  optional sfixed32 offset = 6;
      |}
      |""".stripMargin

  /** The bytes protobuf-java writes of `json`, a message `full` of `text`, and the values xkafka reads from them with the file `text`. */
  private def both(text: String, full: String, json: String): (Chunk[Byte], ProtobufValue.Message, Dynamic) =
    val schema     = new ProtobufSchema(text)
    val descriptor = schema.toDescriptor(full)
    val builder    = DynamicMessage.newBuilder(descriptor)
    JsonFormat.parser().merge(json, builder)
    val written = Chunk.array(builder.build.toByteArray)
    val file    = ProtobufSchemaText.parse(text).fold(fail(_), identity)
    val dynamic = new Dynamic(Pool.of(file, Map.empty).fold(fail(_), identity))
    (written, dynamic.decode(full, written).fold(fail(_), identity), dynamic)

  test("xkafka reads protobuf-java's messages of a proto3 file as the values they hold, and writes them as the same bytes"):
    val (written, value, dynamic) =
      both(
        shop,
        "com.example.shop.Order",
        """{"id":"b","placed":"2026-11-10T07:00:00.5Z","lines":[{"sku":"fig","quantity":-2}],"tags":{"a":"-9223372036854775808"},"note":"",
          |"counts":[1,2],"voucher":"V1","status":"STATUS_PAID","history":["STATUS_PAID","STATUS_OLD"],"weight":4294967295,"balance":"-3",
          |"code":"18446744073709551615","ratio":0.25,"raw":"AQI="}""".stripMargin
      )
    assertEquals(
      value,
      ProtobufValue.Message(
        "com.example.shop.Order",
        Vector(
          "id"     -> ProtobufValue.String("b"),
          "placed" ->
            ProtobufValue
              .Message("google.protobuf.Timestamp", Vector("seconds" -> ProtobufValue.Int64(1794294000L), "nanos" -> ProtobufValue.Int32(500000000))),
          "lines" -> ProtobufValue.List(Vector(
            ProtobufValue.Message("com.example.shop.Order.Line", Vector("sku" -> ProtobufValue.String("fig"), "quantity" -> ProtobufValue.Int32(-2)))
          )),
          "tags"    -> ProtobufValue.Map(Vector(ProtobufValue.String("a") -> ProtobufValue.Int64(Long.MinValue))),
          "note"    -> ProtobufValue.String(""),
          "counts"  -> ProtobufValue.List(Vector(ProtobufValue.Int32(1), ProtobufValue.Int32(2))),
          "voucher" -> ProtobufValue.String("V1"),
          "status"  -> ProtobufValue.Enum("STATUS_PAID"),
          "history" -> ProtobufValue.List(Vector(ProtobufValue.Enum("STATUS_PAID"), ProtobufValue.Enum("STATUS_OLD"))),
          "weight"  -> ProtobufValue.UInt32(4294967295L),
          "balance" -> ProtobufValue.Int64(-3),
          "code"    -> ProtobufValue.UInt64(BigInt("18446744073709551615")),
          "ratio"   -> ProtobufValue.Double(0.25),
          "raw"     -> ProtobufValue.Bytes(Chunk[Byte](1, 2))
        )
      )
    )
    assertEquals(dynamic.encode("com.example.shop.Order", value), Right(written))

  test("xkafka reads protobuf-java's messages of a proto2 file, leaving out optional fields that are not set, and writes them as the same bytes"):
    val (written, value, dynamic) = both(legacy, "legacy.Item", """{"id":"x","sizes":[1,2],"marks":[3,4],"offset":-7}""")
    assertEquals(
      value,
      ProtobufValue.Message(
        "legacy.Item",
        Vector(
          "id"     -> ProtobufValue.String("x"),
          "sizes"  -> ProtobufValue.List(Vector(ProtobufValue.Int32(1), ProtobufValue.Int32(2))),
          "marks"  -> ProtobufValue.List(Vector(ProtobufValue.Int32(3), ProtobufValue.Int32(4))),
          "offset" -> ProtobufValue.Int32(-7)
        )
      )
    )
    assertEquals(dynamic.encode("legacy.Item", value), Right(written))

  test("Confluent takes the files xkafka writes out as the files they were read from"):
    List(shop, legacy).foreach: text =>
      val rendered = ProtobufSchemaText.render(ProtobufSchemaText.parse(text).fold(fail(_), identity))
      assertEquals(new ProtobufSchema(rendered).canonicalString, new ProtobufSchema(text).canonicalString)

  test("Confluent's deserializer reads what xkafka writes, and xkafka reads what Confluent's serializer writes"):
    val mock         = new MockSchemaRegistryClient(List(new ProtobufSchemaProvider()).asJava)
    val serializer   = new KafkaProtobufSerializer[DynamicMessage](mock)
    val deserializer = new KafkaProtobufDeserializer[DynamicMessage](mock)
    serializer.configure(Map("schema.registry.url" -> "mock://xkafka", "auto.register.schemas" -> "true").asJava, false)
    deserializer.configure(Map("schema.registry.url" -> "mock://xkafka").asJava, false)
    val theirs = order.copy(payment = Card("9"), note = None)
    for
      serde   <- ProtobufSerde.of[IO, Order](ConfluentProtobufRegistry.bridged(mock), Part.Value)
      ours    <- serde.serializer.serialize(topic, Headers.empty, order)
      read    <- IO.blocking(deserializer.deserialize("orders", ours.get.toArray))
      written <- IO.blocking(serializer.serialize("orders", java(ProtobufCodec[Order], theirs)._2))
      back    <- serde.deserializer.deserialize(topic, Headers.empty, Some(Chunk.array(written)))
    yield
      assertEquals(Chunk.array(read.toByteArray), ProtobufCodec[Order].encode(order).fold(fail(_), identity))
      assertEquals(back, theirs)
      assertEquals(mock.getAllSubjects.asScala.toList, List("orders-value"))
