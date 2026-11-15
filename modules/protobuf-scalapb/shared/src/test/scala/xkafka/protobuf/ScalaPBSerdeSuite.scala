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

import cats.data.NonEmptyList
import cats.effect.IO
import google.`type`.date.Date
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp.Timestamp
import example.dated.dated.Delivery
import example.legacy.legacy.{Item, Kind}
import example.open.open.Open
import example.refused.refused.Basket
import example.shop.shop.{Cash, Order, Status}
import fs2.Chunk
import munit.CatsEffectSuite
import xkafka.{Headers, Topic}
import xkafka.schemaregistry.*

/** Messages ScalaPB generated, written and read with the .proto files they were generated from, on every platform. */
class ScalaPBSerdeSuite extends CatsEffectSuite:
  private val settings = RegistrySettings.from(NonEmptyList.one("http://registry:8081")).toOption.get
  private val topic    = Topic.from("orders").toOption.get

  private def client: IO[SchemaRegistry[IO]] = FakeRegistry().flatMap(fake => SchemaRegistry.of(settings, fake.transport("http://registry:8081")))

  private def schemaOf[M <: scalapb.GeneratedMessage: scalapb.GeneratedMessageCompanion]: String = ScalaPBFormat[M].fold(fail(_), _.schema.text)

  private def noSchema[A](outcome: Either[Throwable, A]): String =
    outcome match
      case Left(error: RegistryException.NoSchema) => error.reason
      case other                                   => fail(s"expected no schema, got $other")

  private val order =
    Order(
      id = "o-1",
      lines = Seq(Order.Line("apple", 3), Order.Line("pear", 1)),
      note = Some(""),
      codes = Seq(7, -1),
      counts = Map("apple" -> 3L),
      status = Status.STATUS_PAID,
      placed = Some(Timestamp(1700000000L, 5)),
      payment = Order.Payment.Cash(Cash(250L)),
      first = Some(Order.Line("apple", 3))
    )

  private val shop =
    """syntax = "proto3";
      |
      |package example.shop;
      |
      |import "google/protobuf/timestamp.proto";
      |
      |message Cash {
      |  int64 cents = 1;
      |}
      |
      |message Order {
      |  string id = 1;
      |  repeated .example.shop.Order.Line lines = 2;
      |  optional string note = 3;
      |  repeated int32 codes = 4;
      |  map<string, int64> counts = 5;
      |  .example.shop.Status status = 6;
      |  .google.protobuf.Timestamp placed = 7;
      |  oneof payment {
      |    string card = 8;
      |    .example.shop.Cash cash = 9;
      |  }
      |  .example.shop.Order.Line first = 10;
      |  message Line {
      |    string sku = 1;
      |    int32 quantity = 2;
      |  }
      |}
      |
      |enum Status {
      |  STATUS_UNKNOWN = 0;
      |  STATUS_PAID = 1;
      |}
      |""".stripMargin

  test("a message's schema is the file it was generated from: its maps, oneofs, optional fields and imports of the well-known types"):
    assertEquals(schemaOf[Order], shop)
    assertEquals(ScalaPBFormat[Order].map(_.recordName), Right(Some("example.shop.Order")))
    assertEquals(schemaOf[Order.Line], shop)
    assertEquals(ScalaPBFormat[Order.Line].map(_.recordName), Right(Some("example.shop.Order.Line")))

  test("a proto2 message's schema keeps its required fields, defaults and packing, and leaves out ScalaPB's options"):
    assertEquals(
      schemaOf[Item],
      """syntax = "proto2";
        |
        |package example.legacy;
        |
        |message Item {
        |  required string sku = 1;
        |  optional string label = 2 [default = "say \"hi\""];
        |  optional bytes tag = 3 [default = "\001x"];
        |  optional .example.legacy.Kind kind = 4 [default = KIND_FANCY];
        |  optional int32 count = 5 [default = 7];
        |  repeated int32 sizes = 6 [packed = true];
        |}
        |
        |enum Kind {
        |  KIND_PLAIN = 0;
        |  KIND_FANCY = 1;
        |}
        |""".stripMargin
    )

  test("a message whose file imports a file Confluent's serializers know beside the well-known types is registered and read back"):
    val delivery = Delivery(on = Some(Date(2026, 11, 14)))
    for
      registry <- client
      serde    <- ScalaPBSerde.of[IO, Delivery](registry, Part.Value)
      written  <- serde.serializer.serialize(topic, Headers.empty, delivery)
      read     <- serde.deserializer.deserialize(topic, Headers.empty, written)
    yield
      assertEquals(read, delivery)
      assert(schemaOf[Delivery].contains("import \"google/type/date.proto\";"), schemaOf[Delivery])

  test("a message is written as ScalaPB writes it, after its path through the file's messages, and read back"):
    for
      registry <- client
      orders   <- ScalaPBSerde.of[IO, Order](registry, Part.Value)
      written  <- orders.serializer.serialize(topic, Headers.empty, order)
      read     <- orders.deserializer.deserialize(topic, Headers.empty, written)
      lines    <- ScalaPBSerde.of[IO, Order.Line](registry, Part.Value, SubjectNaming.RecordName)
      line     <- lines.serializer.serialize(topic, Headers.empty, Order.Line("kiwi", 2))
      items    <- ScalaPBSerde.of[IO, Item](registry, Part.Value, SubjectNaming.RecordName)
      item     <-
        items.serializer.serialize(topic, Headers.empty, Item("s-1", sizes = Seq(1, 2)))
          .flatMap(items.deserializer.deserialize(topic, Headers.empty, _))
      subjects <- registry.latest(Subject.from("orders-value").toOption.get)
    yield
      assertEquals(read, order)
      assertEquals(written.map(_.drop(5)).map(ProtobufWire.readIndexes), Some(Right((List(1), Chunk.array(order.toByteArray)))))
      assertEquals(line.map(_.drop(5)).map(ProtobufWire.readIndexes), Some(Right((List(1, 0), Chunk.array(Order.Line("kiwi", 2).toByteArray)))))
      assertEquals(item, Item("s-1", sizes = Seq(1, 2)))
      assertEquals(
        (item.getLabel, item.getTag, item.getKind, item.getCount),
        ("say \"hi\"", ByteString.copyFrom(Array[Byte](1, 'x')), Kind.KIND_FANCY, 7)
      )
      assertEquals(subjects.map(_.schema.text), Some(shop))

  test("records ScalaPB writes are read as written and by a generic serde, and ScalaPB reads what a generic serde writes"):
    val file = ProtobufSchemaText.parse(shop).fold(fail(_), identity)
    for
      registry <- client
      orders   <- ScalaPBSerde.of[IO, Order](registry, Part.Value)
      written  <- orders.serializer.serialize(topic, Headers.empty, order)
      reader   <- ProtobufSerde.asWritten[IO](registry)
      asRead   <- reader.deserialize(topic, Headers.empty, written)
      generic  <- ProtobufSerde.generic[IO](registry, file, "example.shop.Order", Part.Value)
      value    <- generic.deserializer.deserialize(topic, Headers.empty, written)
      back     <- generic.serializer.serialize(topic, Headers.empty, value).flatMap(orders.deserializer.deserialize(topic, Headers.empty, _))
    yield
      assertEquals(asRead.value.name, "example.shop.Order")
      assertEquals(asRead.value.field("status"), Some(ProtobufValue.Enum("STATUS_PAID")))
      assertEquals(asRead.value.field("note"), Some(ProtobufValue.String("")))
      assertEquals(value, asRead.value)
      assertEquals(back, order)

  test("a message is written with the subject's latest or a pinned version of its file, which may have moved it, and refused where it lacks it"):
    val moved = shop.replace("message Cash {", "message Header {\n  string trace = 1;\n}\n\nmessage Cash {")
    val other = "syntax = \"proto3\";\npackage example.shop;\nmessage Header {}\n"
    for
      registry <- client
      id       <- registry.register(Subject.from("orders-value").toOption.get, Schema(moved, SchemaType.Protobuf))
      orders   <- ScalaPBSerde.of[IO, Order](registry, Part.Value, writing = Writing.Latest)
      written  <- orders.serializer.serialize(topic, Headers.empty, order)
      read     <- orders.deserializer.deserialize(topic, Headers.empty, written)
      pinned   <-
        ScalaPBSerde.of[IO, Order](registry, Part.Value, writing = Writing.Pinned(id)).flatMap(_.serializer.serialize(topic, Headers.empty, order))
      elsewhere <- client
      _         <- elsewhere.register(Subject.from("orders-value").toOption.get, Schema(other, SchemaType.Protobuf))
      refused   <-
        ScalaPBSerde.of[IO, Order](elsewhere, Part.Value, writing = Writing.Latest).flatMap(_.serializer.serialize(topic, Headers.empty, order))
          .attempt
    yield
      assertEquals(written.map(_.drop(5)).map(ProtobufWire.readIndexes(_).map(_._1)), Some(Right(List(2))))
      assertEquals(pinned, written)
      assertEquals(read, order)
      refused match
        case Left(error: RegistryException.Unserializable) => assertEquals(error.reason, "the schema declares no message example.shop.Order")
        case other                                         => fail(s"expected unserializable, got $other")

  test("bytes that are not the message fail to read, saying why"):
    for
      registry <- client
      orders   <- ScalaPBSerde.of[IO, Order](registry, Part.Value)
      written  <- orders.serializer.serialize(topic, Headers.empty, order)
      broken   <- orders.deserializer.deserialize(topic, Headers.empty, written.map(bytes => bytes.take(6) ++ Chunk[Byte](10, 50, 1))).attempt
    yield broken match
      case Left(error: RegistryException.Unserializable) => assert(error.reason.startsWith("example.shop.Order could not be read: "), error.reason)
      case other                                         => fail(s"expected unserializable, got $other")

  test("a message whose file imports another of the application's files, or declares extensions, has no schema, saying why"):
    for
      registry <- client
      imports  <- ScalaPBSerde.of[IO, Basket](registry, Part.Value).attempt
      open     <- ScalaPBSerde.of[IO, Open](registry, Part.Value).attempt
    yield
      assert(noSchema(imports).startsWith("the schema imports shop.proto"), noSchema(imports))
      assert(
        noSchema(imports).endsWith("; a ScalaPB message is registered with a file that imports only the files Confluent's serializers know"),
        noSchema(imports)
      )
      assertEquals(noSchema(open), "example.open.Open has extensions, which xkafka does not read")

  test("a file that declares a group or is written in an edition of Protobuf cannot be read"):
    import com.google.protobuf.descriptor.{DescriptorProto, FieldDescriptorProto, FileDescriptorProto}
    val group =
      FieldDescriptorProto(name = Some("item"), number = Some(1), `type` = Some(FieldDescriptorProto.Type.TYPE_GROUP), typeName = Some(".Item"))
    val grouped = FileDescriptorProto(name = Some("old.proto"), messageType = Seq(DescriptorProto(name = Some("Basket"), field = Seq(group))))
    assertEquals(ScalaPBDescriptors.file(grouped), Left("Basket.item is a group, which xkafka does not read"))
    assertEquals(
      ScalaPBDescriptors.file(FileDescriptorProto(name = Some("new.proto"), syntax = Some("editions"))),
      Left("new.proto is written in editions, which xkafka does not read")
    )
