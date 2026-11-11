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

import scala.jdk.CollectionConverters.*

import cats.effect.IO
import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}
import com.google.protobuf.DynamicMessage
import com.google.protobuf.timestamp.Timestamp
import com.google.protobuf.util.JsonFormat
import example.shop.shop.{Cash, Order, ShopProto, Status}
import fs2.Chunk
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import io.confluent.kafka.schemaregistry.protobuf.{ProtobufSchema, ProtobufSchemaProvider}
import io.confluent.kafka.serializers.protobuf.{KafkaProtobufDeserializer, KafkaProtobufSerializer}
import munit.CatsEffectSuite
import xkafka.{Headers, Topic}
import xkafka.schemaregistry.{Compatibility, Part, Registered, Schema, SchemaId, SchemaRegistry, SchemaType, Subject}

/** ScalaPB's messages, as xkafka writes and reads them, against Confluent's Protobuf serializer and deserializer. */
class ConfluentScalaPBSuite extends CatsEffectSuite:
  private val topic = Topic.from("orders").toOption.get

  private val order =
    Order(
      id = "o-1",
      lines = Seq(Order.Line("apple", 3)),
      note = Some("ring twice"),
      counts = Map("apple" -> 3L),
      status = Status.STATUS_PAID,
      placed = Some(Timestamp(1700000000L, 5)),
      payment = Order.Payment.Cash(Cash(250L))
    )

  /** xkafka's registry operations on Confluent's registry in memory. */
  private def bridged(mock: MockSchemaRegistryClient): SchemaRegistry[IO] =
    def missing[A](run: => A): IO[Option[A]] = IO.blocking(Option(run)).recover { case error: RestClientException if error.getStatus == 404 => None }
    new SchemaRegistry[IO]:
      override def schema(id: SchemaId): IO[Schema] = IO.blocking(Schema(mock.getSchemaById(id.value).canonicalString, SchemaType.Protobuf))
      override def register(subject: Subject, schema: Schema): IO[SchemaId] =
        IO.blocking(SchemaId(mock.register(subject.value, new ProtobufSchema(schema.text))))
      override def lookup(subject: Subject, schema: Schema): IO[Option[Registered]] =
        missing(mock.getId(subject.value, new ProtobufSchema(schema.text))).map(_.map(id => Registered(SchemaId(id), subject, 1, schema)))
      override def latest(subject: Subject): IO[Option[Registered]] =
        missing(mock.getLatestSchemaMetadata(subject.value))
          .map(_.map(meta => Registered(SchemaId(meta.getId), subject, meta.getVersion, Schema(meta.getSchema, SchemaType.Protobuf))))
      override def version(subject: Subject, version: Int): IO[Option[Registered]]    = IO.pure(None)
      override def compatibility(subject: Subject, schema: Schema): IO[Compatibility] = IO.pure(Compatibility.Compatible)

  /** Each message's fields as protobuf-java sees them: name, number, type, and whether it repeats or records presence; a map's by its key and value
    * types, since the name of the entry message a parser makes for it is its own.
    */
  private def fields(message: Descriptor): List[(String, String, Int, String, Boolean, Boolean)] =
    message.getFields.asScala.toList.flatMap: field =>
      val kind =
        if field.isMapField then
          val entry = field.getMessageType
          s"map<${entry.findFieldByNumber(1).getType}, ${entry.findFieldByNumber(2).getType}>"
        else field.getType.toString
      val nested =
        Option.when(field.getJavaType == FieldDescriptor.JavaType.MESSAGE && !field.isMapField)(field.getMessageType)
          .filter(_.getFullName.startsWith("example.")).toList.flatMap(fields)
      (message.getFullName, field.getName, field.getNumber, kind, field.isRepeated, field.hasPresence) :: nested
    .distinct.sorted

  test("the schema xkafka registers for a ScalaPB message describes the message as the file it was generated from does"):
    val ours                         = new ProtobufSchema(ScalaPBFormat[Order].fold(fail(_), _.schema.text)).toDescriptor("example.shop.Order")
    val theirs                       = ShopProto.javaDescriptor.findMessageTypeByName("Order")
    val full                         = order.withCodes(Seq(7, -1)).withFirst(Order.Line("pear", 1))
    def json(descriptor: Descriptor) = JsonFormat.printer().print(DynamicMessage.parseFrom(descriptor, full.toByteArray))
    assertEquals(fields(ours), fields(theirs))
    assertEquals(json(ours), json(theirs))

  test("Confluent's deserializer reads what xkafka writes of a ScalaPB message, and xkafka reads what Confluent's serializer writes"):
    val mock         = new MockSchemaRegistryClient(List(new ProtobufSchemaProvider()).asJava)
    val serializer   = new KafkaProtobufSerializer[DynamicMessage](mock)
    val deserializer = new KafkaProtobufDeserializer[DynamicMessage](mock)
    serializer.configure(Map("schema.registry.url" -> "mock://xkafka", "auto.register.schemas" -> "true").asJava, false)
    deserializer.configure(Map("schema.registry.url" -> "mock://xkafka").asJava, false)
    val theirs  = order.withPayment(Order.Payment.Card("4111")).withNote("")
    val dynamic = DynamicMessage.parseFrom(ShopProto.javaDescriptor.findMessageTypeByName("Order"), theirs.toByteArray)
    for
      serde   <- ScalaPBSerde.of[IO, Order](bridged(mock), Part.Value)
      ours    <- serde.serializer.serialize(topic, Headers.empty, order)
      read    <- IO.blocking(deserializer.deserialize("orders", ours.get.toArray))
      written <- IO.blocking(serializer.serialize("orders", dynamic))
      back    <- serde.deserializer.deserialize(topic, Headers.empty, Some(Chunk.array(written)))
    yield
      assertEquals(Order.parseFrom(read.toByteArray), order)
      assertEquals(back, theirs)
      assertEquals(mock.getAllSubjects.asScala.toList, List("orders-value"))
