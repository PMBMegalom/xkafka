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
import com.google.protobuf.{DynamicMessage, Message}
import com.google.protobuf.util.JsonFormat
import example.proto.*
import fs2.Chunk
import io.circe.jawn.parse
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.schemaregistry.protobuf.{ProtobufSchema as ConfluentSchema, ProtobufSchemaProvider}
import io.confluent.kafka.serializers.protobuf.{KafkaProtobufDeserializer, KafkaProtobufSerializer}
import munit.CatsEffectSuite
import xkafka.{Headers, Topic}
import xkafka.schemaregistry.Part

/** The .proto files of smithy4s's messages, and the messages smithy4s writes, against Confluent's Protobuf support and protobuf-java. */
class ConfluentSmithy4sProtobufSuite extends CatsEffectSuite:
  import Till.*

  private val topic = Topic.from("payments").toOption.get
  private val codec = smithy4s.protobuf.Protobuf.codecs.fromSchema(Payment.schema)

  private def confluent: ConfluentSchema =
    new ConfluentSchema(ProtobufSchemaText.render(Smithy4sProtobufSchema.of(Payment.schema).fold(fail(_), identity)))

  /** The message smithy4s writes of `payment`, as protobuf-java reads it with the file. */
  private def java(payment: Payment): DynamicMessage = DynamicMessage.parseFrom(confluent.toDescriptor, codec.writeBlob(payment).toArray)

  /** Whether `message` or any message inside it holds fields its descriptor does not declare. */
  private def unknown(message: Message): Boolean =
    !message.getUnknownFields.asMap.isEmpty || message.getAllFields.asScala.values.exists:
      case inner: Message                   => unknown(inner)
      case values: _root_.java.util.List[?] => values.asScala.exists { case inner: Message => unknown(inner); case _ => false }
      case _                                => false

  test("protobuf-java reads every field smithy4s writes with the file Confluent parses, as the values smithy4s wrote"):
    val message = java(most)
    assert(!unknown(message), message.toString)
    val json = parse(JsonFormat.printer.print(message)).fold(error => fail(error.message), identity)
    assertEquals(json.hcursor.get[String]("delta"), Right("-5"))
    assertEquals(json.hcursor.get[Long]("hash"), Right(4294967295L))
    assertEquals(json.hcursor.get[String]("at"), Right("2023-11-14T22:13:20.000000005Z"))
    assertEquals(json.hcursor.downField("extra").get[Boolean]("a"), Right(true))
    assertEquals(json.hcursor.get[String]("tip"), Right("0"))
    assertEquals(json.hcursor.downField("method").downField("card").get[String]("last4"), Right("4242"))
    assertEquals(json.hcursor.downField("tender").get[Int]("code"), Right(9))
    assertEquals(json.hcursor.get[String]("status"), Right("SETTLED"))
    assertEquals(json.hcursor.downField("totals").get[String]("z"), Right("3"))

  test("Confluent's deserializer reads what the serde writes, and the serde reads what Confluent's serializer writes"):
    val mock         = new MockSchemaRegistryClient(List(new ProtobufSchemaProvider()).asJava)
    val serializer   = new KafkaProtobufSerializer[DynamicMessage](mock)
    val deserializer = new KafkaProtobufDeserializer[DynamicMessage](mock)
    serializer.configure(Map("schema.registry.url" -> "mock://xkafka", "auto.register.schemas" -> "true").asJava, false)
    deserializer.configure(Map("schema.registry.url" -> "mock://xkafka").asJava, false)
    for
      serde   <- Smithy4sProtobufSerde.of[IO, Payment](ConfluentProtobufRegistry.bridged(mock), Part.Value)
      ours    <- serde.serializer.serialize(topic, Headers.empty, most)
      read    <- IO.blocking(deserializer.deserialize("payments", ours.get.toArray))
      written <- IO.blocking(serializer.serialize("payments", java(least)))
      back    <- serde.deserializer.deserialize(topic, Headers.empty, Some(Chunk.array(written)))
    yield
      assertEquals(codec.readBlob(smithy4s.Blob(read.toByteArray)), Right(most))
      assertEquals(back, least)
      assertEquals(mock.getAllSubjects.asScala.toList, List("payments-value"))
