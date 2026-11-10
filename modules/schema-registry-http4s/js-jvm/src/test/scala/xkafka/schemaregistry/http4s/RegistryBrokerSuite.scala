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

package xkafka.schemaregistry.http4s

import java.nio.charset.StandardCharsets

import cats.data.NonEmptyList
import cats.effect.{IO, Resource, SyncIO}
import cats.effect.std.{Env, UUIDGen}
import cats.syntax.all.*
import fs2.Chunk
import munit.{CatsEffectSuite, TestOptions}
import org.http4s.ember.client.EmberClientBuilder
import xkafka.{Headers, Topic}
import xkafka.schemaregistry.*

/** The registry client against a Confluent schema registry, which scripts/with-kafka.sh starts and names in `XKAFKA_SCHEMA_REGISTRY_URL`. */
class RegistryBrokerSuite extends CatsEffectSuite:
  private val url = Env.make[SyncIO].get("XKAFKA_SCHEMA_REGISTRY_URL").unsafeRunSync()

  private val order = Schema("""{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}""", SchemaType.Avro)
  private val added =
    Schema(
      """{"type":"record","name":"Order","fields":[{"name":"id","type":"string"},{"name":"note","type":"string","default":""}]}""",
      SchemaType.Avro
    )
  private val breaking =
    Schema("""{"type":"record","name":"Order","fields":[{"name":"id","type":"string"},{"name":"total","type":"long"}]}""", SchemaType.Avro)

  /** A test against the registry, given a way to make clients of it and a subject name no other test uses; ignored without a registry. */
  private def registryTest(name: String)(body: (IO[SchemaRegistry[IO]], String) => IO[Unit]): Unit =
    val options = TestOptions(name)
    test(if url.isEmpty then options.ignore else options):
      url.fold(IO.unit): at =>
        val settings = RegistrySettings.from(NonEmptyList.one(at)).toOption.get
        (EmberClientBuilder.default[IO].build, Resource.eval(UUIDGen[IO].randomUUID)).tupled.use: (client, id) =>
          body(SchemaRegistry.of(settings, Http4sTransport(client)), s"xkafka-$id")

  private def subject(name: String): Subject = Subject.from(name).toOption.get

  registryTest("registering gives an id, the same id for the same schema, and the schema back by id"): (registry, name) =>
    for
      first <- registry.flatMap(_.register(subject(name), order))
      again <- registry.flatMap(_.register(subject(name), order))
      read  <- registry.flatMap(_.schema(first))
    yield
      assertEquals(again, first)
      assertEquals(read, order)

  registryTest("looking up finds a registered schema with its version, and nothing for an unknown schema or subject"): (registry, name) =>
    for
      client  <- registry
      id      <- client.register(subject(name), order)
      found   <- client.lookup(subject(name), order)
      other   <- client.lookup(subject(name), added)
      unknown <- client.lookup(subject(s"$name-unknown"), order)
    yield
      assertEquals(found, Some(Registered(id, subject(name), 1, order)))
      assertEquals(other, None)
      assertEquals(unknown, None)

  registryTest("the latest version and versions by number, and nothing for a version or subject the registry lacks"): (registry, name) =>
    for
      client  <- registry
      _       <- client.register(subject(name), order)
      id      <- registry.flatMap(_.register(subject(name), added))
      latest  <- client.latest(subject(name))
      first   <- client.version(subject(name), 1)
      missing <- client.version(subject(name), 3)
      unknown <- client.latest(subject(s"$name-unknown"))
    yield
      assertEquals(latest, Some(Registered(id, subject(name), 2, added)))
      assertEquals(first.map(_.schema), Some(order))
      assertEquals(missing, None)
      assertEquals(unknown, None)

  registryTest("compatibility follows the subject's rule, any schema suits a subject without versions, and registering a breaking schema is refused"):
    (registry, name) =>
      for
        client     <- registry
        empty      <- client.compatibility(subject(name), breaking)
        _          <- client.register(subject(name), order)
        compatible <- client.compatibility(subject(name), added)
        refused    <- client.compatibility(subject(name), breaking)
        registered <- client.register(subject(name), breaking).attempt
      yield
        assertEquals(empty, Compatibility.Compatible)
        assertEquals(compatible, Compatibility.Compatible)
        refused match
          case Compatibility.Incompatible(reasons) => assert(reasons.nonEmpty, "the registry gives its reasons")
          case other                               => fail(s"expected incompatible, got $other")
        registered match
          case Left(error: RegistryException.Refused) => assertEquals((error.status, error.code), (409, 409))
          case other                                  => fail(s"expected a refusal, got $other")

  registryTest("a Protobuf schema keeps its type and the schemas it imports"): (registry, name) =>
    val money = Schema("syntax = \"proto3\";\n\nmessage Money {\n  int64 cents = 1;\n}\n", SchemaType.Protobuf)
    for
      client <- registry
      _      <- client.register(subject(s"$name-money"), money)
      uses =
        Schema(
          "syntax = \"proto3\";\n\nimport \"money.proto\";\n\nmessage Order {\n  Money total = 1;\n}\n",
          SchemaType.Protobuf,
          List(SchemaReference("money.proto", subject(s"$name-money"), 1))
        )
      id   <- client.register(subject(name), uses)
      read <- registry.flatMap(_.schema(id))
    yield
      assertEquals(read.schemaType, SchemaType.Protobuf)
      assertEquals(read.references, uses.references)

  registryTest("a subject with spaces and letters outside ASCII is escaped as the registry reads it"): (registry, name) =>
    val odd = subject(s"$name orders é:1")
    for
      client <- registry
      id     <- client.register(odd, order)
      found  <- registry.flatMap(_.lookup(odd, order))
    yield assertEquals(found.map(registered => (registered.id, registered.subject)), Some((id, odd)))

  registryTest("a serde writes in Confluent's wire format with the id the registry gives, and reads it back"): (registry, name) =>
    val topic = Topic.from(name).toOption.get
    val text  =
      new SchemaFormat[String]:
        override val schema: Schema             = Schema("\"string\"", SchemaType.Avro)
        override val recordName: Option[String] = None
        override def writer(target: Schema)     = Right(value => Right(Chunk.array(value.getBytes(StandardCharsets.UTF_8))))
        override def reader(writer: Schema)     = Right(bytes => Right(new String(bytes.toArray, StandardCharsets.UTF_8)))
    for
      client  <- registry
      serde   <- RegistrySerde.of(client, text, Part.Value)
      written <- serde.serializer.serialize(topic, Headers.empty, "hello").map(_.get)
      found   <- client.lookup(subject(s"$name-value"), text.schema)
      back    <- registry.flatMap(RegistrySerde.of(_, text, Part.Value)).flatMap(_.deserializer.deserialize(topic, Headers.empty, Some(written)))
    yield
      assertEquals(found.map(_.version), Some(1))
      assertEquals(written.take(5), Chunk.singleton(0.toByte) ++ Chunk.array(java.nio.ByteBuffer.allocate(4).putInt(found.get.id.value).array))
      assertEquals(back, "hello")
