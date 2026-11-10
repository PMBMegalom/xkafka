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

package xkafka.schemaregistry

import scala.concurrent.duration.*

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.testkit.TestControl
import munit.CatsEffectSuite

class SchemaRegistrySuite extends CatsEffectSuite:
  private val base     = "http://registry:8081"
  private val order    = Schema("""{"type":"record","name":"Order","fields":[]}""", SchemaType.Avro)
  private val newer    = Schema("""{"type":"record","name":"Order","fields":[{"name":"id","type":"string","default":""}]}""", SchemaType.Avro)
  private val orders   = subject("orders-value")
  private val settings = RegistrySettings.from(NonEmptyList.one(base)).toOption.get

  private def subject(name: String): Subject = Subject.from(name).toOption.get

  private def registry(fake: FakeRegistry, chosen: RegistrySettings = settings): IO[SchemaRegistry[IO]] =
    SchemaRegistry.of(chosen, fake.transport(base))

  test("settings take http and https URLs, without a trailing slash, and refuse others and a negative cache time"):
    assertEquals(RegistrySettings.from(NonEmptyList.of("https://a:8081/", "http://b")).map(_.urls.toList), Right(List("https://a:8081", "http://b")))
    assertEquals(
      RegistrySettings.from(NonEmptyList.one("registry:8081")).left.map(_.message),
      Left(SchemaRegistryError.InvalidUrl("registry:8081").message)
    )
    assertEquals(RegistrySettings.from(NonEmptyList.one("http://")), Left(SchemaRegistryError.InvalidUrl("http://")))
    assertEquals(settings.withLatestCachedFor((-1).second), Left(SchemaRegistryError.NegativeLatestCacheTime((-1).second)))
    assertEquals(settings.withLatestCachedFor(1.minute).map(_.withoutLatestExpiry), Right(settings))
    assertEquals(Subject.from(""), Left(SchemaRegistryError.EmptySubject))

  test("registering gives the schema's id, and asks the registry once for a schema it already registered"):
    for
      fake      <- FakeRegistry()
      client    <- registry(fake)
      first     <- client.register(orders, order)
      again     <- client.register(orders, order)
      asked     <- fake.requests.get
      read      <- client.schema(first)
      afterRead <- fake.requests.get
    yield
      assertEquals(first, again)
      assertEquals(asked.size, 1)
      assertEquals(read, order)
      assertEquals(afterRead.size, 1, "the schema registered is kept under its id")

  test("a schema by id keeps its type and references, and is asked for once"):
    val referenced =
      Schema(
        """syntax = "proto3"; import "money.proto"; message Order { Money total = 1; }""",
        SchemaType.Protobuf,
        List(SchemaReference("money.proto", subject("money"), 3))
      )
    for
      fake   <- FakeRegistry()
      _      <- fake.registered("money", """syntax = "proto3"; message Money { int64 cents = 1; }""", "PROTOBUF")
      writer <- registry(fake)
      id     <- writer.register(orders, referenced)
      reader <- registry(fake)
      first  <- reader.schema(id)
      second <- reader.schema(id)
      asked  <- fake.requests.get
    yield
      assertEquals(first, referenced)
      assertEquals(second, referenced)
      assertEquals(asked.count(_.url.contains("/schemas/ids/")), 1)

  test("looking up finds a registered schema with its version, and nothing for an unknown subject or schema"):
    for
      fake    <- FakeRegistry()
      client  <- registry(fake)
      unknown <- client.lookup(orders, order)
      _       <- fake.registered("orders-value", order.text)
      id      <- fake.registered("orders-value", newer.text)
      found   <- client.lookup(orders, newer)
      other   <- client.lookup(orders, Schema("\"string\"", SchemaType.Avro))
    yield
      assertEquals(unknown, None)
      assertEquals(found, Some(Registered(SchemaId(id), orders, 2, newer)))
      assertEquals(other, None)

  test("a version is found by number, and nothing for one the subject does not have"):
    for
      fake    <- FakeRegistry()
      client  <- registry(fake)
      id      <- fake.registered("orders-value", order.text)
      first   <- client.version(orders, 1)
      missing <- client.version(orders, 2)
      none    <- client.version(subject("payments-value"), 1)
    yield
      assertEquals(first, Some(Registered(SchemaId(id), orders, 1, order)))
      assertEquals(missing, None)
      assertEquals(none, None)

  test("the latest version is kept until the program ends by default"):
    for
      fake   <- FakeRegistry()
      client <- registry(fake)
      none   <- client.latest(orders)
      _      <- fake.registered("orders-value", order.text)
      first  <- client.latest(orders)
      _      <- fake.registered("orders-value", newer.text)
      kept   <- client.latest(orders)
    yield
      assertEquals(none, None, "a subject without versions is asked again")
      assertEquals(first.map(_.schema), Some(order))
      assertEquals(kept.map(_.schema), Some(order))

  test("the latest version is asked again once the time it is kept has passed"):
    val program =
      for
        fake    <- FakeRegistry()
        client  <- registry(fake, settings.withLatestCachedFor(1.minute).toOption.get)
        _       <- fake.registered("orders-value", order.text)
        first   <- client.latest(orders)
        _       <- fake.registered("orders-value", newer.text)
        _       <- IO.sleep(59.seconds)
        kept    <- client.latest(orders)
        _       <- IO.sleep(1.second)
        renewed <- client.latest(orders)
      yield (first.map(_.version), kept.map(_.version), renewed.map(_.version))
    TestControl.executeEmbed(program).assertEquals((Some(1), Some(1), Some(2)))

  test("compatibility gives the registry's reasons, and any schema suits a subject without versions"):
    for
      fake       <- FakeRegistry(incompatible = _.contains("\"id\""))
      client     <- registry(fake)
      empty      <- client.compatibility(orders, newer)
      _          <- fake.registered("orders-value", order.text)
      compatible <- client.compatibility(orders, order)
      refused    <- client.compatibility(orders, newer)
    yield
      assertEquals(empty, Compatibility.Compatible)
      assertEquals(compatible, Compatibility.Compatible)
      assertEquals(refused, Compatibility.Incompatible(List("field removed")))

  test("a refusal carries the registry's status, code and reason"):
    for
      fake    <- FakeRegistry(incompatible = _ => true)
      client  <- registry(fake)
      refused <- client.register(orders, order).attempt
    yield refused match
      case Left(error: RegistryException.Refused) =>
        assertEquals((error.status, error.code, error.reason), (409, 409, "Schema being registered is incompatible with an earlier schema"))
      case other => fail(s"expected a refusal, got $other")

  test("a 404 that is not the registry's answer for something it lacks fails, and is not taken for nothing found"):
    val elsewhere: Transport[IO] =
      _ => IO.pure(RegistryResponse(404, fs2.Chunk.array("""{"error_code":404,"message":"HTTP 404 Not Found"}""".getBytes)))
    SchemaRegistry.of(settings, elsewhere).flatMap(_.latest(orders)).attempt.map:
      case Left(error: RegistryException.Refused) => assertEquals((error.status, error.code), (404, 404))
      case other                                  => fail(s"expected a refusal, got $other")

  test("an answer that is not the registry's is unreadable"):
    val proxy: Transport[IO] = _ => IO.pure(RegistryResponse(502, fs2.Chunk.array("<html>Bad Gateway</html>".getBytes)))
    SchemaRegistry.of(settings, proxy).flatMap(_.schema(SchemaId(1))).attempt.map:
      case Left(error: RegistryException.Unreadable) => assertEquals((error.status, error.reason), (502, "<html>Bad Gateway</html>"))
      case other                                     => fail(s"expected an unreadable answer, got $other")

  test("requests go to the next URL where one cannot be reached, and fail as unreachable where none can"):
    val urls = NonEmptyList.of("http://down:8081", base)
    for
      fake    <- FakeRegistry()
      reached <- SchemaRegistry.of(RegistrySettings.from(urls).toOption.get, fake.transport(base)).flatMap(_.register(orders, order))
      tried   <- fake.requests.get
      failed  <-
        SchemaRegistry.of(RegistrySettings.from(NonEmptyList.of("http://down:8081", "http://gone:8081")).toOption.get, fake.transport(base))
          .flatMap(_.register(orders, order)).attempt
    yield
      assertEquals(tried.map(_.url).toList, List("http://down:8081/subjects/orders-value/versions", s"$base/subjects/orders-value/versions"))
      assertEquals(reached, SchemaId(1))
      failed match
        case Left(error: RegistryException.Unreachable) =>
          assertEquals(error.urls.toList, List("http://down:8081", "http://gone:8081"))
          assert(error.getMessage.contains("connection refused: http://gone:8081"), error.getMessage)
        case other => fail(s"expected unreachable, got $other")

  test("a subject is escaped in the URL, and requests name the registry's media type"):
    val odd = subject("payments/eu value-é")
    for
      fake   <- FakeRegistry()
      client <- registry(fake)
      id     <- client.register(odd, order)
      found  <- client.lookup(odd, order)
      asked  <- fake.requests.get
    yield
      assertEquals(found.map(_.subject), Some(odd))
      assertEquals(found.map(_.id), Some(id))
      assertEquals(asked.head.url, s"$base/subjects/payments%2Feu%20value-%C3%A9/versions")
      assert(asked.forall(_.headers.contains("Content-Type" -> "application/vnd.schemaregistry.v1+json")), asked.toString)
