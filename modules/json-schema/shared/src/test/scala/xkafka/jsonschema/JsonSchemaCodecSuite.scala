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

package xkafka.jsonschema

import java.time.{Instant, LocalDate}
import java.util.UUID

import cats.data.NonEmptyList
import cats.effect.IO
import fs2.Chunk
import io.circe.Json
import io.circe.jawn.parse
import munit.CatsEffectSuite
import xkafka.{Headers, Topic}
import xkafka.schemaregistry.*

final case class Line(sku: String, quantity: Int = 1) derives JsonSchemaCodec

@SchemaName("Line")
final case class LineBefore(sku: String) derives JsonSchemaCodec

final case class Strict(
    sku: String,
    @SchemaNoDefault
    quantity: Int = 1
) derives JsonSchemaCodec

object Here:
  final case class Spot(x: Int) derives JsonSchemaCodec

/** Derived once for every type argument, so a page of pages names its inner page alike, whatever that holds. */
final case class Page[A](items: List[A]) derives JsonSchemaCodec

final case class Doc(a: Page[Page[Int]], b: Page[Page[String]]) derives JsonSchemaCodec

final case class Twice(note: Option[Option[String]]) derives JsonSchemaCodec

/** A type whose schema is a whole file, written by hand, with a definition of its own that it names by reference. */
final case class Listed(items: List[String])

object Listed:
  given JsonSchemaCodec[Listed] =
    JsonSchemaCodec.from(
      parse(
        """{"$schema":"http://json-schema.org/draft-07/schema#","title":"Listed","type":"object","properties":{"items":{"type":"array","items":{"$ref":"#/definitions/Item"}}},"definitions":{"Item":{"type":"string"}}}"""
      ).toOption.get,
      io.circe.Encoder.forProduct1("items")(_.items),
      io.circe.Decoder.forProduct1("items")(Listed(_))
    )

final case class HoldsListed(listed: Listed) derives JsonSchemaCodec

sealed trait EitherListed derives JsonSchemaCodec
final case class OneListed(listed: Listed) extends EitherListed
final case class NoneListed()              extends EitherListed

/** Two versions of one event, whose cases share a simple name. */
sealed trait Event derives JsonSchemaCodec
object Event:
  object V1:
    final case class Created(id: String) extends Event
  object V2:
    final case class Created(id: String, at: Long) extends Event

enum Status derives JsonSchemaCodec:
  case Open, Paid

sealed trait Payment derives JsonSchemaCodec
final case class Card(last4: String) extends Payment
case object Cash                     extends Payment
final case class Transfer(
    iban: String,
    @SchemaDoc("When it cleared")
    cleared: Option[Instant] = None
) extends Payment

@SchemaNamespace("com.example") @SchemaDoc("An order")
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
    @SchemaAlias("attempts")
    retries: Int = 0,
    raw: Chunk[Byte] = Chunk.empty
) derives JsonSchemaCodec

final case class Node(value: Int, next: Option[Node] = None) derives JsonSchemaCodec

final case class Maybe(note: Option[String]) derives JsonSchemaCodec

final case class Clash(`type`: String) extends Shape
sealed trait Shape derives JsonSchemaCodec

sealed trait Vehicle derives JsonSchemaCodec
final case class Car(seats: Int) extends Vehicle
sealed trait Signal              extends Vehicle
case object Stop                 extends Signal
case object Go                   extends Signal

/** A case met again inside itself, through the trait it is a case of. */
sealed trait Link derives JsonSchemaCodec
final case class Holder(next: Option[Link]) extends Link derives JsonSchemaCodec

class JsonSchemaCodecSuite extends CatsEffectSuite:
  private def json(text: String): Json = parse(text).fold(error => fail(error.message), identity)

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
      Chunk[Byte](1, 2, 3)
    )

  test("a time of day is ISO-8601 text that leaves out seconds that are zero"):
    assertEquals(JsonSchemaCodec[java.time.LocalTime].encode(java.time.LocalTime.of(7, 0)), Right(Json.fromString("07:00")))
    assertEquals(JsonSchemaCodec[java.time.LocalTime].encode(java.time.LocalTime.of(7, 0, 5)), Right(Json.fromString("07:00:05")))

  test("two cases whose names would both tell the case apart are refused, since a value of one would read as the other"):
    assert(JsonSchemaCodec[Event].refusal.exists(_.contains("'Created'")), JsonSchemaCodec[Event].refusal)

  test("a whole schema written for another draft is refused inside another, which is written for draft 7, and taken on its own"):
    val later: JsonSchemaCodec[String] =
      JsonSchemaCodec.from(
        parse("""{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"string"}""").toOption.get,
        io.circe.Encoder.encodeString,
        io.circe.Decoder.decodeString
      )
    assertEquals(
      JsonSchemaCodec[List[String]](using JsonSchemaCodec.given_JsonSchemaCodec_List(using later)).refusal,
      Some("a whole schema for https://json-schema.org/draft/2020-12/schema cannot be held in another, which is for draft 7")
    )
    assertEquals(later.refusal, None)
    List("http://json-schema.org/draft-07/schema#", "https://json-schema.org/draft-07/schema#", "http://json-schema.org/draft-07/schema").foreach:
      named =>
        val seven: JsonSchemaCodec[String] =
          JsonSchemaCodec
            .from(parse(s"""{"$$schema":"$named","type":"string"}""").toOption.get, io.circe.Encoder.encodeString, io.circe.Decoder.decodeString)
        assertEquals(JsonSchemaCodec[List[String]](using JsonSchemaCodec.given_JsonSchemaCodec_List(using seven)).refusal, None, named)

  test("an option of a schema that takes null already is refused, since null would match both branches of its oneOf"):
    def written(schema: String): JsonSchemaCodec[String] =
      JsonSchemaCodec.from(parse(schema).toOption.get, io.circe.Encoder.encodeString, io.circe.Decoder.decodeString)
    def refusal(schema: String) = JsonSchemaCodec[Option[String]](using JsonSchemaCodec.given_JsonSchemaCodec_Option(using written(schema))).refusal
    // Every keyword must take a value, so null is one unless a keyword rules it out.
    List(
      """{"type":["string","null"]}""",
      """{"enum":["a",null]}""",
      """{}""",
      """{"oneOf":[{"type":"null"},{"type":"string"}]}""",
      """{"examples":[1]}""",
      """{"minimum":0}""",
      """{"format":"email"}""",
      """{"const":null}""",
      """{"not":{"type":"string"}}""",
      """{"allOf":[{"type":"null"}]}""",
      """{"not":{"if":{"type":"null"},"then":{"type":"string"}}}"""
    ).foreach(schema => assert(refusal(schema).isDefined, schema))
    List(
      """{"type":"string"}""",
      """{"type":"string","oneOf":[{},{"minLength":1}]}""",
      """{"const":"a"}""",
      """{"not":{}}""",
      """{"allOf":[{"type":"string"},{}]}""",
      """{"anyOf":[{"type":"string"},{"type":"integer"}]}"""
    ).foreach(schema => assertEquals(refusal(schema), None, schema))

  test("an option holding an option is refused, since null would be a value of both and Some(None) would read back as None"):
    assert(JsonSchemaCodec[Twice].refusal.exists(_.contains("an option holding an option")), JsonSchemaCodec[Twice].refusal)
    assertEquals(JsonSchemaCodec[Maybe].refusal, None)

  test("a whole schema naming its own definitions is refused inside another, where its references would name the other's, and taken on its own"):
    val message = "a whole schema, Listed, names its own definitions by reference, which inside another schema would name that schema's"
    List(
      JsonSchemaCodec[HoldsListed].refusal,
      JsonSchemaCodec[Option[Listed]].refusal,
      JsonSchemaCodec[List[Listed]].refusal,
      JsonSchemaCodec[Map[String, Listed]].refusal,
      JsonSchemaCodec[EitherListed].refusal
    ).foreach(refusal => assertEquals(refusal, Some(message)))
    assertEquals(JsonSchemaCodec[Listed].refusal, None)
    for
      fake   <- FakeRegistry()
      client <-
        SchemaRegistry.of(RegistrySettings.from(NonEmptyList.one("http://registry:8081")).toOption.get, fake.transport("http://registry:8081"))
      refused <- JsonSchemaSerde.of[IO, HoldsListed](client, Part.Value).attempt
    yield assert(refused.left.exists(_.isInstanceOf[RegistryException.NoSchema]), refused)

  test("two types given one name are refused, since one definition cannot say what each is"):
    assert(JsonSchemaCodec[Doc].refusal.exists(_.contains("two types are named xkafka.jsonschema.Page__A")), JsonSchemaCodec[Doc].refusal)
    assert(JsonSchemaFormat[Doc].writer(JsonSchemaFormat[Doc].schema).isLeft)
    assertEquals(JsonSchemaCodec[Page[Page[Int]]].refusal, None)

  test("a type in an object has the object in its title's namespace"):
    assertEquals(JsonSchemaCodec[Here.Spot].schema.hcursor.get[String]("title"), Right("xkafka.jsonschema.Here.Spot"))

  test("a field marked SchemaNoDefault is required when read, as its schema says, though its type has a default"):
    assertEquals(JsonSchemaCodec[Strict].decode(Json.obj("sku" -> Json.fromString("apple"))).left.map(_.contains("quantity")), Left(true))
    assertEquals(
      JsonSchemaCodec[Strict].decode(Json.obj("sku" -> Json.fromString("apple"), "quantity" -> Json.fromInt(2))),
      Right(Strict("apple", 2))
    )

  test("a record's long keeps every digit, beyond what a double holds"):
    final case class Big(n: Long) derives JsonSchemaCodec
    val bytes = Chunk.array("""{"n":9007199254740993}""".getBytes("UTF-8"))
    assertEquals(JsonSchemaFormat.read(bytes).flatMap(JsonSchemaCodec[Big].decode), Right(Big(9007199254740993L)))

  test("a record that is not UTF-8 is refused, not read with its bad bytes replaced"):
    assertEquals(JsonSchemaFormat.read(Chunk[Byte]('"', 'a', 0xff.toByte, '"')), Left("the record is not UTF-8"))
    assertEquals(JsonSchemaFormat.read(Chunk.array("\"é\"".getBytes("UTF-8"))), Right(Json.fromString("é")))

  test("records nested deeper than the limit are refused when written too, so nothing is written that reading refuses"):
    def nodes(count: Int)   = (1 until count).foldLeft(Node(0))((inner, value) => Node(value, Some(inner)))
    val refused             = s"the record is nested more than ${JsonSchemaFormat.MaxDepth} deep"
    val format              = JsonSchemaFormat[Node]
    def written(count: Int) = format.writer(format.schema).flatMap(_(nodes(count)))
    assert(written(JsonSchemaFormat.MaxDepth + 1).left.exists(_.endsWith(refused)), written(JsonSchemaFormat.MaxDepth + 1).map(_.size))
    assert(written(100000).left.exists(_.endsWith(refused)))
    assert(written(JsonSchemaFormat.MaxDepth).isRight)

  test("records nested deeper than the limit are refused, not read until the stack runs out"):
    val nested = Chunk.array(("[" * 1000 + "]" * 1000).getBytes("UTF-8"))
    assertEquals(JsonSchemaFormat.read(nested), Left(s"the record is nested more than ${JsonSchemaFormat.MaxDepth} deep"))
    val format  = JsonSchemaFormat[Node]
    val deepest = (1 until JsonSchemaFormat.MaxDepth).foldLeft(Node(0))((inner, value) => Node(value, Some(inner)))
    val read    =
      for
        write <- format.writer(format.schema)
        read  <- format.reader(format.schema)
        bytes <- write(deepest)
        value <- read(bytes)
      yield value
    assertEquals(read, Right(deepest))

  test("a case class is a closed object of its fields, those without a default required"):
    assertEquals(
      JsonSchemaCodec[Line].schema,
      json(
        """{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","title":"xkafka.jsonschema.Line",
             |"properties":{"sku":{"type":"string"},"quantity":{"type":"integer","default":1}},"required":["sku"],"additionalProperties":false}"""
          .stripMargin
      )
    )

  test("a field marked SchemaDecimal is a number as any BigDecimal is, with its default"):
    final case class Priced(
        @SchemaDecimal(10, 2)
        price: BigDecimal = BigDecimal("1.50")
    ) derives JsonSchemaCodec
    assertEquals(
      JsonSchemaCodec[Priced].schema,
      json("""{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","title":"xkafka.jsonschema.Priced",
             |"properties":{"price":{"type":"number","default":1.50}},"required":[],"additionalProperties":false}""".stripMargin)
    )
    assertEquals(
      JsonSchemaCodec[Priced].encode(Priced(BigDecimal("2.25"))).flatMap(JsonSchemaCodec[Priced].decode),
      Right(Priced(BigDecimal("2.25")))
    )

  test("an optional field is oneOf null and its type, and may be left out"):
    assertEquals(
      JsonSchemaCodec[Maybe].schema,
      json("""{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","title":"xkafka.jsonschema.Maybe",
             |"properties":{"note":{"oneOf":[{"type":"null"},{"type":"string"}]}},"required":[],"additionalProperties":false}""".stripMargin)
    )
    assertEquals(JsonSchemaCodec[Maybe].decode(json("{}")), Right(Maybe(None)))
    assertEquals(JsonSchemaCodec[Maybe].encode(Maybe(None)), Right(json("""{"note":null}""")))

  test("an enum of objects is text, one of its cases' names"):
    assertEquals(
      JsonSchemaCodec[Status].schema,
      json("""{"$schema":"http://json-schema.org/draft-07/schema#","type":"string","title":"xkafka.jsonschema.Status","enum":["Open","Paid"]}""")
    )
    assertEquals(JsonSchemaCodec[Status].encode(Status.Paid), Right(Json.fromString("Paid")))

  test("a case that is a sealed trait of objects only is written as objects that tell their case, as the trait it is in reads them"):
    List[Vehicle](Car(4), Stop, Go).foreach: value =>
      assertEquals(JsonSchemaCodec[Vehicle].encode(value).flatMap(JsonSchemaCodec[Vehicle].decode), Right(value))
    assertEquals(JsonSchemaCodec[Vehicle].encode(Stop), Right(Json.obj("type" -> Json.fromString("Stop"))))

  test("a sealed trait is oneOf its cases' objects, each telling its case in the property type"):
    val schema = JsonSchemaCodec[Payment].schema
    assertEquals(
      schema.hcursor.downField("oneOf").downN(1).focus,
      Some(json(
        """{"type":"object","title":"xkafka.jsonschema.Cash","properties":{"type":{"const":"Cash"}},"required":["type"],"additionalProperties":false}"""
      ))
    )
    assertEquals(JsonSchemaCodec[Payment].encode(Card("4242")), Right(json("""{"type":"Card","last4":"4242"}""")))
    assertEquals(JsonSchemaCodec[Payment].decode(json("""{"last4":"1","type":"Card"}""")), Right(Card("1")))
    assertEquals(JsonSchemaCodec[Payment].decode(json("""{"type":"Cash"}""")), Right(Cash))

  test("a case with a field named type, which tells the cases apart, is refused"):
    assertEquals(
      JsonSchemaCodec[Shape].encode(Clash("x")),
      Left("case Clash of xkafka.jsonschema.Shape has a field named 'type', which tells the cases apart")
    )

  test("values of every kind read back as they were written, times in their ISO-8601 forms"):
    val written = JsonSchemaCodec[Order].encode(order)
    assertEquals(written.flatMap(JsonSchemaCodec[Order].decode), Right(order))
    assertEquals(written.map(_.hcursor.get[String]("placed_at")), Right(Right("2026-11-10T07:00:00.123Z")))
    assertEquals(written.map(_.hcursor.get[String]("day")), Right(Right("2026-11-10")))
    assertEquals(written.map(_.hcursor.get[String]("raw")), Right(Right("AQID")))
    val plain = order.copy(payment = Cash, note = None)
    assertEquals(JsonSchemaCodec[Order].encode(plain).flatMap(JsonSchemaCodec[Order].decode), Right(plain))

  test("a record lacking fields reads them as their defaults or none, reads a field under an alias, and ignores properties it does not know"):
    val read =
      JsonSchemaCodec[Order]
        .decode(json("""{"id":"3f2b8c1e-9a4d-4e6f-8b2a-1c3d5e7f9a0b","placed_at":"2026-11-10T07:00:00Z","day":"2026-11-10","lines":[{"sku":"fig"}],
        |"status":"Open","payment":{"type":"Cash"},"attempts":4,"unknown":{"anything":true}}""".stripMargin))
    assertEquals(read.map(value => (value.lines, value.note, value.tags, value.retries)), Right((List(Line("fig")), None, Map.empty, 4)))

  test("a record lacking a required field is refused, naming it"):
    assertEquals(JsonSchemaCodec[Line].decode(json("""{"quantity":2}""")), Left("xkafka.jsonschema.Line: no field 'sku'"))

  test("a type that holds itself is a reference to its definition"):
    val schema = JsonSchemaCodec[Node].schema
    assertEquals(
      schema.hcursor.downField("properties").downField("next").downField("oneOf").downN(1).focus,
      Some(json("""{"$ref":"#/definitions/xkafka.jsonschema.Node"}"""))
    )
    assert(schema.hcursor.downField("definitions").downField("xkafka.jsonschema.Node").focus.isDefined, schema.spaces2)
    assertEquals(JsonSchemaCodec[Node].encode(Node(1, Some(Node(2)))).flatMap(JsonSchemaCodec[Node].decode), Right(Node(1, Some(Node(2)))))

  test("numbers JSON cannot hold are refused"):
    assertEquals(JsonSchemaCodec[Double].encode(Double.NaN), Left("NaN is not a number JSON can hold"))

  test("serdes write the schema's id and then the JSON, and read records written before a field was added"):
    for
      fake   <- FakeRegistry()
      client <-
        SchemaRegistry.of(RegistrySettings.from(NonEmptyList.one("http://registry:8081")).toOption.get, fake.transport("http://registry:8081"))
      topic   <- IO.fromEither(Topic.from("lines").left.map(error => new IllegalArgumentException(error.message)))
      before  <- JsonSchemaSerde.of[IO, LineBefore](client, Part.Value).flatMap(_.serializer.serialize(topic, Headers.empty, LineBefore("apple")))
      read    <- JsonSchemaSerde.of[IO, Line](client, Part.Value).flatMap(_.deserializer.deserialize(topic, Headers.empty, before))
      written <- JsonSchemaSerde.asWritten[IO](client).flatMap(_.deserialize(topic, Headers.empty, before))
      subject <- client.latest(Subject.from("lines-value").toOption.get)
    yield
      assertEquals(before.map(_.drop(5).toArray.toList), Some("""{"sku":"apple"}""".getBytes.toList))
      assertEquals(read, Line("apple", 1))
      assertEquals(written, JsonSchemaWritten(JsonSchemaCodec[LineBefore].schema, json("""{"sku":"apple"}""")))
      assertEquals(subject.map(_.schema.schemaType), Some(SchemaType.Json))

  test("a large value an error names is cut short, saying how long it was"):
    val refused = JsonSchemaCodec[Line].decode(Json.fromString("x" * 100000))
    assert(refused.left.exists(reason => reason.length < 300 && reason.endsWith("... (100002 characters)")), refused)
