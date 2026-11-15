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

package xkafka.avro

import java.time.{Instant, LocalDate, LocalDateTime, LocalTime}
import java.util.UUID

import fs2.Chunk
import munit.FunSuite
import xkafka.schemaregistry.{Schema, SchemaAlias, SchemaDecimal, SchemaDoc, SchemaName, SchemaNamespace, SchemaNoDefault, SchemaType}

final case class Line(sku: String, quantity: Int = 1) derives AvroCodec

final case class Tally(amount: Int) derives AvroCodec

final case class Blob(data: Chunk[Byte]) derives AvroCodec

sealed trait Figure derives AvroCodec
sealed trait Flat                  extends Figure
case object Dot                    extends Flat
case object Dash                   extends Flat
final case class Ring(radius: Int) extends Figure
@AvroFixed(2)
final case class Code(bytes: Chunk[Byte]) extends Figure

final case class Drawn(figure: Figure) derives AvroCodec

sealed trait Pet derives AvroCodec
final case class Cat(lives: Int)   extends Pet
final case class Dog(name: String) extends Pet

object Here:
  final case class Line(x: Int) derives AvroCodec

object There:
  final case class Line(y: Long) derives AvroCodec

final case class Both(here: Here.Line, there: There.Line) derives AvroCodec

/** Each level holds the one below twice, so its schema defines each once and names it after, not once for every way down to it. */
final case class Pair[A](a: A, b: A)
type P0  = Int
type P1  = Pair[P0]
type P2  = Pair[P1]
type P3  = Pair[P2]
type P4  = Pair[P3]
type P5  = Pair[P4]
type P6  = Pair[P5]
type P7  = Pair[P6]
type P8  = Pair[P7]
type P9  = Pair[P8]
type P10 = Pair[P9]
type P11 = Pair[P10]
type P12 = Pair[P11]
type P13 = Pair[P12]
type P14 = Pair[P13]
type P15 = Pair[P14]
type P16 = Pair[P15]

/** Each level derived where it is named, with the level below given. */
object Pairs:
  given p1: AvroCodec[P1]   = AvroCodec.derived
  given p2: AvroCodec[P2]   = AvroCodec.derived
  given p3: AvroCodec[P3]   = AvroCodec.derived
  given p4: AvroCodec[P4]   = AvroCodec.derived
  given p5: AvroCodec[P5]   = AvroCodec.derived
  given p6: AvroCodec[P6]   = AvroCodec.derived
  given p7: AvroCodec[P7]   = AvroCodec.derived
  given p8: AvroCodec[P8]   = AvroCodec.derived
  given p9: AvroCodec[P9]   = AvroCodec.derived
  given p10: AvroCodec[P10] = AvroCodec.derived
  given p11: AvroCodec[P11] = AvroCodec.derived
  given p12: AvroCodec[P12] = AvroCodec.derived
  given p13: AvroCodec[P13] = AvroCodec.derived
  given p14: AvroCodec[P14] = AvroCodec.derived
  given p15: AvroCodec[P15] = AvroCodec.derived
  given p16: AvroCodec[P16] = AvroCodec.derived

// Derived for each type argument where a field uses it, so each is a record of its own name.
final case class Crate[A](value: A)

final case class Boxes(ints: Crate[List[Int]], texts: Crate[List[String]]) derives AvroCodec

@SchemaName("Same")
final case class One(x: Int) derives AvroCodec
@SchemaName("Same")
final case class Two(y: Long) derives AvroCodec

final case class Clashing(one: One, two: Two) derives AvroCodec

final case class Dock(street: String) derives AvroCodec

final case class Shipment(billing: Dock, shipping: Option[Dock] = Some(Dock("dock"))) derives AvroCodec

final case class Chain(v: Int, next: Option[Chain] = Some(Chain(0, None))) derives AvroCodec

/** A default that differs each time it is evaluated. */
final case class Stamp(at: Long = System.nanoTime()) derives AvroCodec
final case class Stamped(created: Stamp, updated: Stamp) derives AvroCodec
final case class TwoChains(a: Chain, b: Chain) derives AvroCodec

final case class Twice(value: Option[Option[String]]) derives AvroCodec

/** A record whose annotations name their arguments. */
@SchemaNamespace(namespace = "com.example") @SchemaName(name = "Order") @SchemaDoc(doc = "An order.") @SchemaAlias(alias = "Purchase")
final case class NamedArguments(
    @SchemaName(name = "n") @SchemaDoc(doc = "How many.")
    count: Int
) derives AvroCodec

/** A sealed trait two of whose records are given one name, and are alike. */
sealed trait Recorded derives AvroCodec
@SchemaName("Created")
final case class FirstCreated(id: Int) extends Recorded
@SchemaName("Created")
final case class SecondCreated(id: Int) extends Recorded

/** A record put in no namespace, as a schema file that names none puts it. */
@SchemaNamespace("")
final case class Unspaced(id: Int) derives AvroCodec

/** Constants annotations take, in place of literals. */
object Constants:
  final val Name      = "Order"
  final val Size      = 4
  final val Precision = 6

/** A record whose annotations take constants the compiler folds. */
@SchemaNamespace("com." + "example") @SchemaName(Constants.Name)
final case class ConstantArguments(
    @SchemaDecimal(Constants.Precision, 2)
    price: BigDecimal,
    hash: ConstantFixed
) derives AvroCodec

@AvroFixed(Constants.Size)
final case class ConstantFixed(bytes: Chunk[Byte]) derives AvroCodec

/** An enum two of whose cases are named alike, in two objects. */
sealed trait Happened derives AvroCodec
object Happened:
  object First:
    case object Created extends Happened
  object Second:
    case object Created extends Happened

sealed trait Shape derives AvroCodec
final case class Split(left: Shape, right: Shape) extends Shape
final case class Tip(v: Int)                      extends Shape

/** A tree whose default moves first the branch of a type defined further into the union. */
final case class Tree(root: Shape = Tip(0)) derives AvroCodec

object Workpackage:
  final case class Item(x: Int) derives AvroCodec

final case class Inner(x: Option[Int] = None) derives AvroCodec

/** Defaults whose values hold a union's later branch inside them, where no reordering of the field's own union can place them. */
final case class Outer(inner: Inner = Inner(Some(5)), counts: List[Option[Int]] = List(Some(1)), plain: Inner = Inner()) derives AvroCodec

final case class Charged(
    @SchemaDecimal(10, 4)
    price: BigDecimal,
    at: Instant,
    @SchemaDecimal(10, 4)
    tip: Option[BigDecimal]
) derives AvroCodec

final case class Reading(level: Double = Double.NaN, peak: Float = Float.PositiveInfinity, low: Double = Double.NegativeInfinity) derives AvroCodec

enum Status derives AvroCodec:
  case Open, Paid, Shipped

sealed trait Payment derives AvroCodec
final case class Card(last4: String) extends Payment
case object Cash                     extends Payment
final case class Transfer(
    iban: String,
    @SchemaDoc("When it cleared")
    cleared: Option[Instant] = None
) extends Payment

@SchemaNamespace("com.example") @SchemaDoc("An order") @SchemaAlias("Purchase")
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
    @SchemaNoDefault
    retries: Int = 0
) derives AvroCodec

final case class Node(value: Int, next: Option[Node] = None) derives AvroCodec

final case class Box[A](value: A)

final case class Preferred(note: Option[String] = Some("none")) derives AvroCodec

final case class Maybe(note: Option[String]) derives AvroCodec

@SchemaName("Line")
final case class LineBefore(sku: String) derives AvroCodec

class AvroCodecSuite extends FunSuite:
  private def json[A](codec: AvroCodec[A]): String = AvroSchemaJson.render(codec.schema)

  private def roundTrip[A](value: A)(using codec: AvroCodec[A]): Either[String, A] =
    val format = AvroFormat[A]
    for
      write <- format.writer(format.schema)
      bytes <- write(value)
      read  <- format.reader(format.schema)
      back  <- read(bytes)
    yield back

  private val order =
    Order(
      UUID.fromString("3f2b8c1e-9a4d-4e6f-8b2a-1c3d5e7f9a0b"),
      Instant.ofEpochMilli(1762761600123L),
      LocalDate.of(2026, 11, 10),
      List(Line("apple", 3), Line("pear")),
      Status.Paid,
      Transfer("DE00", Some(Instant.ofEpochMilli(5))),
      Some("ring twice"),
      Map("priority" -> 2L),
      1
    )

  test("a case class is a record of its fields, in its package, with its defaults"):
    assertEquals(
      json(AvroCodec[Line]),
      """{"type":"record","name":"Line","namespace":"xkafka.avro","fields":[{"name":"sku","type":"string"},{"name":"quantity","type":"int","default":1}]}"""
    )

  test("an enum of objects is an Avro enum of its cases' names"):
    assertEquals(json(AvroCodec[Status]), """{"type":"enum","name":"Status","namespace":"xkafka.avro","symbols":["Open","Paid","Shipped"]}""")

  test("a sealed trait is a union of its cases' records, an object's empty"):
    assertEquals(
      json(AvroCodec[Payment]),
      """[{"type":"record","name":"Card","namespace":"xkafka.avro","fields":[{"name":"last4","type":"string"}]},""" +
        """{"type":"record","name":"Cash","namespace":"xkafka.avro","fields":[]},""" +
        """{"type":"record","name":"Transfer","namespace":"xkafka.avro","fields":[{"name":"iban","type":"string"},""" +
        """{"name":"cleared","type":["null",{"type":"long","logicalType":"timestamp-millis"}],"doc":"When it cleared","default":null}]}]"""
    )

  test("annotations name, place, document and alias a record and its fields, and leave a default out"):
    AvroCodec[Order].schema match
      case AvroSchema.Record(name, fields, doc, aliases) =>
        assertEquals(name, FullName(Some("com.example"), "Order"))
        assertEquals(doc, Some("An order"))
        assertEquals(aliases, List(FullName(Some("com.example"), "Purchase")))
        assertEquals(fields.map(_.name), List("id", "placed_at", "day", "lines", "status", "payment", "note", "tags", "retries"))
        assertEquals(fields.map(_.default.isDefined), List(false, false, false, false, false, false, true, true, false))
        assertEquals(fields.find(_.name == "id").map(_.schema), Some(AvroSchema.Logical(LogicalType.Uuid)))
        assertEquals(fields.find(_.name == "day").map(_.schema), Some(AvroSchema.Logical(LogicalType.Date)))
      case other => fail(s"expected a record, got $other")

  test("values of every kind read back as they were written"):
    assertEquals(roundTrip(order), Right(order))
    assertEquals(
      roundTrip(order.copy(payment = Cash, note = None, status = Status.Open)),
      Right(order.copy(payment = Cash, note = None, status = Status.Open))
    )
    assertEquals(roundTrip[Payment](Card("4242")), Right(Card("4242")))
    assertEquals(roundTrip(Node(1, Some(Node(2, Some(Node(3)))))), Right(Node(1, Some(Node(2, Some(Node(3)))))))

  test("a record that holds itself reads back as deep as records may be nested"):
    val deepest = (1 until AvroBinary.MaxDepth).foldLeft(Node(0))((inner, value) => Node(value, Some(inner)))
    assertEquals(roundTrip(deepest), Right(deepest))

  test("a record that holds itself names itself inside"):
    assertEquals(
      json(AvroCodec[Node]),
      """{"type":"record","name":"Node","namespace":"xkafka.avro","fields":[{"name":"value","type":"int"},{"name":"next","type":["null","Node"],"default":null}]}"""
    )

  test("a generic case class's name carries its type arguments"):
    assertEquals(AvroCodec.derived[Box[Int]].schema.fullName, Some(FullName(Some("xkafka.avro"), "Box__Int")))
    assertEquals(AvroCodec.derived[Box[String]].schema.fullName, Some(FullName(Some("xkafka.avro"), "Box__String")))

  test("an optional value is a union of null and its type, null first, as avro4s and Confluent's tools write it"):
    assertEquals(
      json(AvroCodec[Maybe]),
      """{"type":"record","name":"Maybe","namespace":"xkafka.avro","fields":[{"name":"note","type":["null","string"]}]}"""
    )

  test("an optional field with a value by default puts that branch first, as Avro takes a union's default from its first branch"):
    assertEquals(
      json(AvroCodec[Preferred]),
      """{"type":"record","name":"Preferred","namespace":"xkafka.avro","fields":[{"name":"note","type":["string","null"],"default":"none"}]}"""
    )
    assertEquals(roundTrip(Preferred(None)), Right(Preferred(None)))

  test("a record written before a field was added reads with the field's default"):
    val before  = AvroFormat[LineBefore]
    val written = before.writer(before.schema).flatMap(_(LineBefore("apple")))
    assertEquals(written.flatMap(bytes => AvroFormat[Line].reader(before.schema).flatMap(_(bytes))), Right(Line("apple", 1)))

  test("a value is written with another schema of its record, taking the defaults of fields it lacks and leaving out those it has not"):
    val after =
      Schema(
        """{"type":"record","name":"Line","namespace":"xkafka.avro","fields":[{"name":"sku","type":"string"},{"name":"colour","type":"string","default":"red"}]}""",
        SchemaType.Avro
      )
    val written = AvroFormat[Line].writer(after).flatMap(_(Line("apple", 3)))
    assertEquals(
      written.flatMap(AvroBinary.decode(AvroSchemaJson.parse(after.text).toOption.get, _)),
      Right(AvroValue.Record(FullName(Some("xkafka.avro"), "Line"), Vector("sku" -> AvroValue.String("apple"), "colour" -> AvroValue.String("red"))))
    )

  test("a record whose defaults are not numbers, NaN or an infinity, is written and read with its own schema"):
    val format = AvroFormat[Reading]
    val value  = Reading(1.5, 2.5f, -3.5)
    assertEquals(format.writer(format.schema).flatMap(_(value)).flatMap(bytes => format.reader(format.schema).flatMap(_(bytes))), Right(value))

  test("a value written with another schema of its record is read into it as the schema says: through a field's alias, and an int as a long"):
    def schema(fields: String)  = Schema(s"""{"type":"record","name":"Tally","namespace":"xkafka.avro","fields":[$fields]}""", SchemaType.Avro)
    def written(target: Schema) =
      AvroFormat[Tally].writer(target).flatMap(_(Tally(7))).flatMap(AvroBinary.decode(AvroSchemaJson.parse(target.text).toOption.get, _))
    val name = FullName(Some("xkafka.avro"), "Tally")
    assertEquals(
      written(schema("""{"name":"total","type":"int","default":0,"aliases":["amount"]}""")),
      Right(AvroValue.Record(name, Vector("total" -> AvroValue.Int(7))))
    )
    assertEquals(written(schema("""{"name":"amount","type":"long"}""")), Right(AvroValue.Record(name, Vector("amount" -> AvroValue.Long(7L)))))

  test("types of one simple name in two objects, and a generic type of two type arguments, are records of their own names"):
    def roundTrip[A](value: A)(using codec: AvroCodec[A]) =
      AvroFormat[A].writer(AvroFormat[A].schema).flatMap(_(value)).flatMap(bytes => AvroFormat[A].reader(AvroFormat[A].schema).flatMap(_(bytes)))
    val both = AvroSchemaJson.render(AvroCodec[Both].schema)
    assert(both.contains("\"xkafka.avro.Here\"") && both.contains("\"xkafka.avro.There\""), both)
    assertEquals(roundTrip(Both(Here.Line(1), There.Line(2L))), Right(Both(Here.Line(1), There.Line(2L))))
    val boxes = AvroSchemaJson.render(AvroCodec[Boxes].schema)
    assert(boxes.contains("Crate__List__Int") && boxes.contains("Crate__List__String"), boxes)
    assertEquals(roundTrip(Boxes(Crate(List(1)), Crate(List("a")))), Right(Boxes(Crate(List(1)), Crate(List("a")))))

  test("an object whose name ends in 'package' is in its types' namespace, as only the object Scala makes around a file's definitions is left out"):
    assertEquals(AvroCodec[Workpackage.Item].schema.fullName.map(_.value), Some("xkafka.avro.Workpackage.Item"))

  test("a value nested deeper than the limit is refused as it is encoded, with the reading limit's reason, not encoded until the stack runs out"):
    def nodes(count: Int) = (1 until count).foldLeft(Node(0))((inner, value) => Node(value, Some(inner)))
    val refused           = s"the value is nested more than ${AvroBinary.MaxDepth} deep"
    assertEquals(AvroCodec[Node].encode(nodes(100000)).left.toOption.exists(_.endsWith(refused)), true)
    assertEquals(AvroCodec[Node].encode(nodes(AvroBinary.MaxDepth + 1)).left.toOption.exists(_.endsWith(refused)), true)
    assert(AvroCodec[Node].encode(nodes(AvroBinary.MaxDepth)).isRight)

  test("an enum two of whose cases have one symbol, and a union two of whose branches are of one type, are refused, as Avro allows neither"):
    assertEquals(AvroCodec[Happened].refusal, Some("enum 'xkafka.avro.Happened' holds the symbol 'Created' twice"))
    assertEquals(AvroCodec.union[(java.time.Instant, Long)].refusal, Some("a union holds 'Long' twice"))
    assertEquals(AvroCodec[Recorded].refusal, Some("'xkafka.avro.Recorded' has two cases named 'xkafka.avro.Created'"))
    assertEquals(AvroCodec.union[(Int, String)].refusal, None)

  test("a union of a named type and a primitive of the same name is read, as their full names differ"):
    val text = """["string",{"type":"record","name":"String","fields":[]},{"type":"map","values":"int"},{"type":"record","name":"Map","fields":[]}]"""
    assert(AvroSchemaJson.parse(text).isRight, AvroSchemaJson.parse(text))
    assertEquals(AvroSchemaJson.parse("""["string","string"]"""), Left("a union holds 'String' twice"))

  test("annotations whose arguments are named are read as those given in place"):
    assertEquals(
      AvroSchemaJson.render(AvroCodec[NamedArguments].schema),
      """{"type":"record","name":"Order","namespace":"com.example","doc":"An order.","fields":[{"name":"n","type":"int","doc":"How many."}],"aliases":["Purchase"]}"""
    )
    assert(
      compileErrors("""val name = "x"; @SchemaName(name) final case class Computed(n: Int) derives AvroCodec""")
        .contains("@SchemaName takes constant text")
    )
    assert(
      compileErrors("final case class Computed(@SchemaDoc(\"\"\"|a\n  |b\"\"\".stripMargin) n: Int) derives AvroCodec")
        .contains("@SchemaDoc takes constant text")
    )

  test("a type given an empty namespace has none, so its schema matches a file that names none"):
    val file = """{"type":"record","name":"Unspaced","fields":[{"name":"id","type":"int"}]}"""
    assertEquals(AvroSchemaJson.render(AvroCodec[Unspaced].schema), file)
    assertEquals(AvroFormat(using AvroCodec[Unspaced]).recordName, Some("Unspaced"))
    assertEquals(AvroCodec[Unspaced].matching(file).refusal, None)

  test("annotations take any constant the compiler folds, such as a final val or joined literals"):
    assertEquals(
      AvroSchemaJson.render(AvroCodec[ConstantArguments].schema),
      """{"type":"record","name":"Order","namespace":"com.example","fields":[{"name":"price","type":{"type":"bytes","logicalType":"decimal","precision":6,"scale":2}},{"name":"hash","type":{"type":"fixed","name":"ConstantFixed","namespace":"xkafka.avro","size":4}}]}"""
    )

  test("an option holding an option is refused, as its none and some none would both be written as null"):
    val message = Some("an option holding an option cannot be told from none, as null is a value of both")
    assertEquals(AvroCodec[Option[Option[Int]]].refusal, message)
    assertEquals(AvroCodec[Twice].refusal, message)
    assertEquals(AvroCodec[Option[Int]].refusal, None)

  test("a type a default moves ahead of where it is defined is written in full where it is first met, so the schema reads again"):
    val text = AvroSchemaJson.render(AvroCodec[Tree].schema)
    assertEquals(AvroCodec[Tree].refusal, None)
    assertEquals(AvroSchemaJson.parse(text).map(AvroSchemaJson.render), Right(text), text)
    assertEquals(roundTrip(Tree(Split(Tip(1), Tip(2)))), Right(Tree(Split(Tip(1), Tip(2)))))

  test("a type in a package object is in its package's namespace, as one in the object Scala makes around a file's definitions is"):
    assertEquals(AvroCodec[legacy.Old].schema.fullName.map(_.value), Some("xkafka.avro.legacy.Old"))

  test("a type used twice is one type, though its default differs between builds or is placed only once the type is complete"):
    assertEquals(AvroCodec[Stamped].refusal, None)
    assertEquals(AvroCodec[TwoChains].refusal, None)

  test("a default holding a union's later branch inside it is left out, as Avro takes a nested union's default from its first branch"):
    val schema = AvroCodec[Outer].schema
    val fields =
      schema match
        case AvroSchema.Record(_, fields, _, _) => fields.map(field => field.name -> field.default.isDefined).toMap;
        case _                                  => Map.empty
    assertEquals(fields, Map("inner" -> false, "counts" -> false, "plain" -> true))
    assertEquals(AvroSchemaJson.parse(AvroSchemaJson.render(schema)), Right(schema))

  test("two types given one name are refused, since one schema cannot say what each is"):
    assert(AvroFormat[Clashing].writer(AvroFormat[Clashing].schema).left.exists(_.contains("Same")))
    assert(AvroFormat[Clashing].reader(AvroFormat[Clashing].schema).left.exists(_.contains("Same")))

  test("a type used twice is defined once and named after, so a codec's schema is the one its text reads back as"):
    assertEquals(AvroSchemaJson.parse(AvroSchemaJson.render(AvroCodec[Shipment].schema)), Right(AvroCodec[Shipment].schema))

  test("a type each level holds twice is defined once at each level, not once for every way down to it"):
    def records(schema: AvroSchema): Int =
      schema match
        case AvroSchema.Record(_, fields, _, _) => 1 + fields.map(field => records(field.schema)).sum
        case AvroSchema.Union(branches)         => branches.map(records).sum
        case AvroSchema.Array(items)            => records(items)
        case AvroSchema.Map(values)             => records(values)
        case _                                  => 0
    assertEquals(records(Pairs.p16.schema), 16)

  test("a default of a type defined before it is kept in its union's branch, and one of the record it is in is left out, not written as null"):
    val order = AvroSchemaJson.render(AvroCodec[Shipment].schema)
    assert(order.contains("\"default\":{\"street\":\"dock\"}"), order)
    val node = AvroSchemaJson.render(AvroCodec[Chain].schema)
    assert(!node.contains("\"default\":null"), node)

  test("a union of types one of which is a union itself is one union of all their branches, as Avro has no union inside a union"):
    given AvroCodec[Pet | String] = AvroCodec.union[(Pet, String)]
    val schema                    = AvroCodec[Pet | String].schema
    assertEquals(
      schema match
        case AvroSchema.Union(branches) => branches.flatMap(_.fullName).map(_.name) ++ branches.filter(_ == AvroSchema.String).map(_ => "string");
        case _                          => Nil
      ,
      List("Cat", "Dog", "string")
    )
    def roundTrip(value: Pet | String) =
      AvroCodec[Pet | String].encode(value).flatMap(written => AvroBinary.encode(schema, written)).flatMap(AvroBinary.decode(schema, _))
        .flatMap(AvroCodec[Pet | String].decode)
    List[Pet | String](Cat(9), Dog("rex"), "plain").foreach(value => assertEquals(roundTrip(value), Right(value)))

  test("a case of a sealed trait that is itself an enum, or a fixed, reads back as it was written"):
    def roundTrip(value: Drawn) =
      AvroFormat[Drawn].writer(AvroFormat[Drawn].schema).flatMap(_(value))
        .flatMap(bytes => AvroFormat[Drawn].reader(AvroFormat[Drawn].schema).flatMap(_(bytes)))
    List(Drawn(Dot), Drawn(Ring(2)), Drawn(Code(Chunk[Byte](1, 2)))).foreach(value => assertEquals(roundTrip(value), Right(value)))

  test("a value is not written with another schema whose bytes are text, since bytes that are not UTF-8 would be changed"):
    def target(kind: String) =
      Schema(s"""{"type":"record","name":"Blob","namespace":"xkafka.avro","fields":[{"name":"data","type":$kind}]}""", SchemaType.Avro)
    assert(AvroFormat[Blob].writer(target("\"bytes\"")).isRight)
    assert(AvroFormat[Blob].writer(target("\"string\"")).left.exists(_.contains("string")))
    assert(AvroFormat[Blob].writer(target("""["null","string"]""")).isLeft)

  test("a value is not written with another schema whose decimals have another scale or fewer digits, or whose times count another unit"):
    def target(price: String, at: String, tip: String) =
      Schema(
        s"""{"type":"record","name":"Charged","namespace":"xkafka.avro","fields":[{"name":"price","type":$price},""" +
          s"""{"name":"at","type":$at},{"name":"tip","type":["null",$tip]}]}""",
        SchemaType.Avro
      )
    def decimal(precision: Int, scale: Int) = s"""{"type":"bytes","logicalType":"decimal","precision":$precision,"scale":$scale}"""
    val millis                              = """{"type":"long","logicalType":"timestamp-millis"}"""
    def writes(schema: Schema)              = AvroFormat[Charged].writer(schema).isRight
    assert(writes(target(decimal(10, 4), millis, decimal(10, 4))), "the codec's own")
    assert(writes(target(decimal(12, 4), millis, decimal(12, 4))), "room for more digits")
    assert(!writes(target(decimal(10, 2), millis, decimal(10, 4))), "another scale")
    assert(!writes(target(decimal(8, 4), millis, decimal(10, 4))), "fewer digits")
    assert(!writes(target(decimal(10, 4), millis, decimal(10, 2))), "another scale in a union")
    assert(!writes(target(decimal(10, 4), """{"type":"long","logicalType":"timestamp-micros"}""", decimal(10, 4))), "microseconds")

  test("decimals keep their digits, and refuse a number with more than the schema allows rather than round it"):
    val money = AvroCodec.decimal(6, 2)
    assertEquals(money.encode(BigDecimal("1234.5")).flatMap(money.decode), Right(BigDecimal("1234.50")))
    assertEquals(money.encode(BigDecimal("-0.01")).flatMap(money.decode), Right(BigDecimal("-0.01")))
    assertEquals(money.encode(BigDecimal("1.005")), Left("1.005 has more than 2 digits after the point"))
    assertEquals(money.encode(BigDecimal("12345.67")), Left("12345.67 has more than 6 digits"))
    assertEquals(money.decode(AvroValue.Bytes(Chunk.empty)), Left("a decimal of no bytes"))

  test("times keep what their logical type keeps"):
    val instant = Instant.parse("2026-11-10T07:00:00.123456789Z")
    assertEquals(AvroCodec[Instant].encode(instant), Right(AvroValue.Long(instant.toEpochMilli)))
    assertEquals(AvroCodec.instantMicros.encode(instant).flatMap(AvroCodec.instantMicros.decode), Right(Instant.parse("2026-11-10T07:00:00.123456Z")))
    assertEquals(AvroCodec.instantNanos.encode(instant).flatMap(AvroCodec.instantNanos.decode), Right(instant))
    assertEquals(AvroCodec.instantMicros.encode(Instant.parse("1969-12-31T23:59:59.999999Z")), Right(AvroValue.Long(-1)))
    assertEquals(AvroCodec.instantMicros.decode(AvroValue.Long(-1)), Right(Instant.parse("1969-12-31T23:59:59.999999Z")))
    assertEquals(AvroCodec.instantNanos.decode(AvroValue.Long(-1)), Right(Instant.parse("1969-12-31T23:59:59.999999999Z")))
    assertEquals(AvroCodec[LocalDate].encode(LocalDate.of(1970, 1, 2)), Right(AvroValue.Int(1)))
    assertEquals(AvroCodec[LocalTime].encode(LocalTime.of(0, 0, 1)), Right(AvroValue.Long(1000000)))
    assertEquals(AvroCodec.localTimeMillis.encode(LocalTime.of(0, 0, 1)), Right(AvroValue.Int(1000)))
    val local = LocalDateTime.of(2026, 11, 10, 7, 0, 0, 1000)
    assertEquals(AvroCodec[LocalDateTime].encode(local).flatMap(AvroCodec[LocalDateTime].decode), Right(local))
    assert(AvroCodec.instantNanos.encode(Instant.MAX).isLeft, "an instant past what nanoseconds since 1970 hold is refused")

  test("fields marked SchemaDecimal are decimals of their own precision and scale, inside options and lists too, and with their defaults"):
    final case class Priced(
        @SchemaDecimal(10, 2)
        price: BigDecimal,
        @SchemaDecimal(precision = 5, scale = 1)
        discount: Option[BigDecimal] = Some(BigDecimal("1.5")),
        @SchemaDecimal(4, 0)
        counts: List[BigDecimal] = Nil
    ) derives AvroCodec
    assertEquals(
      AvroSchemaJson.render(AvroCodec[Priced].schema),
      """{"type":"record","name":"Priced","namespace":"xkafka.avro","fields":[""" +
        """{"name":"price","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}},""" +
        """{"name":"discount","type":[{"type":"bytes","logicalType":"decimal","precision":5,"scale":1},"null"],"default":"\u000f"},""" +
        """{"name":"counts","type":{"type":"array","items":{"type":"bytes","logicalType":"decimal","precision":4,"scale":0}},"default":[]}]}"""
    )
    val priced = Priced(BigDecimal("12.5"), None, List(BigDecimal(7)))
    assertEquals(roundTrip(priced), Right(Priced(BigDecimal("12.50"), None, List(BigDecimal(7)))))
    assertEquals(
      AvroCodec[Priced].encode(priced.copy(price = BigDecimal("1.005"))),
      Left("record 'xkafka.avro.Priced': 1.005 has more than 2 digits after the point")
    )

  test("SchemaDecimal takes the place of a decimal codec given for every BigDecimal, which unmarked fields take"):
    given AvroCodec[BigDecimal] = AvroCodec.decimal(6, 3)
    final case class Mixed(
        @SchemaDecimal(8, 1)
        marked: BigDecimal,
        unmarked: BigDecimal
    ) derives AvroCodec
    assertEquals(
      AvroCodec[Mixed].schema,
      AvroSchema.Record(
        FullName(Some("xkafka.avro"), "Mixed"),
        List(AvroField("marked", AvroSchema.Logical(LogicalType.Decimal(8, 1))), AvroField("unmarked", AvroSchema.Logical(LogicalType.Decimal(6, 3))))
      )
    )

  test("SchemaDecimal on a field without a BigDecimal, or with a precision and scale Avro does not allow, fails to compile"):
    assert(
      compileErrors("final case class Wrong(@SchemaDecimal(4, 1) text: String) derives AvroCodec")
        .contains("@SchemaDecimal on text, which holds no BigDecimal")
    )
    assert(
      compileErrors("final case class Wrong(@SchemaDecimal(2, 3) price: BigDecimal) derives AvroCodec")
        .contains("@SchemaDecimal(2, 3) on price: the precision must be at least 1 and the scale from 0 to the precision")
    )
    assert(
      compileErrors("final case class Wrong(@SchemaDecimal(0, 0) price: BigDecimal) derives AvroCodec")
        .contains("@SchemaDecimal(0, 0) on price: the precision must be at least 1")
    )
    assert(
      compileErrors("final case class Wrong(price: BigDecimal) derives AvroCodec").contains("xkafka.avro.AvroCodec[BigDecimal]"),
      "an unmarked BigDecimal still needs a codec"
    )

  test("a case class marked AvroFixed is a fixed of its size, written as its bytes alone, and named where it is used again"):
    @SchemaAlias("Digest") @AvroFixed(4)
    final case class Hash(bytes: Chunk[Byte]) derives AvroCodec
    @AvroFixed(size = 2)
    final case class Tag(bytes: Array[Byte]) derives AvroCodec
    final case class File(hash: Hash, previous: Option[Hash], tag: Tag) derives AvroCodec
    assertEquals(
      json(AvroCodec[File]),
      """{"type":"record","name":"File","namespace":"xkafka.avro","fields":[""" +
        """{"name":"hash","type":{"type":"fixed","name":"Hash","size":4,"aliases":["Digest"]}},""" +
        """{"name":"previous","type":["null","Hash"]},{"name":"tag","type":{"type":"fixed","name":"Tag","size":2}}]}"""
    )
    val file  = File(Hash(Chunk[Byte](1, 2, 3, 4)), None, Tag(Array[Byte](9, 8)))
    val bytes = AvroFormat[File].writer(AvroFormat[File].schema).flatMap(_(file))
    assertEquals(bytes, Right(Chunk[Byte](1, 2, 3, 4, 0, 9, 8)))
    assertEquals(roundTrip(file).map(read => (read.hash, read.previous, read.tag.bytes.toList)), Right((file.hash, None, List[Byte](9, 8))))
    assertEquals(AvroCodec[Hash].encode(Hash(Chunk[Byte](1, 2, 3))), Left("fixed 'xkafka.avro.Hash': 3 bytes, not 4"))

  test("AvroFixed on a case class without exactly one field of bytes, with a negative size, or documented, fails to compile"):
    assert(
      compileErrors("@AvroFixed(4) final case class Wrong(text: String) derives AvroCodec")
        .contains("@AvroFixed on Wrong, which must have one field, of fs2.Chunk[Byte] or Array[Byte]")
    )
    assert(
      compileErrors("@AvroFixed(4) final case class Wrong(a: Chunk[Byte], b: Chunk[Byte]) derives AvroCodec")
        .contains("@AvroFixed on Wrong, which must have one field")
    )
    assert(
      compileErrors("@AvroFixed(-1) final case class Wrong(a: Chunk[Byte]) derives AvroCodec")
        .contains("@AvroFixed(-1) on Wrong: the size must not be negative")
    )
    assert(
      compileErrors("@SchemaDoc(\"a hash\") @AvroFixed(4) final case class Wrong(a: Chunk[Byte]) derives AvroCodec")
        .contains("@SchemaDoc on Wrong, which @AvroFixed makes a fixed: an Avro fixed has no documentation")
    )

  test("an enum marked AvroEnumDefault declares its default, which a reader takes for a symbol it does not know"):
    @AvroEnumDefault("LOW")
    enum Level derives AvroCodec:
      case LOW, HIGH
    assertEquals(json(AvroCodec[Level]), """{"type":"enum","name":"Level","namespace":"xkafka.avro","symbols":["LOW","HIGH"],"default":"LOW"}""")
    val writer  = Schema(AvroSchemaJson.render(AvroSchema.Enum(FullName(Some("xkafka.avro"), "Level"), List("LOW", "MID", "HIGH"))), SchemaType.Avro)
    val written =
      AvroFormat(using AvroCodec.generic(AvroSchemaJson.parse(writer.text).toOption.get)).writer(writer)
        .flatMap(_(AvroValue.Enum(FullName(Some("xkafka.avro"), "Level"), "MID")))
    assertEquals(written.flatMap(bytes => AvroFormat[Level].reader(writer).flatMap(_(bytes))), Right(Level.LOW))

  test("a sealed trait of objects marked AvroEnumDefault takes a case's name for the schema, as SchemaName gives it"):
    @AvroEnumDefault("FAST")
    sealed trait Speed derives AvroCodec
    @SchemaName("FAST")
    case object Quick extends Speed
    case object Slow  extends Speed
    assertEquals(json(AvroCodec[Speed]), """{"type":"enum","name":"Speed","namespace":"xkafka.avro","symbols":["FAST","Slow"],"default":"FAST"}""")

  test("AvroEnumDefault naming no case, or on a type whose cases are not all objects, fails to compile"):
    assert(
      compileErrors("@AvroEnumDefault(\"MID\") enum Wrong derives AvroCodec { case LOW, HIGH }")
        .contains("@AvroEnumDefault(\"MID\") on Wrong, which has no case MID: LOW, HIGH")
    )
    assert(
      compileErrors("@AvroEnumDefault(\"A\") enum Wrong derives AvroCodec { case A; case B(x: Int) }")
        .contains("@AvroEnumDefault on Wrong, whose cases are not all objects, so it is not an Avro enum")
    )

  test("a union type is a union of its types in the order given, written as the type a value is of and read back by the record's name"):
    final case class Cash(cents: Long) derives AvroCodec
    final case class Refund(cents: Long) derives AvroCodec
    final case class Card(number: String) derives AvroCodec
    given AvroCodec[Cash | Refund | Card] = AvroCodec.union[(Cash, Refund, Card)]
    final case class Payment(method: Cash | Refund | Card, previous: Option[Cash | Refund | Card] = None) derives AvroCodec
    assertEquals(
      json(AvroCodec[Payment]),
      """{"type":"record","name":"Payment","namespace":"xkafka.avro","fields":[{"name":"method","type":[""" +
        """{"type":"record","name":"Cash","fields":[{"name":"cents","type":"long"}]},""" +
        """{"type":"record","name":"Refund","fields":[{"name":"cents","type":"long"}]},""" +
        """{"type":"record","name":"Card","fields":[{"name":"number","type":"string"}]}]},""" +
        """{"name":"previous","type":["null","Cash","Refund","Card"],"default":null}]}"""
    )
    List(Payment(Refund(5)), Payment(Cash(5), Some(Refund(2))), Payment(Card("4111"), Some(Card("0")))).foreach: payment =>
      assertEquals(roundTrip(payment), Right(payment))

  test("a union of primitive types tells them apart by the value's class"):
    given AvroCodec[Int | Long | String] = AvroCodec.union[(Int, Long, String)]
    assertEquals(json(AvroCodec[Int | Long | String]), """["int","long","string"]""")
    List[Int | Long | String](7, 7L, "7").foreach: value =>
      assertEquals(AvroCodec[Int | Long | String].encode(value).flatMap(AvroCodec[Int | Long | String].decode), Right(value))
    assertEquals(AvroCodec[Int | Long | String].encode(7L), Right(AvroValue.Long(7L)))

  test("a schema's long default keeps every digit, beyond what a double holds"):
    val text = """{"type":"record","name":"Big","fields":[{"name":"n","type":"long","default":9007199254740993}]}"""
    assertEquals(AvroSchemaJson.parse(text).map(AvroSchemaJson.render), Right(text))

  test("bytes, sets, vectors and maps read back as they were written"):
    final case class Kinds(raw: Chunk[Byte], set: Set[Int], vector: Vector[Double], map: Map[String, Option[Boolean]], float: Float) derives AvroCodec
    val kinds = Kinds(Chunk[Byte](0, -1, 7), Set(3, 1), Vector(1.5, -2.25), Map("a" -> Some(true), "b" -> None), 0.5f)
    assertEquals(roundTrip(kinds), Right(kinds))

  test("byte arrays are written as bytes, and what was written does not change with the array"):
    final case class Blob(raw: Array[Byte], default: Array[Byte] = Array[Byte](1, 2)) derives AvroCodec
    val raw     = Array[Byte](0, -1, 7)
    val written = AvroCodec[Blob].encode(Blob(raw))
    raw(0) = 9
    val schema = AvroSchemaJson.render(AvroCodec[Blob].schema)
    assert(
      schema.contains("\"fields\":[{\"name\":\"raw\",\"type\":\"bytes\"},{\"name\":\"default\",\"type\":\"bytes\",\"default\":\"\\u0001\\u0002\"}]"),
      schema
    )
    assertEquals(
      written.flatMap(AvroCodec[Blob].decode).map(blob => (blob.raw.toList, blob.default.toList)),
      Right((List[Byte](0, -1, 7), List[Byte](1, 2)))
    )

  test("records written as one type are refused by a codec of another"):
    val bytes = AvroFormat[Line].writer(AvroFormat[Line].schema).flatMap(_(Line("x", 2)))
    val read  = bytes.flatMap(written => AvroFormat[Status].reader(AvroFormat[Line].schema).flatMap(_(written)))
    assert(read.left.exists(_.contains("cannot read what")), read.toString)

  test("a large value an error names is cut short, saying how long it was"):
    val refused = AvroCodec[Line].decode(AvroValue.String("x" * 100000))
    assert(refused.left.exists(reason => reason.length < 300 && reason.endsWith("... (100008 characters)")), refused)
