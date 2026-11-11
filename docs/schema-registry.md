# Schema registry

xkafka can write records with schemas kept in a Confluent Schema Registry, and
read them back, on the JVM, Scala.js and Scala Native. It supports Avro, JSON
Schema and Protobuf. Records are written in Confluent's wire format, so Java
applications using Confluent's serializers and deserializers read what xkafka
writes, and xkafka reads what they write.

## Dependencies

```scala
// Avro serdes, the registry client they use, and http4s's Ember client to reach the registry
libraryDependencies ++= Seq(
  "io.github.pmbmegalom" %%% "xkafka-avro"                   % "@VERSION@",
  "io.github.pmbmegalom" %%% "xkafka-schema-registry-http4s" % "@VERSION@",
  "org.http4s"           %%% "http4s-ember-client"           % "0.23.38"
)
```

- `xkafka-avro` gives you Avro serdes for your types, `xkafka-json-schema` JSON
  Schema serdes, and `xkafka-protobuf` Protobuf serdes. Add the ones you use.
  Each brings in `xkafka-schema-registry`, the registry client.
- `xkafka-schema-registry-http4s` lets the registry client use any http4s
  client. Ember is shown here, but any http4s client works. To use an HTTP
  client that is not http4s's, see [Another HTTP client](#another-http-client).

On Scala Native, Ember needs the [s2n-tls](https://github.com/aws/s2n-tls)
library, even when the registry is reached over plain HTTP, wherever the program
is built and run. On macOS, Homebrew's `s2n` package provides it; Ubuntu does not
package it, so build it from source there.

## Connecting to the registry

The registry is reached through your own HTTP client, which decides on TLS,
authentication, proxies and timeouts:

```scala mdoc:compile-only
// Connect to the registry at localhost:8081 through an Ember client
import cats.data.NonEmptyList
import cats.effect.{IO, Resource}
import org.http4s.ember.client.EmberClientBuilder

import xkafka.schemaregistry.{RegistrySettings, SchemaRegistry}
import xkafka.schemaregistry.http4s.Http4sTransport

val registry: Resource[IO, SchemaRegistry[IO]] =
  for
    client   <- EmberClientBuilder.default[IO].build
    settings <- Resource.eval(RegistrySettings.from(NonEmptyList.one("http://localhost:8081")).liftTo[IO])
    registry <- Resource.eval(SchemaRegistry.of(settings, Http4sTransport(client)))
  yield registry
```

To authenticate, wrap the client so it adds the credentials to every request,
and pass the wrapped client to `Http4sTransport`:

```scala mdoc:compile-only
// Send basic authentication with every request to the registry
import cats.effect.IO
import org.http4s.BasicCredentials
import org.http4s.client.Client
import org.http4s.headers.Authorization

def authenticated(client: Client[IO], user: String, password: String): Client[IO] =
  Client(request => client.run(request.putHeaders(Authorization(BasicCredentials(user, password)))))
```

`RegistrySettings.from` takes:

- one or more registry URLs, each `http` or `https`. A request that cannot reach
  one is sent to the next.
- `latestCachedFor`: how long a subject's latest version is cached. By default it
  is cached until the program ends, as Confluent's serializers cache it. Use
  `withLatestCachedFor(duration)` to see a new latest version within `duration`,
  and `withoutLatestExpiry` to go back to the default.

A URL that is not `http` or `https`, or a negative duration, is refused;
`liftTo` raises the refusal as an `InvalidRegistrySetting`.

Schemas, and the ids they are registered under, never change, so they are cached
for good: each schema is fetched once, however many records use it.

### Another HTTP client

The registry client only needs a function that sends one request and gives back
the answer:

```scala
type Transport[F[_]] = RegistryRequest => F[RegistryResponse]
```

A `RegistryRequest` has a method, an absolute URL, headers and an optional JSON
body; a `RegistryResponse` has the status code and the body. Fail the effect
only when the registry cannot be reached: an error status is a response like any
other. Pass your function to `SchemaRegistry.of` in place of `Http4sTransport`.

## Writing and reading records

Each format has a codec, which you derive for your type, and an object that
makes serdes in three ways:

- `of` writes and reads your own type, with the schema its codec derives.
- `generic` writes and reads values of a schema your program only learns as it
  runs.
- `asWritten` reads any record with the schema it was written with, for tools
  that show or copy records.

| | Avro | JSON Schema | Protobuf |
| --- | --- | --- | --- |
| Module | `xkafka-avro` | `xkafka-json-schema` | `xkafka-protobuf` |
| Package | `xkafka.avro` | `xkafka.jsonschema` | `xkafka.protobuf` |
| Codec | `AvroCodec` | `JsonSchemaCodec` | `ProtobufCodec` |
| Serdes | `AvroSerde` | `JsonSchemaSerde` | `ProtobufSerde` |
| A record's fields are found by | name, resolved against your schema | name | number |
| Values of a schema known at run time | `AvroValue` | circe's `Json` | `ProtobufValue` |

Here an Avro serde is given to a producer; `JsonSchemaSerde.of` and
`ProtobufSerde.of` are used the same way:

```scala mdoc:compile-only
// Write orders as Avro values, registering their schema under "orders-value" on the first write
import java.time.Instant

import cats.effect.{IO, Resource}

import xkafka.*
import xkafka.avro.{AvroCodec, AvroSerde}
import xkafka.schemaregistry.{Part, SchemaRegistry}

final case class Line(sku: String, quantity: Int = 1) derives AvroCodec
final case class Order(id: String, placedAt: Instant, lines: List[Line], note: Option[String] = None) derives AvroCodec

def orders(registry: SchemaRegistry[IO], client: ClientSettings): Resource[IO, KafkaProducer[IO, String, Order]] =
  for
    serde    <- Resource.eval(AvroSerde.of[IO, Order](registry, Part.Value))
    settings <- Resource.eval(ProducerSettings.from(client, Serializer.utf8[IO], serde.serializer).liftTo[IO])
    producer <- KafkaClient[IO].producer(settings)
  yield producer
```

The serde is a `Serde`: a producer takes its `serializer`, a consumer its
`deserializer`, and a streams program the serde itself. Use `Part.Key` for a
serde of keys. `of` and `generic` also take a `SubjectNaming`, which names the
subject a schema is kept under (see [Subjects](#subjects)), and a `Writing`,
which chooses the schema values are written with (see
[Which schema is written](#which-schema-is-written)).

Each record carries the id of the schema it was written with. Reading fetches
that schema, so records written with an older or newer version of your type
are read. What changes a reader accepts depends on the format; each format's
section says.

A record without a value, such as a tombstone, fails to read with
`Deserializer.NullValueException`. Read with `serde.option`, or
`serde.deserializer.option`, to get `None` for it; writing `None` through
`serde.option` writes a record without a value.

## Avro

The example above derives Avro codecs with `derives AvroCodec`.

### Derived Avro schemas

`derives AvroCodec` makes a schema from the type:

| Scala | Avro |
| --- | --- |
| `Boolean`, `Int`, `Long`, `Float`, `Double`, `String` | `boolean`, `int`, `long`, `float`, `double`, `string` |
| `fs2.Chunk[Byte]` | `bytes` |
| `Option[A]` | a union of `null` and `A`, `null` first |
| `List`, `Vector`, `Seq`, `Set` | `array` |
| `Map[String, A]` | `map` |
| `java.util.UUID` | `string` with logical type `uuid` |
| `java.time.Instant` | `long` with logical type `timestamp-millis` |
| `java.time.LocalDate` | `int` with logical type `date` |
| `java.time.LocalTime` | `long` with logical type `time-micros` |
| `java.time.LocalDateTime` | `long` with logical type `local-timestamp-micros` |
| a case class | a `record` of its fields |
| an enum or sealed trait whose cases are all objects | an `enum` of the cases' names |
| any other sealed trait or enum | a union of its cases' records, an object's having no fields |

- A record is named after its class, in the namespace of its package. A generic
  class's name carries its type arguments: `Box[Int]` is `Box__Int`.
- A field's Scala default is its default in the schema. An optional field whose
  default is a value has that value's branch first in its union, since Avro takes
  a union's default from its first branch.
- Give a type that holds itself, such as a linked list's node, its codec with
  `derives AvroCodec` on that type.

`AvroCodec.instantMicros`, `AvroCodec.instantNanos`,
`AvroCodec.localTimeMillis` and `AvroCodec.localDateTimeMillis` write those
types with the other precisions Avro has. Make one the given codec to use it:

```scala mdoc:compile-only
// Write instants to the microsecond
import java.time.Instant

import xkafka.avro.AvroCodec

given AvroCodec[Instant] = AvroCodec.instantMicros

final case class Reading(sensor: String, at: Instant) derives AvroCodec
```

Decimal numbers have no codec by default, since their precision and scale are
part of the schema. Give one:

```scala mdoc:compile-only
// Write prices with at most 10 digits, 2 of them after the point
import xkafka.avro.AvroCodec

given AvroCodec[BigDecimal] = AvroCodec.decimal(precision = 10, scale = 2)

final case class Price(sku: String, amount: BigDecimal) derives AvroCodec
```

A number with more digits, or more digits after the point, fails to write; it is
not rounded.

For a type of your own, map it to one that has a codec:

```scala mdoc:compile-only
// Write a customer id as the text it wraps
import xkafka.avro.AvroCodec

final case class CustomerId(value: String)

given AvroCodec[CustomerId] = AvroCodec[String].imap(CustomerId(_))(_.value)
```

`emap` does the same where reading can refuse a value.

### Avro records of other versions

Reading converts each record from the schema it was written with to your
codec's schema, as Avro's schema resolution does:

- a field the record lacks takes its default;
- a field the codec lacks is skipped;
- numbers are widened, as an `int` read as a `long`.

A record that cannot be resolved this way fails with
`RegistryException.Unserializable`.

### Avro values known only at run time

Where a program does not know the schema until it runs, such as a tool that
copies or shows any topic's records, it works with `AvroValue`s, which describe
any Avro value, and `AvroSchema`s.

`AvroSerde.asWritten` reads each record as it was written, together with the
schema its writer used:

```scala mdoc:compile-only
// Read any record of any schema, and show its schema and value
import cats.effect.IO

import xkafka.{Deserializer, Headers, Topic}
import xkafka.avro.{AvroSchemaJson, AvroSerde, AvroWritten}
import xkafka.schemaregistry.SchemaRegistry

def show(registry: SchemaRegistry[IO], topic: Topic, bytes: fs2.Chunk[Byte]): IO[String] =
  AvroSerde.asWritten[IO](registry).flatMap: (reading: Deserializer[IO, AvroWritten]) =>
    reading.deserialize(topic, Headers.empty, Some(bytes)).map: written =>
      s"${AvroSchemaJson.render(written.schema)}: ${written.value}"
```

`AvroSerde.generic` writes values of a schema you hold, and reads any record
converted to that schema, as a derived serde does:

```scala mdoc:compile-only
// Write a line of an order, with a schema read from its JSON as the program runs
import cats.effect.IO

import xkafka.Serde
import xkafka.avro.{AvroSchemaJson, AvroSerde, AvroValue, FullName}
import xkafka.schemaregistry.{Part, SchemaRegistry}

val text = """{"type":"record","name":"Line","namespace":"com.example","fields":[{"name":"sku","type":"string"},{"name":"quantity","type":"int"}]}"""

def lines(registry: SchemaRegistry[IO]): IO[Serde[IO, AvroValue]] =
  IO.fromEither(AvroSchemaJson.parse(text).left.map(new IllegalArgumentException(_))).flatMap: schema =>
    AvroSerde.generic[IO](registry, schema, Part.Value)

val line = AvroValue.Record(FullName(Some("com.example"), "Line"), Vector("sku" -> AvroValue.String("apple"), "quantity" -> AvroValue.Int(3)))
```

A value that does not fit the schema fails to write with
`RegistryException.Unserializable`. A value of a logical type is its underlying
value: a `date` is the `AvroValue.Int` of its days since 1970-01-01.

## JSON Schema

Derive a codec for your type, and make a serde from it:

```scala mdoc:compile-only
// Write orders as JSON, registering their JSON Schema under "orders-value" on the first write
import java.time.Instant

import cats.effect.IO

import xkafka.Serde
import xkafka.jsonschema.{JsonSchemaCodec, JsonSchemaSerde}
import xkafka.schemaregistry.{Part, SchemaRegistry}

final case class Line(sku: String, quantity: Int = 1) derives JsonSchemaCodec
final case class Order(id: String, placedAt: Instant, lines: List[Line], note: Option[String] = None) derives JsonSchemaCodec

def orders(registry: SchemaRegistry[IO]): IO[Serde[IO, Order]] = JsonSchemaSerde.of[IO, Order](registry, Part.Value)
```

A record is the schema's id followed by the JSON as UTF-8 text, as Confluent's
`KafkaJsonSchemaSerializer` writes it.

### Derived JSON schemas

`derives JsonSchemaCodec` makes a JSON Schema of draft 7 from the type:

| Scala | JSON Schema | Written as |
| --- | --- | --- |
| `Boolean`, `String` | `boolean`, `string` | as they are |
| `Int`, `Long` | `integer` | numbers |
| `Float`, `Double`, `BigDecimal` | `number` | numbers; infinities and NaN are refused |
| `fs2.Chunk[Byte]` | `string` with `contentEncoding` `base64` | base64 text |
| `Option[A]` | `oneOf` `null` and `A` | `null` where it is `None` |
| `List`, `Vector`, `Seq`, `Set` | `array` | arrays |
| `Map[String, A]` | `object` whose properties are all `A` | objects |
| `java.util.UUID` | `string` with `format` `uuid` | text |
| `java.time.Instant` | `string` with `format` `date-time` | ISO-8601 text, such as `2026-11-10T07:00:00Z` |
| `java.time.LocalDate` | `string` with `format` `date` | ISO-8601 text, such as `2026-11-10` |
| `java.time.LocalTime`, `LocalDateTime` | `string` | ISO-8601 text, such as `07:00:00` |
| a case class | `object` of its fields, closed to others | objects |
| an enum or sealed trait whose cases are all objects | `string` with `enum` of the cases' names | the case's name |
| any other sealed trait or enum | `oneOf` its cases' objects | the case's object, with its name in the property `type` |

- An object's `title` is the type's full name, in the namespace of its package,
  and naming subjects by record uses it, as Confluent's tools do.
- A field with a default, or an optional one, may be left out of a record; the
  others are required.
- Objects are closed: their schema allows no other properties. Under the
  registry's default compatibility rules, that lets you add a field with a
  default, or an optional one, as a new version of the schema, and refuses to
  let you remove a field.
- A case of a sealed trait must not have a field named `type`: writing it fails.

For a schema written by hand, give its JSON with circe's codecs for its values,
which must write what the schema describes:

```scala mdoc:compile-only
// A temperature written as a bare number, with a schema written by hand
import io.circe.{Decoder, Encoder, Json}

import xkafka.jsonschema.JsonSchemaCodec

final case class Celsius(degrees: Double)

given JsonSchemaCodec[Celsius] =
  JsonSchemaCodec.from(Json.obj("type" -> Json.fromString("number")), Encoder[Double].contramap(_.degrees), Decoder[Double].map(Celsius(_)))
```

For a type of your own, map it to one that has a codec with `imap` or `emap`,
as for Avro.

### JSON records of other versions

Reading takes a record's JSON as it is, whatever version of the schema it was
written with:

- a field the record lacks takes its default, or `None` where it is optional;
- a field is also read under any name `@SchemaAlias` gives it;
- properties your type does not have are ignored.

Values are not checked against the schema when they are written or read, as
Confluent's serializers do not check them by default.

### JSON values known only at run time

`JsonSchemaSerde.generic` writes circe `Json` values of a schema you hold, as
they are, and reads each record's JSON as it is:

```scala mdoc:compile-only
// Write a line of an order as JSON, with a schema read from its text as the program runs
import cats.effect.IO
import io.circe.{Json, parser}

import xkafka.Serde
import xkafka.jsonschema.JsonSchemaSerde
import xkafka.schemaregistry.{Part, SchemaRegistry}

val text = """{"type":"object","title":"com.example.Line","properties":{"sku":{"type":"string"},"quantity":{"type":"integer"}}}"""

def lines(registry: SchemaRegistry[IO]): IO[Serde[IO, Json]] =
  IO.fromEither(parser.parse(text)).flatMap(schema => JsonSchemaSerde.generic[IO](registry, schema, Part.Value))

val line = Json.obj("sku" -> Json.fromString("apple"), "quantity" -> Json.fromInt(3))
```

The schema's `title`, where it has one, is its name for naming subjects by
record. `JsonSchemaSerde.asWritten` reads each record as a `JsonSchemaWritten`:
its JSON, with the schema it was written with.

## Protobuf

Derive a codec for your type, numbering every field with `@ProtobufField`, and
make a serde from it:

```scala mdoc:compile-only
// Write orders as Protobuf messages, registering their .proto file under "orders-value" on the first write
import java.time.Instant

import cats.effect.IO

import xkafka.Serde
import xkafka.protobuf.{ProtobufCodec, ProtobufField, ProtobufSerde}
import xkafka.schemaregistry.{Part, SchemaNamespace, SchemaRegistry}

final case class Line(@ProtobufField(1) sku: String, @ProtobufField(2) quantity: Int = 1) derives ProtobufCodec

@SchemaNamespace("com.example")
final case class Order(
    @ProtobufField(1) id: String,
    @ProtobufField(2) placedAt: Instant,
    @ProtobufField(3) lines: List[Line],
    @ProtobufField(4) note: Option[String] = None
) derives ProtobufCodec

def orders(registry: SchemaRegistry[IO]): IO[Serde[IO, Order]] = ProtobufSerde.of[IO, Order](registry, Part.Value)
```

The schema registered for `Order` is this .proto file, which
`ProtobufCodec[Order].schema` also gives:

```proto
syntax = "proto3";

package com.example;

import "google/protobuf/timestamp.proto";

message Order {
  string id = 1;
  google.protobuf.Timestamp placedAt = 2;
  repeated Line lines = 3;
  optional string note = 4;
}

message Line {
  string sku = 1;
  optional int32 quantity = 2;
}
```

Records are written as Confluent's `KafkaProtobufSerializer` writes them. A
Protobuf record is a message, so the type you write must be a case class or a
sealed trait; for any other type, `ProtobufSerde.of` fails with
`RegistryException.NoSchema`.

### Field numbers

Records carry each field's number, not its name, so the numbers are what must
stay the same as your type changes:

- every field needs a number. A field without one, two fields sharing one, or a
  number Protobuf does not allow (outside 1 to 536870911, or from 19000 to
  19999) fails to compile;
- renaming a field, or moving it, changes nothing a reader sees;
- once a field is removed, do not give its number to another field.

### Derived Protobuf schemas

`derives ProtobufCodec` declares, in proto3:

| Scala | Protobuf |
| --- | --- |
| `Boolean`, `Int`, `Long`, `Float`, `Double`, `String` | `bool`, `int32`, `int64`, `float`, `double`, `string` |
| `fs2.Chunk[Byte]` | `bytes` |
| `Option[A]` | an `optional` field; a field of a message, which records whether it is set, needs no label |
| `List`, `Vector`, `Seq`, `Set` | a `repeated` field, packed where it holds numbers |
| `Map[String, A]` | `map<string, A>` |
| `java.util.UUID`, `BigDecimal` | `string`, holding the value as text |
| `java.time.Instant` | `google.protobuf.Timestamp` |
| `java.time.LocalDate`, `LocalTime`, `LocalDateTime` | `string`, holding ISO-8601 text, such as `2026-11-10` |
| a case class | a `message` of its fields |
| an enum or sealed trait whose cases are all objects | an `enum`, whose values carry its name and theirs, as `STATUS_OPEN` |
| any other sealed trait or enum | a `message` whose one field, of a `oneof`, is the case's message |

- Every message and enum is declared in the one file, in the package of the
  message you write, by its simple name. Two types with the same name in
  different namespaces cannot share the file.
- Number each case of a sealed trait or enum with `@ProtobufField` on the case,
  as on a field. An enum needs a case numbered 0, or it fails to compile: a field
  of it that a record lacks reads as that case.
- A field with a Scala default is labelled `optional` and written even where it
  is zero, so a record that lacks it reads the default. Other fields are written
  as proto3 writes them: left out where they are zero (`0`, `""`, `false`, an
  empty list, an enum's case numbered 0), and read as zero where a record lacks
  them. Lists, maps and options do not use a default: a record that lacks one
  reads it as empty or `None`.
- Protobuf has no field for a list of lists, a list or map of options, or an
  option of a list. `ProtobufCodec[A].schema` says which field of `A` holds one,
  and `ProtobufSerde.of` fails with `RegistryException.NoSchema`.

`ProtobufCodec.sint32` and `ProtobufCodec.sint64` write numbers near zero,
negative or not, in fewer bytes than `int32` and `int64`, which take ten bytes for
any negative number. `fixed32`, `fixed64`, `sfixed32` and `sfixed64` write every
number in four or eight bytes. Make one the given codec to use it:

```scala mdoc:compile-only
// Write a change of stock, often negative, as a sint32
import xkafka.protobuf.{ProtobufCodec, ProtobufField}

given ProtobufCodec[Int] = ProtobufCodec.sint32

final case class StockChange(@ProtobufField(1) sku: String, @ProtobufField(2) change: Int) derives ProtobufCodec
```

For a type of your own, map it to one that has a codec with `imap` or `emap`,
as for Avro.

### Protobuf records of other versions

Reading takes a record's fields by number, whatever version of the schema it
was written with:

- a field the record lacks takes its default, or its zero where it has none;
- fields your type does not have are skipped;
- a record whose enum field holds a number your enum lacks fails to read with
  `RegistryException.Unserializable`.

Under the registry's default compatibility rules, adding a field is a
compatible change, and changing a field's type is not.

### Protobuf values known only at run time

Where a program does not know the schema until it runs, such as a tool that
copies or shows any topic's records, it works with `ProtobufValue`s, which
describe any Protobuf value, and `ProtobufSchema.File`s, which describe .proto
files.

`ProtobufSerde.asWritten` reads each record as it was written, together with
the .proto file its writer registered. Where that file imports files registered
under other subjects, they are fetched from the registry. Protobuf's well-known
types, such as `google/protobuf/timestamp.proto`, need no fetching.

```scala mdoc:compile-only
// Read any Protobuf record of any schema, and show its message and one of its fields
import cats.effect.IO

import xkafka.{Headers, Topic}
import xkafka.protobuf.ProtobufSerde
import xkafka.schemaregistry.SchemaRegistry

def show(registry: SchemaRegistry[IO], topic: Topic, bytes: fs2.Chunk[Byte]): IO[String] =
  ProtobufSerde.asWritten[IO](registry).flatMap: reading =>
    reading.deserialize(topic, Headers.empty, Some(bytes)).map(written => s"${written.value.name}: ${written.value.field("id")}")
```

`ProtobufSerde.generic` writes messages of a .proto file you hold, which
`ProtobufSchemaText.parse` reads from its text, and reads records by field
number, as a derived serde does:

```scala mdoc:compile-only
// Write a line of an order, with a .proto file read as the program runs
import cats.effect.IO

import xkafka.Serde
import xkafka.protobuf.{ProtobufSchemaText, ProtobufSerde, ProtobufValue}
import xkafka.schemaregistry.{Part, SchemaRegistry}

val text = "syntax = \"proto3\"; package com.example; message Line { string sku = 1; int32 quantity = 2; }"

def lines(registry: SchemaRegistry[IO]): IO[Serde[IO, ProtobufValue]] =
  IO.fromEither(ProtobufSchemaText.parse(text).left.map(new IllegalArgumentException(_))).flatMap: file =>
    ProtobufSerde.generic[IO](registry, file, "com.example.Line", Part.Value)

val line = ProtobufValue.Message("com.example.Line", Vector("sku" -> ProtobufValue.String("apple"), "quantity" -> ProtobufValue.Int32(3)))
```

The file may import only the well-known types, and must declare the message you
name; otherwise `ProtobufSerde.generic` fails with `RegistryException.NoSchema`.
`ProtobufSchemaText.render` turns a `ProtobufSchema.File` back into text.

A message is named by its full name, such as `com.example.Line`, and holds its
fields by name, in the order the file declares them; `field("sku")` gives one.
When a record is read:

- a proto3 field without a label, such as `int32 quantity = 2;`, that the record
  lacks reads as its zero value, and a list or map as empty;
- any other field the record lacks, such as one labelled `optional`, a message,
  or a field of a `oneof`, is left out of the message;
- fields whose numbers the file does not declare are kept in the message's
  `unknown` fields, and written back as they were, so copying a record loses
  nothing.

Each number field holds the value its type calls for:

| Protobuf | `ProtobufValue` |
| --- | --- |
| `int32`, `sint32`, `sfixed32` | `Int32` |
| `int64`, `sint64`, `sfixed64` | `Int64` |
| `uint32`, `fixed32` | `UInt32`, a `Long` from 0 to 4294967295 |
| `uint64`, `fixed64` | `UInt64`, a `BigInt` from 0 to 2^64 - 1 |

An enum's value is `Enum("STATUS_PAID")`, by name, or `EnumNumber(7)` where a
record holds a number the enum has no name for, as one written by a newer
version of the schema.

Writing fails with `RegistryException.Unserializable` where a value does not fit
its message: it has a field the message lacks, a value of the wrong type, two
fields of one `oneof`, an unsigned number out of range, or lacks a proto2
`required` field.

`ProtobufSchemaText.parse` reads proto2 and proto3 files. It ignores services
and options, except a field's `packed` and `default`, and refuses files that
declare groups or extensions, or are written in an edition of Protobuf.

## Names and documentation

These annotations, in `xkafka.schemaregistry`, change what a derived schema
calls things, for every format:

| Annotation | On | Effect |
| --- | --- | --- |
| `@SchemaName("name")` | a type or field | names it in place of its Scala name |
| `@SchemaNamespace("com.example")` | a type | puts it in this namespace in place of its package's |
| `@SchemaDoc("text")` | a type or field | documents it |
| `@SchemaAlias("Old")` | a type or field | another name it was known by, so records written under that name are read |
| `@SchemaNoDefault` | a field | leaves its Scala default out of the schema |

```scala mdoc:compile-only
// An order renamed from Purchase, in the namespace com.example, with a field renamed for the schema
import java.time.Instant

import xkafka.avro.AvroCodec
import xkafka.schemaregistry.{SchemaAlias, SchemaDoc, SchemaName, SchemaNamespace}

@SchemaNamespace("com.example")
@SchemaAlias("Purchase")
@SchemaDoc("An order a customer placed")
final case class Order(id: String, @SchemaName("placed_at") placedAt: Instant) derives AvroCodec
```

In JSON Schema, a type's name and namespace make its `title`, and a type's
aliases are not used: JSON Schema has no way to record them. A field's aliases
are read as other names for the field.

In Protobuf, documentation becomes comments in the .proto file, and the
namespace of the message you write is the file's package. Aliases are not used:
records name fields by number, so a renamed field or type needs no alias.

## Subjects

A schema is kept under a subject, named as Confluent's subject name strategies
name it:

| `SubjectNaming` | Subject | Confluent's strategy |
| --- | --- | --- |
| `TopicName` (default) | `orders-value` or `orders-key` | `TopicNameStrategy` |
| `RecordName` | `com.example.Order`, the record's full name, the JSON schema's `title` or the Protobuf message's full name | `RecordNameStrategy` |
| `TopicRecordName` | `orders-com.example.Order` | `TopicRecordNameStrategy` |

`RecordName` and `TopicRecordName` need a named record: writing a type that has
none, such as a `String` in Avro, or a JSON schema without a `title`, fails with
`RegistryException.Unserializable`.

## Which schema is written

`of` and `generic` take a `Writing`:

- `Register` (default): writes with your codec's schema, registering it under
  the subject on the first write if it is not there yet, as Confluent's
  `auto.register.schemas` does. Use it when your application owns the schema. If
  the subject's compatibility rule refuses the schema, writing fails with
  `RegistryException.Refused`, with status 409 and error code 40901.
- `AlreadyRegistered`: writes with your codec's schema, which must already be
  registered. Use it when schemas are registered ahead of time, for example by a
  deployment pipeline, and applications must not register them. Writing fails
  with `RegistryException.Unserializable` where the schema is not registered.
- `Latest`: writes with the subject's latest version, as Confluent's
  `use.latest.version` does. Use it when another team owns the schema. With
  Avro, fields the latest version lacks are left out, and those it adds take
  their defaults; JSON is written as your codec writes it. A Protobuf message
  is written as your codec writes it, as the message of the latest version's
  file with your type's name; a file without one is refused. Writing fails with
  `RegistryException.Unserializable` where the subject has no versions.
- `Pinned(id)`: writes with the schema registered as `id`.

## Registry operations

`SchemaRegistry` also offers the registry's operations directly: `schema(id)`,
`register`, `lookup`, `latest`, `version`, and `compatibility`, which tells
whether a schema could be registered as a subject's next version, with the
registry's reasons where it could not.

`lookup`, `latest` and `version` answer `None` where the registry does not have
the subject, the version, or the schema under that subject. `schema(id)` of an
id the registry does not have fails with `RegistryException.Refused`.

## Errors

Failures are `RegistryException`s:

| Exception | When |
| --- | --- |
| `Unreachable` | none of the registry's URLs could be reached |
| `Refused` | the registry refused a request; `code` is its error code, such as 40901 for an incompatible schema |
| `Unreadable` | the registry's answer could not be read, as one from a proxy in front of it |
| `Unserializable` | a value could not be written, or a record read, with its schema |
| `NoSchema` | your type has no schema in the serde's format, as a list of lists has none in Protobuf |

Settings the registry client refuses raise `InvalidRegistrySetting`, and a
record without a value raises `Deserializer.NullValueException` (see
[Writing and reading records](#writing-and-reading-records)).

## Not yet supported

- Avro and JSON Schema schemas that name schemas registered under other subjects
  (schema references): records written with them are refused when reading, and
  `Latest` or `Pinned` cannot write with them. Protobuf records with such
  schemas are read: derived serdes read by field number and do not need the
  other schemas, and `ProtobufSerde.asWritten` fetches them.
- Generic Protobuf serdes of files that import files other than the
  well-known types.
- Protobuf files that declare groups or extensions, or are written in an edition
  of Protobuf.
