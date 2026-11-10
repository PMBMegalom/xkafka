# Schema registry

xkafka can write records with schemas kept in a Confluent Schema Registry, and
read them back, on the JVM, Scala.js and Scala Native. It supports Avro and JSON
Schema. Records are written in Confluent's wire format, so Java applications
using Confluent's serializers and deserializers read what xkafka writes, and
xkafka reads what they write.

## Dependencies

```scala
// Avro serdes, the registry client they use, and http4s's Ember client to reach the registry
libraryDependencies ++= Seq(
  "io.github.pmbmegalom" %%% "xkafka-avro"                   % "@VERSION@",
  "io.github.pmbmegalom" %%% "xkafka-schema-registry-http4s" % "@VERSION@",
  "org.http4s"           %%% "http4s-ember-client"           % "0.23.38"
)
```

- `xkafka-avro` gives you Avro serdes for your types, and `xkafka-json-schema`
  gives you JSON Schema serdes. Add the one you use, or both. Each brings in
  `xkafka-schema-registry`, the registry client.
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

To authenticate, add the credentials to every request the client sends:

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

- one or more registry URLs. A request that cannot reach one is sent to the next.
- `latestCachedFor`: how long a subject's latest version is cached. By default it
  is cached until the program ends, as Confluent's serializers cache it. Use
  `withLatestCachedFor(duration)` to see a new latest version within `duration`,
  and `withoutLatestExpiry` to go back to the default.

Everything else the registry answers never changes, so it is cached for good:
each schema is fetched once, however many records use it.

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

## Writing and reading Avro

Derive a codec for your type, and make a serde from it:

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

`AvroSerde.of` gives a `Serde`: a producer takes its `serializer`, a consumer
its `deserializer`, and a streams program the serde itself. Use `Part.Key` for
a serde of keys.

Each record names the schema it was written with. Reading fetches that schema
and converts the record to your codec's schema, as Avro's schema resolution
does, so records written with an older or newer version of your type are read:

- a field the record lacks takes its default;
- a field the codec lacks is skipped;
- numbers are widened, as an `int` read as a `long`.

A record that cannot be resolved this way fails with
`RegistryException.Unserializable`.

## Derived Avro schemas

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

A number with more digits, or more digits after the point, is refused rather
than rounded.

For a type of your own, map it to one that has a codec:

```scala mdoc:compile-only
// Write a customer id as the text it wraps
import xkafka.avro.AvroCodec

final case class CustomerId(value: String)

given AvroCodec[CustomerId] = AvroCodec[String].imap(CustomerId(_))(_.value)
```

`emap` does the same where reading can refuse a value.

## Avro values known only at run time

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

Derive a codec for your type, and make a serde from it, as for Avro:

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

Reading takes a record's JSON as it is, whatever version of the schema it was
written with:

- a field the record lacks takes its default, or `None` where it is optional;
- a field is also read under any name `@SchemaAlias` gives it;
- properties your type does not have are ignored.

Values are not checked against the schema when they are written or read, as
Confluent's serializers do not check them by default.

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

For values known only at run time, `JsonSchemaSerde.generic` writes circe
`Json` values of a schema you hold, and `JsonSchemaSerde.asWritten` reads each
record's JSON with the schema it was written with.

## Annotations

These change what a derived schema calls things, for Avro and JSON Schema alike.
They are in `xkafka.schemaregistry`:

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

## Subjects

A schema is kept under a subject, named as Confluent's subject name strategies
name it:

| `SubjectNaming` | Subject | Confluent's strategy |
| --- | --- | --- |
| `TopicName` (default) | `orders-value` or `orders-key` | `TopicNameStrategy` |
| `RecordName` | `com.example.Order`, the record's full name or the JSON schema's `title` | `RecordNameStrategy` |
| `TopicRecordName` | `orders-com.example.Order` | `TopicRecordNameStrategy` |

## Which schema is written

`AvroSerde.of` and `JsonSchemaSerde.of` take a `Writing`:

- `Register` (default): writes with your codec's schema, registering it under
  the subject on the first write if it is not there yet, as Confluent's
  `auto.register.schemas` does. Use it when your application owns the schema. If
  the subject's compatibility rule refuses the schema, writing fails with
  `RegistryException.Refused`, with status 409 and error code 40901.
- `AlreadyRegistered`: writes with your codec's schema, which must already be
  registered. Use it when schemas are registered ahead of time, for example by a
  deployment pipeline, and applications must not register them.
- `Latest`: writes with the subject's latest version, as Confluent's
  `use.latest.version` does. Use it when another team owns the schema. With
  Avro, fields the latest version lacks are left out, and those it adds take
  their defaults; JSON is written as your codec writes it.
- `Pinned(id)`: writes with the schema registered as `id`.

## Registry operations

`SchemaRegistry` also offers the registry's operations directly: `schema(id)`,
`register`, `lookup`, `latest`, `version`, and `compatibility`, which tells
whether a schema could be registered as a subject's next version, with the
registry's reasons where it could not.

A subject, version or schema the registry does not have is `None`, not an
error.

## Errors

Failures are `RegistryException`s:

| Exception | When |
| --- | --- |
| `Unreachable` | none of the registry's URLs could be reached |
| `Refused` | the registry refused a request; `code` is its error code, such as 40901 for an incompatible schema |
| `Unreadable` | the registry's answer could not be read, as one from a proxy in front of it |
| `Unserializable` | a value could not be written, or a record read, with its schema |

## Not yet supported

- Schemas that name schemas registered under other subjects (schema
  references) are refused when reading.
- Protobuf.
