# Using types from other libraries

If your types already come from another library, xkafka writes and reads them
with the schemas a Confluent Schema Registry keeps, as
[Schema registry](schema-registry.md) describes for derived codecs:

- `xkafka-avro-vulcan` and `xkafka-avro4s` write and read the types
  [vulcan](https://fd4s.github.io/vulcan/) and
  [avro4s](https://github.com/sksamuel/avro4s) have codecs for, as Avro, on the
  JVM only.
- `xkafka-avro` writes and reads the classes
  [avrohugger](https://github.com/julianpeeters/avrohugger) generates from
  Avro files.
- `xkafka-protobuf-scalapb` writes and reads the Protobuf messages
  [ScalaPB](https://scalapb.github.io) generates from .proto files.
- `xkafka-smithy4s` writes and reads the types
  [smithy4s](https://disneystreaming.github.io/smithy4s/) generates from Smithy
  files, as smithy4s's JSON or as Protobuf.

Each needs a `SchemaRegistry`, made as
[Connecting to the registry](schema-registry.md#connecting-to-the-registry)
shows.

## vulcan or avro4s codecs

If your types already have vulcan codecs or avro4s encoders and decoders, for
example because you are moving from fs2-kafka, use `xkafka-avro-vulcan` or
`xkafka-avro4s` to write and read them with the registry's schemas. Both
modules run on the JVM only, because vulcan and avro4s do.

```scala
// Add vulcan and avro4s serdes; each module is for the JVM only
libraryDependencies += "io.github.pmbmegalom" %% "xkafka-avro-vulcan" % "@VERSION@"
libraryDependencies += "io.github.pmbmegalom" %% "xkafka-avro4s"      % "@VERSION@"
```

```scala mdoc:reset
// Visits, written and read by vulcan's codec
import cats.effect.IO
import cats.syntax.all.*

import xkafka.Serde
import xkafka.avro.VulcanSerde
import xkafka.schemaregistry.{Part, SchemaRegistry}

final case class Visit(id: String, pages: Int = 1)

object Visit:
  given vulcan.Codec[Visit] =
    vulcan.Codec.record("Visit", "com.example.web"): field =>
      (field("id", _.id), field("pages", _.pages, default = Some(1))).mapN(Visit(_, _))

def visits(registry: SchemaRegistry[IO]): IO[Serde[IO, Visit]] = VulcanSerde.of[IO, Visit](registry, Part.Value)
```

```scala mdoc:reset
// Visits, written and read by the encoder and decoder avro4s derives
import cats.effect.IO
import com.sksamuel.avro4s.AvroNamespace

import xkafka.Serde
import xkafka.avro.Avro4sSerde
import xkafka.schemaregistry.{Part, SchemaRegistry}

@AvroNamespace("com.example.web")
final case class Visit(id: String, pages: Int = 1)

def visits(registry: SchemaRegistry[IO]): IO[Serde[IO, Visit]] = Avro4sSerde.of[IO, Visit](registry, Part.Value)
```

xkafka registers the library's schema for the type. With
`SubjectNaming.RecordName`, the subject is that schema's full name. Records are
the bytes Confluent's `KafkaAvroSerializer` writes for the library's values, so
Confluent's serializers and these serdes read each other's records.

Each record is read with the schema it was written with, so a type reads the
records of its older versions:

- a field a record lacks takes its default;
- an `int` is read as a `long`;
- a symbol the enum lacks is read as the enum's default. avro4s gives a Scala 3
  enum no default, so give such an enum a `SchemaFor` whose schema has one.

When a value is written with another schema than the library's, as with
`Writing.Latest`, it is converted to that schema by Avro's schema resolution: a
field the library's schema lacks takes its default.

A type the library gives no schema, such as one whose vulcan codec has a name
that is not a valid Avro name, fails to make a serde with
`RegistryException.NoSchema`.

## avrohugger classes

To generate classes from .avsc or .avdl files, xkafka's own generator is the
first choice: see [Generating classes from schema files](code-generation.md).
Its classes register exactly the schemas in your files, which avrohugger's do
not, as the list below shows.

If you already use [avrohugger](https://github.com/julianpeeters/avrohugger),
its sbt plugin can generate case classes that `AvroCodec.derived` writes and
reads, on the JVM, Scala.js and Scala Native. Generate its standard classes,
with enums as case objects and unions as union types:

```scala
// project/plugins.sbt
addSbtPlugin("com.julianpeeters" % "sbt-avrohugger" % "2.18.0")
```

```scala
// build.sbt: generate classes from src/main/avro whenever the project compiles
Compile / sourceGenerators += (Compile / avroScalaGenerate).taskValue
Compile / avroScalaCustomTypes := avrohugger.format.Standard.defaultTypes.copy(
  `enum` = avrohugger.types.ScalaCaseObjectEnum,
  union = avrohugger.types.OptionScala3UnionType
)
```

In a project built for several platforms, keep the files in the shared
directory and give avrohugger its canonical path, since avrohugger fails on
.avdl files under a path with `..` in it:

```scala
// build.sbt: read the files of the shared directory
Compile / avroSourceDirectories := Seq((baseDirectory.value / ".." / "shared" / "src" / "main" / "avro").getCanonicalFile)
```

Give each record you write or read its codec; the records and enums inside it
get theirs from it. Give each union of several types its codec too, with
`AvroCodec.union`, in the order of the file's union:

```scala
// Visits, a record avrohugger generated from visit.avsc, which may have come by ad or by link
import com.example.web.{Ad, Link, Visit}
import xkafka.avro.AvroCodec

given AvroCodec[Ad | Link] = AvroCodec.union[(Ad, Link)]
given AvroCodec[Visit]     = AvroCodec.derived
```

The schema derived for a class is the file's, without its `doc`s: avrohugger
turns them into comments, which a codec cannot see. Records, enums, primitive
types, `bytes`, unions, arrays, maps, defaults, and the logical types `uuid`,
`date` and `timestamp-millis` are derived as the file declares them. These are
not:

- `decimal`: avrohugger makes it a `BigDecimal` without its precision and
  scale, so it fails to compile until you give one with `AvroCodec.decimal`,
  which then serves every `BigDecimal`;
- `fixed`: avrohugger makes it a case class of an `Array[Byte]`, which is
  derived as a record, not as the file's `fixed`. Do not write such classes:
  their schema is not the file's;
- an enum's `default`: avrohugger's enums do not carry it, so the derived enum
  has none;
- a `bytes` default with a character past `\u007f`: avrohugger generates
  different bytes than the file declares, and the derived default is those.

Avoid avrohugger's `Scala3Enum` option for .avdl files: the classes it
generates for them do not compile.

## ScalaPB messages

If your .proto files come first and ScalaPB generates your messages from them,
`xkafka-protobuf-scalapb` writes and reads those messages:

```scala
// Add Protobuf serdes for the messages ScalaPB generates
libraryDependencies += "io.github.pmbmegalom" %%% "xkafka-protobuf-scalapb" % "@VERSION@"
```

```scala
// Orders, a message ScalaPB generated from shop.proto, written and read with the registry's schemas
import cats.effect.IO

import com.example.shop.shop.Order
import xkafka.Serde
import xkafka.protobuf.ScalaPBSerde
import xkafka.schemaregistry.{Part, SchemaRegistry}

def orders(registry: SchemaRegistry[IO]): IO[Serde[IO, Order]] = ScalaPBSerde.of[IO, Order](registry, Part.Value)
```

ScalaPB writes and reads each message. The schema registered for it is the
.proto file it was generated from: the same messages, fields and enums, without
the file's options. Records are compatible with Confluent's Protobuf
serializers and deserializers in both directions.

Records written with another version of the file are read by field number, as
ScalaPB reads them. With `Writing.Latest` or `Writing.Pinned`, the message is
found in that version of the file by its full name, such as
`com.example.shop.Order`. With `SubjectNaming.RecordName`, the subject is that
name too. A message declared inside another, such as `Order.Line`, is written
and read the same way.

The file may import Protobuf's well-known types, such as
`google/protobuf/timestamp.proto`, and `scalapb/scalapb.proto`, which holds
ScalaPB's options. `ScalaPBSerde.of` fails with `RegistryException.NoSchema` if
the file imports any other file, declares groups or extensions, or is written
in an edition of Protobuf.

`xkafka-protobuf-scalapb` brings in ScalaPB's runtime 0.11.21. Generate your
messages with the same version of ScalaPB's code generator, as
[ScalaPB's sbt setup](https://scalapb.github.io/docs/installation/) shows.

## smithy4s types as JSON

If your types come from Smithy files, `xkafka-smithy4s` writes and reads the
types smithy4s generates as smithy4s's JSON, with the registry's schemas, on
the JVM, Scala.js and Scala Native. It can also write them as Protobuf; see
[smithy4s types as Protobuf](#smithy4s-types-as-protobuf).

```scala
// Add JSON and Protobuf serdes for the types smithy4s generates
libraryDependencies += "io.github.pmbmegalom" %%% "xkafka-smithy4s" % "@VERSION@"
```

```scala
// Orders, a structure smithy4s generated from shop.smithy
import cats.effect.IO

import example.shop.Order
import xkafka.Serde
import xkafka.jsonschema.Smithy4sJsonSerde
import xkafka.schemaregistry.{Part, SchemaRegistry}

def orders(registry: SchemaRegistry[IO]): IO[Serde[IO, Order]] = Smithy4sJsonSerde.of[IO, Order](registry, Part.Value)
```

A record is the JSON smithy4s's own codecs write, byte for byte, so services
built with smithy4s read and write the same records. Reading takes the defaults
of the members a record lacks and ignores the properties it does not know, as
smithy4s does.

xkafka registers a draft 7 JSON Schema describing this JSON, built from the
type's smithy4s schema. Its title is the shape's full name, such as
`example.shop.Order`, which `SubjectNaming.RecordName` uses as the subject:

- A structure is an object of its members, named by `@jsonName` if they have
  one. Its `@required` members are required; the others may be left out or
  null, and carry their `@default`s.
- A union is an object of its one member, such as `{"card": {...}}`; with
  `@discriminated("kind")`, its member's object with `kind` naming the member;
  and with `@untagged`, any of its members' values.
- An enum is the text of its values, and an intEnum the numbers of its values.
- A list is an array, with unique items if it has `@uniqueItems`, and a map
  an object of its values.
- A timestamp is a number of seconds, or with `@timestampFormat` the text of a
  `date-time` or of an `http-date`. A blob is base64 text, and a document any
  value.
- `@length`, `@range` and `@pattern` are JSON Schema's `minLength` and
  `maxLength`, `minItems` and `maxItems`, `minProperties` and `maxProperties`,
  `minimum` and `maximum`, and `pattern`. smithy4s's `@documentation` is each
  schema's `description`.
- A shape that holds itself refers to its definition, or to the schema itself.

## smithy4s types as Protobuf

`xkafka-smithy4s` also writes and reads the types smithy4s generates as
Protobuf messages, byte for byte as `smithy4s-protobuf` writes them, with the
registry's schemas, on the JVM, Scala.js and Scala Native:

```scala
// Payments, a structure smithy4s generated from till.smithy
import cats.effect.IO

import example.till.Payment
import xkafka.Serde
import xkafka.protobuf.Smithy4sProtobufSerde
import xkafka.schemaregistry.{Part, SchemaRegistry}

def payments(registry: SchemaRegistry[IO]): IO[Serde[IO, Payment]] = Smithy4sProtobufSerde.of[IO, Payment](registry, Part.Value)
```

xkafka registers a proto3 .proto file, in the package of the shape's
namespace, declaring the messages smithy4s writes, as
[smithy-translate](https://github.com/disneystreaming/smithy-translate) turns
Smithy into Protobuf:

- A structure is a message of its members, numbered by their `@protoIndex`es,
  or else in their order from 1. A union is a message of one `oneof`, named
  `definition`, or with `@protoInlinedOneOf` the `oneof` itself, in the
  structure holding it. An enum is an enum, numbered by its values'
  `@protoIndex`es or in their order from 0.
- A list is a repeated field, and a map a map. A `Long` or `Integer` is an
  `int64` or `int32`, or by its `@protoNumType` a `sint`, `uint`, `fixed` or
  `sfixed` one; a `Byte` or `Short` an `int32`; a `BigDecimal`, `BigInteger`,
  `UUID`, date or time text; a blob `bytes`.
- A timestamp is a `google.protobuf.Timestamp`, a duration a
  `google.protobuf.Duration`, and a document a `google.protobuf.Value`; with
  `@protoWrapped`, a number, boolean, text or blob is its well-known wrapper,
  such as `google.protobuf.Int64Value`.

The file's first message is the type's. With `SubjectNaming.RecordName`, the
subject is that message's full name, such as `example.till.Payment`.

A type that is not a structure or union, or holds what Protobuf cannot declare,
fails to make a serde with `RegistryException.NoSchema`, saying why:

- a list of lists, maps or nulls, or a map of them;
- a union's member that is a list or map;
- an inlined union whose members take the numbers of its structure's fields;
- two shapes or enum values of one name.

xkafka does not support smithy4s's compact forms of UUIDs, dates, times and
timestamps, or `@protoWrapped` on other types, and fails the same way for them.
