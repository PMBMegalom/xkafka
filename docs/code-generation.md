# Generating classes from schema files

Where your schemas come first, as Avro or JSON Schema files that other
applications share, xkafka can generate Scala classes from them. The classes
register the schemas in your files, on the JVM, JavaScript and Native, and need
only `xkafka-avro` or `xkafka-json-schema` to compile.

## With sbt

Add the plugin, which works with sbt 1 and sbt 2, and the module of your
schemas' format:

```scala
// project/plugins.sbt
addSbtPlugin("io.github.pmbmegalom" % "sbt-xkafka-codegen" % "@VERSION@")
```

```scala
// build.sbt
libraryDependencies += "io.github.pmbmegalom" %%% "xkafka-avro" % "@VERSION@"
libraryDependencies += "io.github.pmbmegalom" %%% "xkafka-json-schema" % "@VERSION@"
```

Put your .avsc and .avdl files in `src/main/avro`, and your JSON Schema .json
files in `src/main/json-schema`. Each time the project compiles, the classes
are generated again if any file or setting changed. A schema the generator
refuses fails the build, saying why.

To read schemas from other directories, such as the shared directory of a
project built for several platforms, set `xkafkaAvroSources` and
`xkafkaJsonSchemaSources`:

```scala
Compile / xkafkaAvroSources       := Seq(baseDirectory.value / ".." / "shared" / "src" / "main" / "avro")
Compile / xkafkaJsonSchemaSources := Seq(baseDirectory.value / ".." / "shared" / "src" / "main" / "json-schema")
```

Two settings change the classes of JSON Schema files:

```scala
// The package of the classes of files whose titles name none
xkafkaJsonSchemaPackage := Some("com.example.events")
// JSON Schema's numbers as BigDecimal, which keeps every digit, rather than Double
xkafkaJsonSchemaBigDecimals := true
```

The plugin runs `xkafka-codegen` of its own version, in a JVM of its own, so
nothing the generator uses reaches your build's classpath.

## Without sbt

`xkafka-codegen` is also a program, whose jar names its main class,
`xkafka.codegen.Main`. Its first argument is the directory the classes are
written to; the others are .avsc, .avdl and .json files, or directories whose
such files, at any depth, are read. `--json-package` and `--json-big-decimals`
before them do what `xkafkaJsonSchemaPackage` and `xkafkaJsonSchemaBigDecimals`
do. Run it from any build tool that can run a program from a Maven artifact:

```
xkafka.codegen.Main --json-package com.example.events target/generated src/main/avro src/main/json-schema
```

## Avro

### What is generated

Each named type is a file of its own, in the package of its namespace:

| Avro | Scala |
| --- | --- |
| a `record` | a case class of its fields, with the file's defaults as Scala defaults |
| an `enum` | a Scala 3 enum, marked `@AvroEnumDefault` where the enum has a default |
| a `fixed` | a case class of its `bytes: fs2.Chunk[Byte]`, marked `@AvroFixed` |
| `boolean`, `int`, `long`, `float`, `double`, `string` | `Boolean`, `Int`, `Long`, `Float`, `Double`, `String` |
| `bytes` | `fs2.Chunk[Byte]` |
| an `array`, a `map` | `List[A]`, `Map[String, A]` |
| a union of `null` and one type | `Option[A]` |
| a union of several types | a union type, such as `Cash \| Card`, or an `Option` of one where the union has `null` |
| `decimal` | `BigDecimal`, marked `@SchemaDecimal` with its precision and scale |
| `uuid`, `date` | `java.util.UUID`, `java.time.LocalDate` |
| `time-millis`, `time-micros` | `java.time.LocalTime` |
| `timestamp-millis`, `timestamp-micros`, `timestamp-nanos` | `java.time.Instant` |
| `local-timestamp-millis`, `local-timestamp-micros` | `java.time.LocalDateTime` |

Documentation and aliases become `@SchemaDoc` and `@SchemaAlias`. A name that
is a Scala keyword, such as `type`, is written in backquotes. A field or symbol
named as a case class's or enum's own members are, such as `copy` or `values`,
takes an underscore in Scala, `copy_`, and keeps its name in the schema.

Each type's companion holds the schema it was generated from, `avroSchema`, and
its codec, which must have that schema: the codec is made with
`AvroCodec.matching`, so a serde of it fails with `RegistryException.NoSchema`
where it does not. Write and read the classes as any others:

```scala
// Visits, generated from visit.avsc
import cats.effect.IO

import example.web.Visit
import xkafka.Serde
import xkafka.avro.AvroSerde
import xkafka.schemaregistry.{Part, SchemaRegistry}

def visits(registry: SchemaRegistry[IO]): IO[Serde[IO, Visit]] = AvroSerde.of[IO, Visit](registry, Part.Value)
```

### What is refused

The generator refuses a schema it cannot write exactly, saying which type and
field, and why:

- a named type without a namespace, which its package is made from;
- properties of a schema or field other than the ones Avro defines, a field's
  sort order, a logical type xkafka does not know, and a `fixed` type's
  documentation, none of which xkafka's schemas keep;
- `local-timestamp-nanos`, and a `decimal` held in a `fixed` type, which xkafka
  has no codec for;
- a `decimal` in a union of several types;
- a `time-millis` or `time-micros` default that is no time of day, such as a
  negative one;
- `null` in a union anywhere but first, or right after the type of the field's
  default;
- a record with fields of one Scala type at two precisions, such as a
  `timestamp-millis` and a `timestamp-micros`, or with the same union in two
  orders;
- two files declaring one type differently.

## JSON Schema

Each file becomes a type: a case class where the file describes an object, an
enum where it is an enum of text, and a sealed trait where it is a `oneOf` or
`anyOf` of objects, whose cases are those objects. The types the file holds,
in its `definitions` or `$defs` and in its properties, are declared in the
companion of the type that holds them, such as `Order.Line`.

The companion holds the file's schema, `jsonSchema`, and the type's codec,
which registers that schema as it is, without spaces, with `$schema` first.
A file that names no draft is registered as draft 7, the draft Confluent's
serializers take by default. The schema's `title` names it, where subjects are
named by record. Write and read the classes as any others:

```scala
// Orders, generated from order.json
import cats.effect.IO

import com.example.events.Order
import xkafka.Serde
import xkafka.jsonschema.JsonSchemaSerde
import xkafka.schemaregistry.{Part, SchemaRegistry}

def orders(registry: SchemaRegistry[IO]): IO[Serde[IO, Order]] = JsonSchemaSerde.of[IO, Order](registry, Part.Value)
```

### Names

A file's type takes its package and name from its `title`, where that is a
full Scala name such as `com.example.events.Order`. Otherwise it is in the
package `xkafkaJsonSchemaPackage` sets, and is named by its title where that is
a Scala name, or else by its file's name: `line-item.json` and
`line-item.schema.json` make `LineItem`.

The types inside a file are named after where they are:

| Where | Name |
| --- | --- |
| `definitions` or `$defs` | the definition's name, `line` making `Line` |
| a property | the property's name, `shipping` making `Shipping` |
| an array's items | the property's name and `Item`, such as `LinesItem` |
| the values of an object of any properties | the property's name and `Value` |
| a branch of a `oneOf` or `anyOf` | its title, where that is a Scala name, or the property's name and the branch's place, such as `Payment1` |

A property's field keeps the property's name where that is a Scala name, and
otherwise takes its letters and digits: `first-name` makes `firstName`. An
enum's values are named the same way, `in-progress` making `InProgress`. A
name that is a Scala keyword, such as `type`, is written in backquotes, and one
of a case class's or enum's own members, such as `copy` or `values`, takes an
underscore. Each keeps its name in JSON.

### What is generated

| JSON Schema | Scala |
| --- | --- |
| an object with `properties` | a case class |
| an `enum` of text | a Scala 3 enum |
| `boolean`, `integer`, `string` | `Boolean`, `Long`, `String` |
| `number` | `Double`, or `BigDecimal` where `xkafkaJsonSchemaBigDecimals` is set |
| a `string` of the `format` `date-time`, `date`, `time` or `uuid` | `java.time.Instant`, `java.time.LocalDate`, `java.time.OffsetTime`, `java.util.UUID` |
| a `string` of the `contentEncoding` `base64` | `fs2.Chunk[Byte]` |
| an `array` | `List[A]` |
| an `object` without `properties`, of `additionalProperties` of one schema | `Map[String, A]` |
| an `object` without `properties` or a schema for them | `Map[String, io.circe.Json]` |
| `{}`, `true`, or a schema of no type | `io.circe.Json` |
| a type that may be `null`, as `["string", "null"]` or a `oneOf` with `{"type": "null"}` | `Option[A]` |
| a `oneOf` or `anyOf` of several types, or several types, as `["string", "integer"]` | a union type, such as `Cash \| Card` |
| a `$ref` to a definition of the same file, or `#`, the file itself | the definition's type, or the file's |

Each property is a field, read and written as the file has it:

- A required property is always written, and must be there when read; one that
  may be `null` is an `Option`, whose `None` is written as `null`.
- A property that is not required, with a `default`, takes the default as its
  Scala default, and where it is left out when read.
- A property that is not required, without a default, is an `Option`, `None`
  by default: `None` leaves it out, and it is read as `None` where it is left
  out or `null`.
- A required property whose value is one text, a `const` or an `enum` of one
  value, is not a field: it is always written, and must have that value when
  read, so it tells a `oneOf`'s branches apart.

A `description` becomes the documentation of its type or field. Keywords that
only check values, such as `minimum`, `pattern` or a `format` other than those
above, are kept in the schema the type registers, and the classes do not check
them: neither writing nor reading checks values against the schema.

### Unions

A value of a union is written as its branch writes it. Reading tries each
branch twice. First each branch reads the value refusing properties its schema
does not name; where exactly one reads it, the value is of that branch. Where
none does, as where a writer added a property, each reads it again allowing
them, and where exactly one reads it, the value is of that branch.

Where several branches read a value, a `oneOf` fails, since JSON Schema has the
value of a `oneOf` be of exactly one branch, and an `anyOf` takes the first.
Take these payments:

```json
{
  "oneOf": [
    { "title": "Cash", "type": "object",
      "properties": { "amount": { "type": "number" } }, "required": ["amount"] },
    { "title": "Card", "type": "object",
      "properties": { "amount": { "type": "number" }, "last4": { "type": "string" } }, "required": ["amount"] }
  ]
}
```

`{"amount": 10, "last4": "4242"}` is a `Card`, the only branch that names
`last4`. `{"amount": 10}` fails: `Card` reads it as well as `Cash`, since its
`last4` is not required. JSON Schema refuses that value too, as Confluent's
serializers do where they check values against the schema, and with `Cash`
open to other properties it refuses even the card. Branches that a required
constant tells apart, such as `"kind": {"const": "cash"}`, are read without
fail. Numbers are values of every branch of a number type: `5` is both
an `integer` and a `number`, so a `oneOf` of the two fails on it, and an
`anyOf` reads it as the first.

### What is refused

The generator refuses a schema it cannot make types of, saying where in which
file, and why:

- a file whose title names no package, where `xkafkaJsonSchemaPackage` sets
  none, and two files of one type;
- a file that is not an object, an enum of text, or a `oneOf` or `anyOf` of
  objects, including one that may be `null`;
- `allOf`, `if`, `then` and `else`, `dependentSchemas` and `dependencies` of
  schemas, `patternProperties`, `prefixItems`, `items` that are a list of
  schemas, `$dynamicRef` and `$recursiveRef`, which combine schemas in ways
  types cannot hold;
- a `$ref` to another file, which xkafka does not read yet, and a `$ref` beside
  keywords that give its value a type;
- an object of both `properties` and a schema for other properties;
- a `const` that is not text, or not of a required property, and an `enum` of
  values other than text without a `type`;
- a union with two branches of one Scala class, such as two kinds of text or
  two arrays, with a branch of any value, or with a union inside it, whose
  values written could not be told apart; and a type holding one union both as
  a `oneOf` and as an `anyOf`;
- a definition that holds itself without a type of its own, such as an array
  of itself;
- two types of one name in one companion, or a type named as its file's;
- a `default` that is not a value of its property.

Whatever the format, two types whose classes would be one file are refused too:
an Avro and a JSON Schema type of one full name, or two whose names differ only
in case, which macOS and Windows treat as one file name.
