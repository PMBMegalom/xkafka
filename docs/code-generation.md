# Generating classes from schema files

If your schemas come first, as Avro or JSON Schema files shared with other
applications, xkafka can generate Scala classes from them. The classes register
the schemas in your files. They work on the JVM, Scala.js and Scala Native, and
need only `xkafka-avro` or `xkafka-json-schema` to compile.

## With sbt

Add the plugin (it works with sbt 1 and sbt 2) and the module for your schema
format:

```scala
// project/plugins.sbt
addSbtPlugin("io.github.pmbmegalom" % "sbt-xkafka-codegen" % "@VERSION@")
```

```scala
// build.sbt: add the module for your format, or both
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
// Read schemas from the shared directory of a cross-built project
Compile / xkafkaAvroSources       := Seq(baseDirectory.value / ".." / "shared" / "src" / "main" / "avro")
Compile / xkafkaJsonSchemaSources := Seq(baseDirectory.value / ".." / "shared" / "src" / "main" / "json-schema")
```

Two settings change the classes of JSON Schema files:

```scala
// The package of the classes of files whose titles name none
xkafkaJsonSchemaPackage := Some("com.example.events")
// JSON Schema's numbers as BigDecimal, which keeps every digit, in place of Double
xkafkaJsonSchemaBigDecimals := true
```

The generator runs in a JVM of its own, so its dependencies never reach your
build's classpath.

## Without sbt

The generator is also a command-line program, published as
`io.github.pmbmegalom:xkafka-codegen_3:@VERSION@`, with the main class
`xkafka.codegen.Main`. Run it from any tool that can run a program from a Maven
artifact and its dependencies:

- the first argument is the directory the classes are written to;
- the others are .avsc, .avdl and .json files, or directories to search at any
  depth;
- `--json-package <package>` and `--json-big-decimals`, given first, do what
  `xkafkaJsonSchemaPackage` and `xkafkaJsonSchemaBigDecimals` do.

```
# The arguments of xkafka.codegen.Main: options, the output directory, then the schemas
--json-package com.example.events target/generated src/main/avro src/main/json-schema
```

## Avro

### What is generated

Each named type is a file of its own, in the package of its namespace:

| Avro | Scala |
| --- | --- |
| a `record` | a case class of its fields, with the file's defaults as Scala defaults |
| an `enum` | a Scala 3 enum, marked `@AvroEnumDefault` if the enum has a default |
| a `fixed` | a case class of its `bytes: fs2.Chunk[Byte]`, marked `@AvroFixed` |
| `boolean`, `int`, `long`, `float`, `double`, `string` | `Boolean`, `Int`, `Long`, `Float`, `Double`, `String` |
| `bytes` | `fs2.Chunk[Byte]` |
| an `array`, a `map` | `List[A]`, `Map[String, A]` |
| a union of `null` and one type | `Option[A]` |
| a union of several types | a union type, such as `Cash \| Card`, or an `Option` of one if the union has `null` |
| `decimal` | `BigDecimal`, marked `@SchemaDecimal` with its precision and scale |
| `uuid`, `date` | `java.util.UUID`, `java.time.LocalDate` |
| `time-millis`, `time-micros` | `java.time.LocalTime` |
| `timestamp-millis`, `timestamp-micros`, `timestamp-nanos` | `java.time.Instant` |
| `local-timestamp-millis`, `local-timestamp-micros` | `java.time.LocalDateTime` |

Documentation and aliases become `@SchemaDoc` and `@SchemaAlias`. A name that
is a Scala keyword, such as `type`, is written in backquotes. Some names get a
trailing underscore in Scala, such as `copy_`, and keep their name in the
schema:

- a field or symbol named like a member every case class or enum has, such as
  `copy` or `values`;
- a symbol named `avroSchema`, the name of the companion's schema.

Each type's companion object holds `avroSchema`, the schema it was generated
from, and a codec checked against that schema with `matching`. If the codec's
schema ever differs from it, making a serde fails with
`RegistryException.NoSchema`. Write and read the classes like any
others:

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
- a default holding a value of a union's branch other than its first, in the
  field's union or in one inside it, such as `["x"]` for a list of
  `["null", "string"]`, since xkafka writes a union's default in its first
  branch;
- `null` in a union anywhere but first, or right after the type of the field's
  default, and in a union inside a list or a map anywhere but first;
- a record with fields of one Scala type at two precisions, such as a
  `timestamp-millis` and a `timestamp-micros`, or with the same union in two
  orders;
- two files declaring one type differently.

## JSON Schema

Each file becomes a type: a case class if the file describes an object, an
enum if it is an enum of text, and a sealed trait if it is a `oneOf` or
`anyOf` of objects, whose cases are those objects. The types the file holds,
in its `definitions` or `$defs` and in its properties, are declared in the
companion of the type that holds them, such as `Order.Line`.

The companion object holds `jsonSchema`, the file's schema, and the type's
codec. The codec registers the file's schema unchanged apart from whitespace,
with `$schema` first. A file that names no draft is registered as draft 7, the
draft Confluent's serializers take by default. With `SubjectNaming.RecordName`,
the subject is the schema's `title`. A file's type whose schema names its own
definitions by reference cannot be a field of a derived type, or held in an
`Option`, a collection or a map: those references would name the definitions of
the other schema, so a serde of that type fails with
`RegistryException.NoSchema`. Write and read the classes like any others:

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

If a file's `title` is a full Scala name, such as `com.example.events.Order`,
the type takes its package and name from it. Otherwise the type goes in the
package `xkafkaJsonSchemaPackage` sets. It is named by its title if that is a
Scala name, or else by its file's name: `line-item.json` and
`line-item.schema.json` make `LineItem`.

The types inside a file are named after where they are:

| Where | Name |
| --- | --- |
| `definitions` or `$defs` | the definition's name, `line` making `Line` |
| a property | the property's name, `shipping` making `Shipping` |
| an array's items | the property's name and `Item`, such as `LinesItem` |
| the values of an object of any properties | the property's name and `Value` |
| a branch of a `oneOf` or `anyOf` | its title, if that is a Scala name, or else the property's name and the branch's place, such as `Payment1` |

A property's field keeps the property's name if that is a Scala name, and
otherwise takes its letters and digits: `first-name` makes `firstName`. An
enum's values are named the same way, `in-progress` making `InProgress`. A
name that is a Scala keyword, such as `type`, is written in backquotes. A name
of a member every case class or enum has, such as `copy` or `values`, gets a
trailing underscore. Each keeps its name in JSON.

### What is generated

| JSON Schema | Scala |
| --- | --- |
| an object with `properties` | a case class |
| an `enum` of text | a Scala 3 enum |
| `boolean`, `integer`, `string` | `Boolean`, `Long`, `String` |
| `number` | `Double`, or `BigDecimal` if `xkafkaJsonSchemaBigDecimals` is set |
| a `string` of the `format` `date-time`, `date`, `time` or `uuid` | `java.time.Instant`, `java.time.LocalDate`, `java.time.OffsetTime`, `java.util.UUID` |
| a `string` of the `contentEncoding` `base64` | `fs2.Chunk[Byte]` |
| an `array` | `List[A]` |
| an `object` without `properties`, of `additionalProperties` of one schema | `Map[String, A]` |
| an `object` without `properties` or a schema for them | `Map[String, io.circe.Json]` |
| `{}`, `true`, or a schema of no type | `io.circe.Json` |
| a type that may be `null`, such as `["string", "null"]` or a `oneOf` with `{"type": "null"}` | `Option[A]` |
| a `oneOf` or `anyOf` of several types, or several types, such as `["string", "integer"]` | a union type, such as `Cash \| Card` |
| a `$ref` to a definition of the same file, or `#`, the file itself | the definition's type, or the file's |

Each property is a field, read and written as the file has it:

- A required property is always written, and must be there when read; one that
  may be `null` is an `Option`, whose `None` is written as `null`.
- A property that is not required but has a `default` gets that default as its
  Scala default, and takes it when a record leaves the property out.
- A property that is not required, without a default, is an `Option`, `None`
  by default: `None` leaves it out, and it is read as `None` if it is left out
  or `null`.
- A required property with one fixed text value, a `const` or an `enum` of one
  value, is not a field. It is always written, and must have that value when
  read, so it tells a `oneOf`'s branches apart.

A `description` becomes the documentation of its type or field. Keywords that
only check values, such as `minimum`, `pattern` or a `format` other than those
above, are kept in the schema the type registers, and the classes do not check
them: neither writing nor reading checks values against the schema.

### Unions

A value of a union is written as its branch writes it. Reading takes up to two
passes:

1. Each branch reads the value and refuses properties its schema does not name.
   If exactly one branch reads it, the value belongs to that branch.
2. If none does, for example because a writer added a property, each branch
   reads it again and ignores the properties it does not name. If exactly one
   branch reads it this time, the value belongs to that branch.

If several branches read a value, a `oneOf` fails, since JSON Schema has the
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
`last4` is not required. JSON Schema itself refuses that value too. If `Cash`
allowed other properties, JSON Schema would refuse the card as well. To keep
branches apart, give each one a required constant, such as
`"kind": {"const": "cash"}`. Numbers are values of every branch of a number
type: `5` is both
an `integer` and a `number`, so a `oneOf` of the two fails on it, and an
`anyOf` reads it as the first.

### What is refused

The generator refuses a schema it cannot make types of, saying where in which
file, and why:

- a file whose title names no package, if `xkafkaJsonSchemaPackage` sets
  none, and two files of one type;
- a file that is not an object, an enum of text, or a `oneOf` or `anyOf` of
  objects, including one that may be `null`;
- `allOf`, `if`, `then` and `else`, `dependentSchemas` and `dependencies` of
  schemas, `patternProperties`, `prefixItems`, `items` that are a list of
  schemas, `$dynamicRef` and `$recursiveRef`, which combine schemas in ways
  types cannot hold;
- a `$ref` to another file, and a `$ref` beside
  keywords that give its value a type;
- an object of both `properties` and a schema for other properties;
- an object that requires a name none of its `properties` declares, since no
  value its class writes would have it;
- a `const` that is not text, or not of a required property, and an `enum` of
  values other than text without a `type`;
- a union with two branches of one Scala class, such as two kinds of text or
  two arrays, with a branch of any value, or with a union inside it, whose
  values written could not be told apart; and a type holding one union both as
  a `oneOf` and as an `anyOf`;
- a `oneOf` two of whose branches take `null`, which then matches no one of
  them;
- a definition that holds itself without a type of its own, such as an array
  of itself;
- two types of one name in one companion, or a type named as its file's;
- a `default` that is not a value of its property.

Whatever the format, two types whose classes would be one file are refused too:
an Avro and a JSON Schema type of one full name, or two whose names differ only
in case, which macOS and Windows treat as one file name.
