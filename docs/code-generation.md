# Generating classes from schema files

Where your Avro schemas come first, as .avsc or .avdl files that other
applications share, xkafka can generate Scala classes from them. The classes
write and read exactly the schemas in your files, on the JVM, JavaScript and
Native, and need only `xkafka-avro` to compile.

## With sbt

Add the plugin, which works with sbt 1 and sbt 2, and `xkafka-avro`:

```scala
// project/plugins.sbt
addSbtPlugin("io.github.pmbmegalom" % "sbt-xkafka-codegen" % "@VERSION@")
```

```scala
// build.sbt
libraryDependencies += "io.github.pmbmegalom" %%% "xkafka-avro" % "@VERSION@"
```

Put your .avsc and .avdl files in `src/main/avro`. Each time the project
compiles, the classes are generated from them again if any of them changed. A
schema the generator refuses fails the build, saying why.

To read schemas from other directories, such as the shared directory of a
project built for several platforms, set `xkafkaAvroSources`:

```scala
Compile / xkafkaAvroSources := Seq(baseDirectory.value / ".." / "shared" / "src" / "main" / "avro")
```

The plugin runs `xkafka-codegen` of its own version, in a JVM of its own, so
nothing the generator uses reaches your build's classpath.

## Without sbt

`xkafka-codegen` is also a program, whose jar names its main class,
`xkafka.codegen.Main`. Its first argument is the directory the classes are
written to; the others are .avsc and .avdl files, or directories whose .avsc
and .avdl files, at any depth, are read. Run it from any build tool that can run
a program from a Maven artifact:

```
xkafka.codegen.Main target/generated src/main/avro
```

## What is generated

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

## What is refused

The generator refuses a schema it cannot write exactly, saying which type and
field, and why:

- a named type without a namespace, which its package is made from;
- properties of a schema or field other than the ones Avro defines, a field's
  sort order, a logical type xkafka does not know, and a `fixed` type's
  documentation, none of which xkafka's schemas keep;
- `local-timestamp-nanos`, and a `decimal` held in a `fixed` type, which xkafka
  has no codec for;
- a `decimal` in a union of several types;
- `null` in a union anywhere but first, or right after the type of the field's
  default;
- a record with fields of one Scala type at two precisions, such as a
  `timestamp-millis` and a `timestamp-micros`, or with the same union in two
  orders;
- two files declaring one type differently.

JSON Schema files are not read yet.
