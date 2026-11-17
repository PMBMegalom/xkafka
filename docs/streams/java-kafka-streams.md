# Mixing with Java Kafka Streams

An application can have some members running xkafka and others running Java
Kafka Streams, with the same program written for each. If every member of your
application runs xkafka, you can skip this page.

## Adding the module

The `xkafka-streams-java` module, on the JVM only, lets xkafka programs use code
written for Java Kafka Streams, and lets Java members take part in queries. It
depends on Java Kafka Streams itself:

```scala
// In build.sbt
libraryDependencies += "io.github.pmbmegalom" %% "xkafka-streams-java" % "@VERSION@"
```

It gives you:

- [`Partitioner.fromJava`](partitioning.md#java-partitioners-jvm-only), which
  uses a Java `StreamPartitioner` as a partitioner;
- [`Processor.fromJava`](processors.md#java-processors-jvm-only), which runs a Java
  `Processor` or `FixedKeyProcessor` as a step;
- [`JavaQueries` and `JavaQuerying`](queries.md#java-members), with which Java
  members answer and ask queries.

Java members need the module only for queries.

## Writing the Java program

The broker requires every member to describe the application in the same way, and
may refuse the Java members otherwise. Java's `Topology.describe` writes the same
text as xkafka's [`describe`](README.md#describing-a-program) for the same program,
so compare the two to check that they match.

Give the Java program the same names, serdes and stores as the xkafka program:

| xkafka | Java |
| --- | --- |
| The `Named` of a source, a step or a `to` | The name in `Consumed`, `Named` or `Produced` |
| The `Store` of a step that keeps one | The name in `Materialized` |
| The `Repartition` of `groupBy` or `groupByKey` | The name in `Grouped` |
| The name of a stream-table join whose stream changed its key | The name in `Joined` |
| A stream-stream join's name and `Store` | `StreamJoined.with(keySerde, valueSerde, otherValueSerde).withName(join).withStoreName(store)` |
| A foreign-key join's name | `TableJoined.as(name)` |
| `filterRecord`, `mapRecord` and the other [steps that see the whole record](README.md#steps-that-see-the-whole-record) | `process` if the key can change, `processValues` if it cannot, under the same names |
| A [processor](processors.md) | The same processor under the same name, with the same stores |
| `into`, sending records to several flows | The paths written in the order `into` lists them |
| A flow that gets records from several places | `merge`, matched in xkafka with the flow's `merged` step |

`Serde.utf8`, `Serde.long` and `Serde.bytes` write the same bytes as Java's
`Serdes.String()`, `Serdes.Long()` and `Serdes.ByteArray()`, and
`Windowed.timeSerde` and `Windowed.sessionSerde` the same as Java's
`WindowedSerdes.timeWindowedSerdeFrom` and `WindowedSerdes.sessionWindowedSerdeFrom`.

### Joins

If one input of a join has sub-topologies of its own, for example a table counted
after a repartition topic, build what `join` is called on first in the Java
program, before anything of the other input:

```java
// In the Java program: build the stream that join is called on first
KStream<String, String> orders = builder.stream("orders");
KTable<String, Long> visits = builder.<String, String>stream("visits")
    .groupBy((key, page) -> page, Grouped.as("by-page"))
    .count(Materialized.as("visits"));
orders.join(visits, (order, count) -> order + count).to("out");
```

For a foreign-key join, name the join with `TableJoined.as` and build the left
table first:

```java
// In the Java program: build the left table first, and name the join as the xkafka program does
KTable<String, String> orders = builder.table("orders");
KTable<String, String> customers = builder.table("customers");
orders.join(customers, order -> order.contains(":") ? order.split(":")[0] : null, (order, customer) -> order + " for " + customer,
    TableJoined.as("order-customer")).toStream().to("out");
```

Build a global table after the stream that first joins it:

```java
// In the Java program: build the global table after the stream that joins it
KStream<String, String> orders = builder.stream("orders");
GlobalKTable<String, String> catalog = builder.globalTable("products");
orders.join(catalog, (order, product) -> product, (product, details) -> product + ": " + details).to("out");
```

For some pairs of joined topic names, xkafka cannot tell whether its description
will match Java's. Joining topics with other names avoids it.

### Folding groups together

Java Kafka Streams cannot start a program in which several groups are
[folded together](tables.md#folding-streams-together) and any of them goes through
a repartition topic. Such a program runs on xkafka, but cannot have Java members.

### Partitioners

Give the Java members a `StreamPartitioner` that chooses the same partitions as
each of your [partitioners](partitioning.md), in `Produced`, `Repartitioned`, and a
foreign-key join's `TableJoined`. Java has no way to repartition a stream joined
with a table by the table's partitioner, so an application with Java members
cannot rely on that.

## The warning in `state`

When a program has one of the joins or folds above, the application's
[`state`](running.md#watching-the-application) lists
`StreamsProblem.JavaMembersMayBeRefused`, naming the topics. It is listed whether
or not any Java members run, so an application whose members all run xkafka can
ignore it.
