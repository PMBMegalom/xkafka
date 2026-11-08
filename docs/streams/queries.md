# Querying stores

A running application can answer questions from its stores, such as the latest
value a table holds for a key. Each member holds only some of each store, so a
query may need a member other than the one asked. xkafka finds which member holds
the key and asks it. The application supplies the way members reach each other,
over whatever it already uses, such as HTTP.

## Setting up

Each member needs three things:

- the endpoint other members reach it at, given with `withQueries`;
- a `Forward` that sends a query's bytes to another member's endpoint and gives
  back the bytes it answers;
- a route that hands the bytes another member sends to `answer`, and sends back
  what it returns.

```scala mdoc:compile-only
// Run a member whose stores other members can query, and that can query theirs
import cats.effect.IO
import fs2.Chunk

import xkafka.*
import xkafka.streams.*

def member(
    settings: StreamsSettings,
    topology: Topology[IO],
    post: (Endpoint, Chunk[Byte]) => IO[Chunk[Byte]],    // Your HTTP client, posting to another member
    serve: (Chunk[Byte] => IO[Chunk[Byte]]) => IO[Unit]  // Your HTTP server's internal route, run until stopped
): IO[Unit] =
  Endpoint.from("orders-3.internal", 8080).liftTo[IO].flatMap: here =>
    val forward: Forward[IO] = post                      // How this member reaches the others
    KafkaClient[IO].streams(settings.withQueries(here), topology, forward).use: streams =>
      IO.race(streams.run, serve(streams.answer)).void   // Other members' queries are answered by `answer`
```

Keep that route internal: it reads any store a query names.

## Asking

Name the store and its serdes with `QueryableStore`, then ask:

```scala mdoc:compile-only
// Answer a user's balance from whichever member holds it
import cats.effect.IO

import xkafka.streams.*

def balance(streams: KafkaStreams[IO], user: String): IO[Option[Long]] =
  val balances = QueryableStore.keyValue(Store("balances"), Serde.utf8[IO], Serde.long[IO])
  streams.query(balances.get(user)).flatMap:                // The same call wherever the key is held
    case Right(found)                   => IO.pure(found)
    case Left(QueryProblem.Rebalancing) => IO.raiseError(new RuntimeException("moving, ask again shortly"))
    case Left(other)                    => IO.raiseError(new RuntimeException(other.toString))
```

`QueryableStore.keyValue` reaches the stores of tables, of aggregations such as
`count` and `reduce`, and of processors' `KeyValueStore`s, and global tables'
stores, which every member holds whole. It offers:

| query | answer |
| --- | --- |
| `get(key)` | what the store holds for the key, if anything |
| `range(from, to)` | the keys from `from` to `to`, both included, and their values |
| `all` | every key and its value |

`range` and `all` ask every member holding part of the store and give the keys in
the order of their serialized bytes, as Java's stores order them.

## When a query cannot be answered

`query` answers with a `QueryProblem` where it cannot:

| problem | meaning |
| --- | --- |
| `Rebalancing` | the part of the store the query needs is moving between members, or a member is still filling its stores: ask again shortly |
| `Unreachable(member, reason)` | the member holding it could not be asked, or failed to answer |
| `NotXkafka(member)` | the member holding it answered in a way xkafka does not read, as a Java Kafka Streams member does |
| `UnknownStore(store)` | the program has no key-value store of that name |

A range fails whole if any member it needs fails, so an answer is never missing
part of the store without saying so.

## Standby copies

Where the broker's `streams.num.standby.replicas` asks for standby replicas, a
query can let a standby copy answer, with `allowStandby = true`. A copy may be a
little behind the member processing its records, but it can still answer while
its work moves.

## Mixing with Java members

Java Kafka Streams members of the same application cannot answer xkafka's
queries. A query that needs one fails with `NotXkafka`, or with `Unreachable`
where nothing answers at its endpoint.
