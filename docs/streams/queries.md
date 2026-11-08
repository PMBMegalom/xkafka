# Querying stores

A running application can answer questions from its stores: a user's latest
balance in a table, the count of a page's views, and so on. Each member holds only
part of each store, so the member you ask may not hold the key. xkafka finds the
member that does and asks it, through a connection your application provides.

## Setting up

Every member of the application needs:

1. **An endpoint**, the host and port other members reach it at, given with
   `withQueries`. A member without one cannot be asked, so the keys it holds
   cannot be answered by any other member.
2. **A `Forward`**, which sends the bytes of a query to another member's endpoint
   and gives back the bytes it answers. This is your HTTP client, or whatever your
   members already use to talk to each other.
3. **An internal route** on that endpoint, which passes the bytes it receives to
   `answer` and sends back what `answer` returns.

```scala mdoc:compile-only
// Run a member that answers other members' queries and can query theirs
import cats.effect.IO
import fs2.Chunk

import xkafka.*
import xkafka.streams.*

def member(
    settings: StreamsSettings,
    topology: Topology[IO],
    sendTo: (Endpoint, Chunk[Byte]) => IO[Chunk[Byte]],      // Your HTTP client: posts the bytes, returns the response's bytes
    serveRoute: (Chunk[Byte] => IO[Chunk[Byte]]) => IO[Unit] // Your HTTP server: runs the internal route with this handler
): IO[Unit] =
  Endpoint.from("orders-3.internal", 8080).liftTo[IO].flatMap: here =>
    val forward: Forward[IO] = sendTo
    KafkaClient[IO].streams(settings.withQueries(here), topology, forward).use: streams =>
      IO.race(streams.run, serveRoute(streams.answer)).void // Other members' queries arrive here
```

Keep the internal route away from the outside world: it answers any query, for
any store.

## Asking

Describe a key-value store with `QueryableStore.keyValue`, giving its name and the
serdes its keys and values are written with, then pass a query to `query`. The call is
the same whichever member holds the key:

```scala mdoc:compile-only
// Look up a user's balance, asking again while the store is moving between members
import scala.concurrent.duration.*

import cats.effect.IO

import xkafka.streams.*

val balances = QueryableStore.keyValue(Store("balances"), Serde.utf8[IO], Serde.long[IO])

def balance(streams: KafkaStreams[IO], user: String): IO[Either[QueryProblem, Option[Long]]] =
  streams.query(balances.get(user)).flatMap:
    case Left(QueryProblem.Rebalancing) => IO.sleep(1.second) *> balance(streams, user) // Moving: ask again
    case answered                       => IO.pure(answered)                            // Found, absent, or another problem
```

These stores can be queried:

- a table's store, given when the table is read or made;
- the store of an aggregation, such as `count`, `reduce` or `aggregate`;
- a processor's `KeyValueStore`;
- a global table's store, which every member holds whole, so it is always
  answered by the member you ask.

Each store offers:

| query | answer |
| --- | --- |
| `get(key)` | what the store holds for the key, if anything |
| `range(from, to)` | the keys from `from` to `to`, both included, with their values |
| `all` | every key with its value |

`range` and `all` ask every member holding part of the store. Their keys come in
the order of their serialized bytes. For text keys that is alphabetical order;
for numbers written with `Serde.long`, negative numbers come after the positive
ones.

## Window and session stores

A windowed aggregation keeps a value for each key and window, and a session
aggregation one for each key and session. Query them with
`QueryableStore.window` and `QueryableStore.session`, giving the serdes of the
plain key, not of the windowed one:

```scala mdoc:compile-only
// Look up a page's views per minute over the last hour, and a user's visits
import scala.concurrent.duration.*

import cats.effect.IO

import xkafka.*
import xkafka.streams.*

val perMinute = QueryableStore.window(Store("views-per-minute"), Serde.utf8[IO], Serde.long[IO])
val visits    = QueryableStore.session(Store("visits"), Serde.utf8[IO], Serde.long[IO])

def lastHour(streams: KafkaStreams[IO], page: String): IO[Either[QueryProblem, Vector[(Long, Long)]]] =
  IO.realTime.flatMap: now =>
    val from = Timestamp.fromEpochMillis((now - 1.hour).toMillis)
    streams.query(perMinute.fetch(page, from, Timestamp.fromEpochMillis(now.toMillis))) // Each window's start, with its count

def visitsOf(streams: KafkaStreams[IO], user: String): IO[Either[QueryProblem, Vector[(Window, Long)]]] =
  streams.query(visits.fetch(user))                                                    // Each visit's window, with its count
```

| store | query | answer |
| --- | --- | --- |
| window | `fetch(key, from, to)` | the key's windows starting from `from` to `to`, both included, each by its start, earliest first |
| window | `fetchAll(from, to)` | every key's windows starting in that time, by start and then by key |
| session | `fetch(key)` | the key's sessions, each by its window, the earliest ending first |

A window store keeps windows for its retention, so older windows are no longer
found.

## When a query cannot be answered

`query` returns a `QueryProblem` when it cannot answer:

| problem | meaning | what to do |
| --- | --- | --- |
| `Rebalancing` | the part of the store the query needs is moving between members, or is still being filled | ask again shortly |
| `Unreachable(member, reason)` | the member holding it could not be reached, or failed to answer | check that member and your `Forward` |
| `NotXkafka(member)` | the member holding it answered in a format xkafka does not read | see [Java members](#java-members) |
| `UnknownStore(store)` | the program has no store of that name of the kind queried | check the store's name, and that it is a key-value, window or session store as queried |

A range fails as a whole if any member it needs cannot answer, so you never get
part of a store without being told.

## Standby copies

Where the broker keeps [standby copies](running.md#standby-copies) of the
stores, `query(..., allowStandby = true)` lets a copy answer. A copy can be a
little behind, but it can answer while its store is moving between members.

## Java members

An application can mix xkafka and Java Kafka Streams members, but Java members
cannot answer xkafka's queries. A query for a key a Java member holds fails with
`NotXkafka`, or with `Unreachable` where nothing answers at that member's
endpoint.
