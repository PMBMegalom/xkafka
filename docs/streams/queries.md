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
   and gives back the bytes it answers.
3. **An internal route** on that endpoint, which passes the bytes it receives to
   `answer` and sends back what `answer` returns.

xkafka does not choose how members talk to each other. Here they use HTTP, with
[http4s](https://http4s.org):

```scala mdoc:compile-only
// Run a member that answers other members' queries over HTTP, and asks them for the keys they hold
import cats.effect.IO
import com.comcast.ip4s.*
import fs2.Chunk
import org.http4s.*
import org.http4s.dsl.io.*
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.ember.server.EmberServerBuilder

import xkafka.*
import xkafka.streams.*

def member(settings: StreamsSettings, topology: Topology[IO]): IO[Unit] =
  Endpoint.from("orders-3.internal", 8080).liftTo[IO].flatMap: here =>
    EmberClientBuilder.default[IO].build.use: http =>
      // Post a query's bytes to the member that holds the key, and give back what it answers
      val forward: Forward[IO] = (member, query) =>
        val route = Uri.unsafeFromString(s"http://${member.host}:${member.port}/xkafka-query")
        http.expect[Array[Byte]](Request[IO](Method.POST, route).withEntity(query.toArray)).map(Chunk.array(_))
      KafkaClient[IO].streams(settings.withQueries(here), topology, forward).use: streams =>
        // Answer the queries other members post here
        val internal = HttpRoutes.of[IO]:
          case request @ POST -> Root / "xkafka-query" =>
            request.as[Array[Byte]].flatMap(query => streams.answer(Chunk.array(query))).flatMap(answer => Ok(answer.toArray))
        val server = EmberServerBuilder.default[IO].withHost(ipv4"0.0.0.0").withPort(port"8080").withHttpApp(internal.orNotFound).build
        server.surround(streams.run)                       // Serve the route for as long as the member runs
```

Keep the internal route away from the outside world: it answers any query, for
any store.

## Asking

Describe a key-value store with `QueryableStore.keyValue`, giving its name and the
serdes its keys and values are written with, then pass a query to `query`. The call
is the same whichever member holds the key. Here a public route answers with a
user's balance:

```scala mdoc:compile-only
// Answer GET /balance/<user> with the user's balance, wherever it is held
import cats.effect.IO
import org.http4s.*
import org.http4s.dsl.io.*
import org.http4s.headers.`Retry-After`

import xkafka.streams.*

val balances = QueryableStore.keyValue(Store("balances"), Serde.utf8[IO], Serde.long[IO])

def public(streams: KafkaStreams[IO]): HttpRoutes[IO] =
  HttpRoutes.of[IO]:
    case GET -> Root / "balance" / user =>
      streams.query(balances.get(user)).flatMap:
        case Right(Some(balance))           => Ok(balance.toString)
        case Right(None)                    => NotFound()
        case Left(QueryProblem.Rebalancing) => ServiceUnavailable().map(_.putHeaders(`Retry-After`.unsafeFromLong(1))) // Moving: ask again shortly
        case Left(problem)                  => BadGateway(problem.toString)
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
the order of their serialized bytes. For text keys that is the order of their
characters, capitals before lower-case letters, so `Zoe` comes before `adam`. For
numbers written with `Serde.long`, negative numbers come after the positive ones.

## Window and session stores

A windowed aggregation keeps a value for each key and window, and a session
aggregation one for each key and session. Query them with
`QueryableStore.window` and `QueryableStore.session`, giving the serdes of the key
itself, not the windowed serde your output topic uses:

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
| window | `fetch(key, from, to)` | the key's windows starting from `from` to `to`, both included: each window's start time with its value, earliest first |
| window | `fetchAll(from, to)` | the windows of every key starting in that time: each key and window with its value, earliest start first, then by key |
| session | `fetch(key)` | the key's sessions: each session's window with its value, the earliest ending first |

`fetchAll` gives each window's start and end; the end is the start plus the window
size your program sets. A window store keeps windows only for its retention, so
older windows are no longer found.

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

An application can mix xkafka and Java Kafka Streams members. A Java member
answers xkafka's queries through `JavaQueries`, from the
[`xkafka-streams-java` module](README.md#java-partitioners-jvm-only). Give it the
instance, the application's id, and each store that can be queried with its Java
serdes, then call its `answer` from the internal route. The member's
`application.server` setting is the endpoint xkafka members reach it at.

```java
// Let a Java member answer xkafka members' queries of its page view counts
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;

import xkafka.streams.interop.JavaQueries;

final class Answering {
  private final JavaQueries queries;

  Answering(KafkaStreams streams) {
    queries =
        JavaQueries.of(streams, "page-view-counter")
            .keyValue("views-per-page", Serdes.String(), Serdes.Long())   // A table or a count
            .window("views-per-minute", Serdes.String(), Serdes.Long());  // A windowed count
  }

  // Call this from the internal route: the bytes posted to it in, the answer's bytes out
  byte[] answer(byte[] query) {
    return queries.answer(query);
  }
}
```

`JavaQueries` also takes `session` stores. It reads each store through Java's own
interactive queries on that instance. A Java member without it does not answer
in a format xkafka reads, so a query that needs it fails with `NotXkafka`, or with
`Unreachable` where nothing answers at its endpoint.

A Java member asks with `JavaQuerying`, giving it the instance, its topology, the
application's id, and how to send a query to another member. It finds the member
holding a key through Java's own metadata, which lists xkafka members too, and
sends the query to that member's internal route, even when that member is itself:

```java
// Let a Java member ask for a page's view count, wherever it is held
import java.util.Optional;
import java.util.function.BiFunction;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.HostInfo;

import xkafka.streams.interop.JavaQuerying;

final class Asking {
  private final JavaQuerying querying;

  // `post` is your HTTP client: it posts the bytes to the member's internal route and returns what it answers
  Asking(KafkaStreams streams, Topology topology, BiFunction<HostInfo, byte[], byte[]> post) {
    querying = JavaQuerying.of(streams, topology, "page-view-counter", post);
  }

  // Throws a QueryProblemException when it cannot answer, whose problem says why, as xkafka's queries do
  Optional<Long> views(String page) {
    return querying.get("views-per-page", page, Serdes.String(), Serdes.Long());
  }
}
```

| method | answer |
| --- | --- |
| `get(store, key, ...)` | what a key-value store holds for the key, if anything |
| `range(store, from, to, ...)` | the keys of a key-value store from `from` to `to`, both included, as a list of Kafka's `KeyValue` |
| `all(store, ...)` | every key of a key-value store with its value, as a list of Kafka's `KeyValue` |
| `fetch(store, key, from, to, ...)` | a key's windows starting in that time: each window's start time with its value |
| `fetchAll(store, from, to, windowSize, ...)` | the windows of every key starting in that time, each as a `Windowed` key with its value |
| `sessions(store, key, ...)` | a key's sessions, each as a `Windowed` key with its value |

Java's metadata does not say how long a store's windows are, so `fetchAll` needs
the size from you, and ends each window that long after it starts. Give it the
size your program's windows have: a wrong size gives wrong ends, and nothing can
tell.
