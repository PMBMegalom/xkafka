# Headers

A header is a named value carried alongside a record's key and value, such as a
trace id or the tenant a record belongs to. A record can have several headers with
the same name, and they stay in the order they were added.

```scala mdoc:compile-only
// Attach headers to a record you send, and read one from a record you receive
import xkafka.*

def traced(topic: Topic): ProducerRecord[String, String] =
  val headers = Headers.empty.append("trace", "abc").append("attempt", 2)  // Each value is encoded by its type
  ProducerRecord(topic, "key", "value", headers = headers)

def traceOf[F[_]](received: CommittableConsumerRecord[F, String, String]): Option[Either[ValidationError, String]] =
  received.record.headers.get[String]("trace")  // Read the header named trace as a String
```

In a streams program, [steps that see the whole record](streams/README.md#steps-that-see-the-whole-record)
and [processors](streams/processors.md) read and change headers in the same way.

## Reading and changing headers

```scala mdoc:compile-only
// Read, replace and remove headers by name
import xkafka.*

val headers = Headers.empty.append("trace", "abc").append("attempt", 2).append("trace", "def")

val trace   = headers.get[String]("trace")   // Some(Right("def")): the last header with this name
val missing = headers.get[String]("tenant")  // None: no header has this name
val invalid = headers.get[Long]("attempt")   // Some(Left(...)): this value is not a Long

val retried = headers.set("attempt", 3)      // One "attempt" header, in place of any there were
val cleaned = headers.remove("trace")        // No "trace" headers
```

| To | Use |
| --- | --- |
| Read the last header with a name | `get[A](name)` |
| Read the raw bytes of every header with a name | `getAll(name)` |
| Read every header | `values`, then `as[A]` on each |
| Add a header, keeping the others | `append(name, value)` |
| Replace all headers with a name by one, at the end | `set(name, value)` |
| Remove all headers with a name | `remove(name)` |

`get` and `as` fail with `InvalidHeaderLength` when a number has the wrong width, not
by cutting it short, and with `MissingHeaderValue` when the header has no value.

## How values are written

| type | written as |
| --- | --- |
| `String` | UTF-8 |
| `Int` | four bytes, most significant first |
| `Long` | eight bytes, most significant first |
| `Chunk[Byte]` | the bytes themselves |

## Headers with no value

Kafka allows a header to have no value, which is `Header(name, None)`. Every platform
keeps it, and it stays different from an empty value, `Header(name, Some(Chunk.empty))`.

## Your own types

`HeaderSerializer` and `HeaderDeserializer` write and read a type. `contramap` and
`map` adapt one to another type, and `emap` rejects values the bytes can carry but
the type cannot:

```scala mdoc:compile-only
// Read and write a tenant, stored in a header as its name
import xkafka.*

final case class Tenant(name: String)

given HeaderSerializer[Tenant]   = HeaderSerializer.utf8.contramap(_.name)  // Write the name
given HeaderDeserializer[Tenant] = HeaderDeserializer.utf8.map(Tenant(_))   // Read the name

val tenant = Headers.empty.append("tenant", Tenant("acme")).get[Tenant]("tenant")
```

These are plain functions, not effects, because a header value is already in memory
by the time one is applied.
