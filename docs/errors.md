# Errors

xkafka raises a `KafkaException` when Kafka reports a failure, or when it
refuses a value or settings. [Streams](#streams), the
[schema registry](#schema-registry) and [deserializers](#deserializing) have
exceptions of their own, described below. A failure from your own code, such as
a serializer or a handler, reaches you unchanged. An invalid argument, such as a
`maxQueuedRecords` that is not positive, raises `IllegalArgumentException`.

| case | raised when |
| --- | --- |
| `BackendFailure` | Kafka or the platform's client reported a failure |
| `InvalidBackendResponse` | the client returned data xkafka cannot represent, which is a bug in xkafka worth reporting |
| `InvalidValue` | a value did not meet the rules of the type it was to become |
| `InvalidSettings` | settings could not be constructed |
| `StartingOffsetsTimedOut` | a consumer's [starting offsets](offsets.md#starting-from-offsets-you-keep) were not supplied within their limit |
| `CommitFailed` | a commit kept failing as retriable after every retry its [recovery policy](offsets.md#retrying-a-failed-commit) allowed, with a policy that retries at least once |
| `Unsupported` | a transaction was asked to record offsets that did not come from a consumer of `KafkaClient`, such as offsets from a committer of your own |
| `UncommittedRevocation` | a consumer whose [rebalance-safe commits](offsets.md#committing-before-a-partition-moves) fail at their limit gave up partitions with records still uncommitted |

`InvalidValue` carries the `ValidationError` that rejected the value, and
`InvalidSettings` carries every `SettingsError`, so accumulated validation
survives being lifted into an effect.

## Streams

An invalid streams program, or invalid streams settings, raise an exception of
their own when `liftTo` carries the refusal into the effect:

- `InvalidTopology` carries every `TopologyError` in `errors`.
- `InvalidStreamsSettings` carries every `StreamsSettingsError` in `errors`.

## Schema registry

The registry client and its serdes fail with a `RegistryException`, and refused
registry settings raise `InvalidRegistrySetting`. See
[Schema registry](schema-registry.md#errors).

## Deserializing

A record without a value fails the provided deserializers with
`Deserializer.NullValueException`, unless they read with `.option`. See
[Nulls and tombstones](serialization.md#nulls-and-tombstones).

## Backend failures

`BackendFailure` keeps the original failure as its cause, if there is one. Its
`code` is the failure's `ErrorCode`, if the client gave one, and three flags say
more:

- `retriable` is what the code's `retriable` says, so it is the same on every
  platform, and `None` only if there is no code.
- `fatal` and `transactionAbortRequired` are what the platform's client
  reports: that the client cannot recover, and that the current transaction must
  be aborted. Each is `None` if the client does not report it. A
  transactional producer fenced by a newer one fails with `fatal` set to `true`
  on every platform.

```scala
// Retry only a failure that can clear on its own
action.handleErrorWith:
  case failure: KafkaException.BackendFailure if failure.retriable.contains(true) => retry
  case failure                                                                    => giveUp(failure)
```

`ErrorCode.retriable` is true for a network failure, a timed out request, an
unavailable broker, a partition whose leader is unavailable or has moved, an
unavailable, loading or moved coordinator, and a rebalance in progress.

## Matching on error codes

The named `ErrorCode` cases are Kafka protocol errors, which every broker
reports under the same code, plus client-side conditions such as a timed out
request. `ErrorCode.Other(value)` carries a code xkafka does not name. Its value
comes from the platform's client and can differ between the JVM and Scala.js or
Scala Native, so code meant for every platform should not match on it.

```scala
// Choose what to do by the failure's code
failure.code match
  case Some(ErrorCode.OffsetOutOfRange)        => resetPosition
  case Some(ErrorCode.SaslAuthenticationFailed) => refreshCredentials
  case _                                        => giveUp
```

Three codes cover failures around transactional producers:

| code | raised when |
| --- | --- |
| `ErrorCode.Fenced` | a newer producer with the same transactional id has taken over, and every later transaction on this producer fails the same way; for a consumer, another with the same `groupInstanceId` has taken its place in the group |
| `ErrorCode.InvalidProducerEpoch` | the broker refuses the producer's epoch as out of date |
| `ErrorCode.Purged` | the client discarded a record before sending it, for example one still waiting when its transaction aborted |

`ErrorCode.OffsetResetRequired` fails a consumer's record stream when its
`autoOffsetReset` is `Fail` and a partition has no committed offset, or one that
is no longer in the partition. See
[Where a consumer starts](consuming.md#where-a-consumer-starts).
