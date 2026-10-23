# Errors

Failures Kafka reports, and values or settings xkafka rejects, are
`KafkaException` values. User-supplied effects keep their own failures, and
ordinary method precondition failures such as a non-positive partition-stream
queue threshold use their usual exception type.

| case | raised when |
| --- | --- |
| `BackendFailure` | the backend reported a failure |
| `InvalidBackendResponse` | the client returned data xkafka cannot represent, which is a bug in xkafka worth reporting |
| `InvalidValue` | a value did not meet the rules of the type it was to become |
| `InvalidSettings` | settings could not be constructed |
| `StartingOffsetsTimedOut` | a consumer's [starting offsets](offsets.md#starting-from-offsets-you-keep) were not supplied within their limit |
| `CommitFailed` | a commit kept failing as retriable after every retry its [recovery policy](offsets.md#retrying-a-failed-commit) allowed |
| `Unsupported` | a transaction was asked to record offsets that came from a consumer of another client |
| `UncommittedRevocation` | a consumer whose [rebalance-safe commits](offsets.md#committing-before-a-partition-moves) fail at their limit gave up partitions with records still uncommitted |

`InvalidValue` carries the `ValidationError` that rejected the value, and
`InvalidSettings` carries every `SettingsError`, so accumulated validation
survives being lifted into an effect.

## Backend failures

`BackendFailure` keeps the original failure as its cause, where there is one,
and carries an `ErrorCode` with three flags: `retriable`, `fatal`, and
`transactionAbortRequired`. A flag is `None` where the platform's client does
not report it, so which flags are present differs between platforms. `fatal` is
always `true` for `ErrorCode.Fenced`.

For a retry decision that is the same on every platform, ask the code, as
commit recovery does:

```scala
if failure.code.exists(_.retriable) then retry else give up
```

`ErrorCode.retriable` is true for an unavailable or loading coordinator, a
leader that has moved, a rebalance in progress, a network failure, and a timed
out request.

## Portable codes

The named `ErrorCode` cases are Kafka protocol errors, which every broker
reports under the same code, plus client-side conditions such as a timed out
request. `ErrorCode.Other(value)` carries a code xkafka does not name. Its value
comes from the platform's client and can differ between the JVM and Scala.js or
Native, so code meant for every platform should not match on it.

```scala
failure.code match
  case Some(ErrorCode.OffsetOutOfRange)        => resetPosition
  case Some(ErrorCode.SaslAuthenticationFailed) => refreshCredentials
  case _                                        => giveUp
```

Matching on a protocol error such as `OffsetOutOfRange` behaves the same on all
three platforms.

Three codes cover failures around transactional producers:

| code | raised when |
| --- | --- |
| `ErrorCode.Fenced` | a newer producer with the same transactional id has taken over, and every later transaction on this producer fails the same way |
| `ErrorCode.InvalidProducerEpoch` | the broker refuses the producer's epoch as out of date |
| `ErrorCode.Purged` | the client discarded a record before sending it, such as one still waiting when its transaction aborted |

`ErrorCode.OffsetResetRequired` fails a consumer's record stream when its
`autoOffsetReset` is `Fail` and a partition has no committed offset, or one that
is no longer in the partition. See
[Where a consumer starts](consuming.md#where-a-consumer-starts).
