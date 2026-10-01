# Errors

Failures xkafka classifies from Kafka or its portable data model are
`KafkaException` values. User-supplied effects keep their own failures, and
ordinary method precondition failures such as a non-positive partition-stream
queue threshold use their usual exception type.

| case | raised when |
| --- | --- |
| `BackendFailure` | the backend reported a failure |
| `InvalidBackendResponse` | a backend returned data the portable API cannot represent |
| `InvalidValue` | a value did not meet the rules of the type it was to become |
| `InvalidSettings` | settings could not be constructed |

`InvalidValue` carries the `ValidationError` that rejected the value, and
`InvalidSettings` carries every `SettingsError`, so accumulated validation
survives being lifted into an effect.

## Backend failures

`BackendFailure` preserves an original cause when one is available and carries a portable
`ErrorCode` along with retriable, fatal, and transaction-abort-required
classifications. A `None` for any of those means the backend did not make that
classification available.

The classifications preserve what a backend reports. When a backend supplies
only a code, `retriable` falls back to the portable `ErrorCode.retriable` answer.
`fatal` is always `true` for `ErrorCode.Fenced`; otherwise it, like
`transactionAbortRequired`, remains `None` when unavailable. Code that
needs one portable retry decision should use `ErrorCode.retriable`; commit
recovery does so as well:

```scala
if failure.code.exists(_.retriable) then retry else give up
```

It is true for an unavailable or loading coordinator, a leader that has moved, a
rebalance in progress, a network failure, and a timed out request.

## Portable codes

The named `ErrorCode` cases are Kafka protocol errors, which every broker
reports under the same code, plus the client-side conditions each backend raises
on its own. `ErrorCode.Other` carries a code with no portable meaning.

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
