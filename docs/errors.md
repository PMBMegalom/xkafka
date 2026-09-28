# Errors

Every failure xkafka raises is a `KafkaException`.

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

`BackendFailure` preserves the original cause and carries a portable
`ErrorCode` along with retriable and fatal classifications. A `None` for any of
those means the backend did not make that classification available.

`retriable` answers the same way on all three platforms for the same condition.
`ErrorCode.retriable` gives that answer directly, and commit recovery uses it to
decide what to retry:

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
