# Contributing to xkafka

Thanks for your interest in xkafka. How to build it and run its tests, including
the broker-backed suites, is described in
[Contributing](https://pmbmegalom.github.io/xkafka/contributing.html)
([docs/contributing.md](docs/contributing.md) in this repository).

## What a pull request should come with

- Code formatted and checked as continuous integration checks it (see
  [Formatting and checks](docs/contributing.md#formatting-and-checks)).
- Tests. Shared behaviour belongs in shared tests, which run on every backend.
  Backend-specific tests should cover translation, resource cleanup, and offset
  semantics at the platform boundary.
- Broker-backed tests for behaviour that involves a broker, in the shared
  suites, which run the same assertions against every backend.
- Documentation in `docs/` for changes to the public API. The documentation
  describes current behaviour.
- A regenerated workflow if the change affects continuous integration (see
  [Changes to the build](docs/contributing.md#changes-to-the-build)).
