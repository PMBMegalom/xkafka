# Contributing to xkafka

Thanks for your interest in xkafka.

## Building

xkafka cross-builds for the JVM, Scala.js on Node.js, and Scala Native, so a
change usually has to work on all three.

```sh
sbt test
```

The Scala.js backend uses the Node.js version in `.nvmrc`:

```sh
nvm use
```

The Native build compiles librdkafka with TLS and compression, which needs
OpenSSL, zlib, and zstd. See the README for the packages to install.

Format and check before opening a pull request, which is what continuous
integration runs:

```sh
sbt scalafmtAll scalafmtSbt
sbt scalafmtCheckAll scalafmtSbtCheck headerCheckAll test
```

## Broker-backed tests

Behaviour that involves a broker belongs in the shared suites, which run the
same assertions against every backend. Docker is required:

```sh
scripts/integration-test.sh
```

`KafkaConformanceSuite` asserts that the backends agree. A case may name the
backends known to diverge from it, and those are marked as expected to fail, so
fixing a backend makes the marked case fail and the marker has to be removed
with it. A marker therefore cannot outlive the divergence it records.

## Changes to the build

The continuous integration workflow is generated from `build.sbt`. After
changing anything that affects it:

```sh
sbt githubWorkflowGenerate
```

`githubWorkflowCheck` fails the build when the committed workflow is stale.

## What a change should come with

- Shared behaviour belongs in shared tests.
- Backend-specific tests should cover translation, resource cleanup, and offset
  semantics at the platform boundary.
- Public API changes belong in the README, which documents current behaviour.
