# swiftbets-building-blocks

[![ci](https://github.com/remonenaidoo/swiftbets-building-blocks-showcase/actions/workflows/ci.yml/badge.svg)](https://github.com/remonenaidoo/swiftbets-building-blocks-showcase/actions/workflows/ci.yml)

The cross-cutting infrastructure every SwiftBets service shares. Each concern is its own package so a service takes only what it uses.

| Package | What it gives a service |
|---|---|
| `SwiftBets.BuildingBlocks.Core` | `CorrelationContext`, `IFaultPoint` (deterministic crash points, refused in Production), `AddValidatedOptions<T>` (fail at startup, not at first use) |
| `SwiftBets.BuildingBlocks.Resilience` | Named Polly v8 pipelines: `idempotent-http`, `keyed-http` (retries a POST only when it carries `Idempotency-Key`), `keyed-grpc`, `sql-transient` (deadlocks plus Azure SQL transients), `kafka-produce` |
| `SwiftBets.BuildingBlocks.Messaging` | Idempotent Kafka producer with envelope headers; `KafkaConsumerHost<T>`: validate → handle in a DI scope → commit only after success; poison messages go to `<topic>.dlq` and the partition keeps flowing; transient failures and future `retry-due-at` messages pause the partition instead of sleeping a worker. `AddCompactedState<T>` keeps the latest value per key of a compacted topic in memory and holds readiness until it has read to the end |
| `SwiftBets.BuildingBlocks.Persistence` | `ISqlConnectionFactory`, `SqlResources` (one embedded `.sql` per query, no stored procedures), DbUp `MigrationRunner` with a journal, `MigrationRollback` (reverses migrations newest-first down to a target from scripts under `Rollbacks/`; refuses before changing anything if one is missing), `GrantSqlServerAppLoginAsync` (maps a platform-created login into the least-privilege `swiftbets_app` role), transactional `IInboxStore`, readiness checks |
| `SwiftBets.BuildingBlocks.Redis` | Lazily connecting multiplexer (a Redis blip degrades readiness, never crashes the host) and a readiness check |
| `SwiftBets.BuildingBlocks.Outbox` | `IOutbox.EnqueueAsync` inside the caller's transaction; `OutboxRelay` claims batches under a lease (`UPDLOCK, READPAST` on SQL Server, `FOR UPDATE SKIP LOCKED` on Postgres), keeps per-key order, backs off failures and never drops a row. `AddSqlServerOutbox` or `AddPostgresOutbox` |
| `SwiftBets.BuildingBlocks.Observability` | Serilog compact JSON, correlation ids in and out, OpenTelemetry tracing over OTLP, prometheus-net `/metrics`, `/health/live` and `/health/ready` |
| `SwiftBets.BuildingBlocks.Web` | The single error envelope (RFC 7807 + `code` + `correlationId`) for `Result` failures, validation and unhandled exceptions; security headers; RS256 JWT bearer validation against the issuer's JWKS |
| `SwiftBets.BuildingBlocks.Testing` | Testcontainers fixtures pinned to the exact platform images |

## Tests

```bash
dotnet test tests/SwiftBets.BuildingBlocks.Tests               # unit, no Docker
dotnet test tests/SwiftBets.BuildingBlocks.IntegrationTests    # Redpanda + SQL Server via Testcontainers
```

The integration suite includes the broker compatibility check (idempotent produce, consume, manual commit on the pinned client and broker), dead-lettering with the partition still flowing, transient redelivery, outbox rollback atomicity, relay publish-and-mark, lease skipping and inbox deduplication, on both SQL Server and Postgres.

## Part of SwiftBets

SwiftBets is a sportsbook and casino platform built as independent .NET services. It runs live at [swiftbets.remonenaidoo.com](https://swiftbets.remonenaidoo.com). These repositories show a selection of it: [Settlement](https://github.com/remonenaidoo/swiftbets-settlement-showcase) · [Wallet](https://github.com/remonenaidoo/swiftbets-wallet-showcase) · [Originals](https://github.com/remonenaidoo/swiftbets-originals-showcase) · [Steward](https://github.com/remonenaidoo/swiftbets-steward-showcase) · [Contracts](https://github.com/remonenaidoo/swiftbets-contracts-showcase).

## License

MIT
