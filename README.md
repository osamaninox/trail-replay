# Trail Replay

Event sourcing and PostgreSQL CDC (Change Data Capture) system — track data changes, replay event streams, and capture WAL changes for audit and revert workflows.

## Overview

Trail Replay has two core capabilities:

- **Trail API** — Event-sourced domain model with append-only event streams and replay
- **Stream Processor** — PostgreSQL logical replication consumer that captures WAL changes into queryable tables (`wal_transaction`, `wal_change`) for audit trails and data reverts

```
                  ┌──────────────────────┐
POST /trails ────►│     Trail API        │────► trails / events tables
GET  /events ────►│     (Go 1.22 HTTP)   │
                  └──────────────────────┘

                  ┌──────────────────────┐
WAL stream ──────►│  Stream Processor    │────► trailwal DB
(pgoutput)        │  (logical replication)│     (wal_transaction / wal_change)
                  └──────────────────────┘
```

## Getting Started

### Prerequisites

- Go 1.25+
- PostgreSQL 16+ (with `wal_level = logical`)
- Docker (optional, for containerized setup)

### Running with Docker

```bash
docker compose up --build
```

The API starts on `http://localhost:8080` and PostgreSQL on `localhost:5433`.

### Running Locally

```bash
# Start PostgreSQL
docker compose up postgres -d

# Run migrations
go run ./cmd/migrate
go run ./cmd/migrate -wal

# Start API server
go run ./cmd/api
```

### Stream Processor

The stream processor connects to PostgreSQL logical replication, captures WAL changes, and stores them in the `trailwal` database for audit and replay.

```bash
# Ensure trailwal database exists and migrations are applied
go run ./cmd/migrate -wal

# Start the WAL stream processor
go run ./cmd/stream-process
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST /trails` | `{ "name": "..." }` | Create a new trail |
| `GET /trails` | — | List all trails |
| `GET /trails/{id}` | — | Get a trail by ID |
| `POST /trails/{id}/events` | `{ "type": "created", "payload": {} }` | Append an event |
| `GET /trails/{id}/replay?from=1` | — | Replay events from a sequence |
| `GET /wal/transactions?page=1&page_size=20` | — | List paginated WAL transactions with grouped wal_changes |

## Architecture

Hexagonal Architecture (Ports & Adapters). Business logic in `internal/core/trail/` depends only on interfaces. Adapters in `internal/adapters/` implement those interfaces.

```
cmd/api ──► adapters/inbound/http ──► core/trail/ports/inbound
                                    ──► core/trail/services
                                         core/trail/ports/outbound ◄── adapters/outbound/storage
```

### Project Structure

```
cmd/
├── api/                  # HTTP API server
├── stream-process/       # WAL logical replication consumer
└── migrate/              # Database migration runner
internal/
├── core/trail/
│   ├── domain/           # Entities (Trail, Event)
│   ├── ports/inbound/    # Driving port (TrailService)
│   ├── ports/outbound/   # Driven port (TrailRepository)
│   └── services/         # Business logic
└── adapters/
    ├── inbound/http/     # Go 1.22 HTTP handler
    └── outbound/storage/ # In-memory and PostgreSQL repositories
migrations/               # Goose SQL migrations for WAL storage
```

## Configuration

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `HTTP_ADDR` | `:8080` | API listen address |
| `DB_HOST` | `localhost` | PostgreSQL host |
| `DB_PORT` | `5432` | PostgreSQL port |
| `DB_USER` | `trailuser` | Database user |
| `DB_PASSWORD` | `trailpass` | Database password |
| `DB_NAME` | `traildb` | Database name |
| `WAL_DB_NAME` | `trailwal` | WAL database name |

## Testing

Unit tests (no external dependencies):

```bash
go test -short ./...
```

Integration tests (requires Docker for testcontainers):

```bash
go test ./internal/adapters/inbound/http/
```

Run all tests:

```bash
go test ./...
```

Integration tests require Docker to be running and will be skipped automatically with `-short` flag or when `CI=1 SKIP_INTEGRATION=1` is set.
