# Production Readiness TODO

## Critical (data loss or breakage)

- [*] **Persist LSN checkpoint** — Start replication from last committed LSN instead of `0` on restart
- [*] **Send standby status updates** — Periodically send `StandbyStatusUpdate` to prevent WAL accumulation
- [ ] **Remove automatic slot drop** — `cleanup()` drops the replication slot on shutdown, destroying position; make it opt-in or persist across restarts
- [*] **Generate `forward_dml_sql` / `reverse_dml_sql`** — Both are `NOT NULL` on `wal_change` but never populated; replay will produce empty SQL
- [*] **Set `changed_columns`** — Required column with `NOT NULL` default; must compute changed columns for UPDATE operations

## Configuration & Security

- [ ] **Externalize config** — Move DB creds, ports, slot names to env vars / config file / secret store
- [ ] **Enable TLS** — Remove `sslmode=disable`; support proper SSL/TLS connection
- [ ] **Strip PII from logs** — `printTupleData` logs all row values; gate behind debug level or scrub

## Reliability & Resilience

- [ ] **Process supervision & auto-restart** — If the stream process crashes or exits unexpectedly, it must restart automatically. Options:
  - **systemd** service unit with `Restart=always` and `RestartSec=5s` (simplest for VMs/bare metal)
  - **Docker** restart policy `--restart=always` or `unless-stopped`
  - **Kubernetes** Deployment with `replicas: 1` and liveness/readiness probes (needs `/healthz` endpoint first)
  - A `supervisord` / `systemd` watchdog that monitors the PID and respawns
- [ ] **Internal reconnection loop** — Instead of calling `os.Exit(0)` / `log.Fatalf` on transient failures (network blip, PG restart), wrap the stream loop in a retry loop with exponential backoff. Only exit on truly unrecoverable errors (e.g., slot dropped, invalid config). This makes the process self-healing without external supervision.
- [ ] **Crash-safe LSN checkpointing** — Restarting from a stale LSN replays already-persisted transactions (ON CONFLICT DO NOTHING handles dedup), but restarting from LSN 0 replays everything. Must persist `lastLSN` atomically with each committed transaction so no WAL gap exists on restart.
- [ ] **Replication slot preservation** — Never drop the slot on exit (`cleanup()` currently drops it). Dropping the slot loses the server-side WAL position; on restart you must re-snapshot the entire database. Keep the slot alive across process restarts and only drop it via an explicit admin action.
- [ ] **Heartbeat / liveliness check** — If the process freezes (deadlock, infinite loop, GC pause), external supervision won't notice unless it has a health check. A `/healthz` HTTP endpoint or a file touch (mtime-based watchdog) lets the supervisor distinguish "running but idle" from "dead".
- [ ] **Add retry with backoff** — Network blips or DB restarts crash via `log.Fatalf`; add exponential backoff
- [ ] **Use context with timeouts** — All `context.Background()` calls hang forever; add request-scoped timeouts
- [ ] **Graceful drain on shutdown** — Signal handler calls `os.Exit(0)` immediately; wait for in-flight work
- [ ] **Tune connection pool** — `SetMaxOpenConns`, `SetMaxIdleConns`, `SetConnMaxLifetime` on the `sqlx.DB`

## Observability

- [ ] **Add health check HTTP server** — `/healthz` / `/readyz` endpoints for orchestration probes
- [ ] **Add metrics** — Prometheus counters for processed txns, errors, lag, latency
- [ ] **Structured logging** — Replace `log.Printf` with levels (debug/info/warn/error), JSON output, correlation IDs

## Throughput & Scalability (cmd/stream-process)

Bottlenecks identified in `postgres_stream_process.go`:

- [ ] **Batch `wal_change` inserts** — Currently inserts changes one-by-one in a `for` loop with `tx.Exec()` (line 203-213); switch to multi-VALUES `INSERT` or `CopyIn` for 3-10x improvement on wide transactions
- [ ] **Decouple receive from persist** — Single goroutine handles receive → parse → log → persist → checkpoint; use a channel + worker goroutine to pipeline WAL consumption and DB writes, ~2-3x gain
- [ ] **Batch small transactions into single DB txn** — Each commit triggers its own DB transaction + checkpoint UPSERT; batch N small txns into one DB commit+checkpoint cycle, ~2x for small-transaction workloads
- [ ] **Remove verbose logging from hot path** — Every message type logs 2-5 lines via `log.Printf` (tuple data, relation schema, etc.); gate behind debug level or strip entirely for 20-50% TPS improvement
- [ ] **Preload relation schema on startup** — PK lookups hit `information_schema` synchronously on first RelationMessage per table (line 611-639); preload all PK info at boot to avoid latency spikes during table discovery
- [ ] **Increase DB connection pool** — `sqlx.DB` uses default pool size (2 connections); tune `SetMaxOpenConns`/`SetMaxIdleConns` for higher concurrency once worker pool is introduced
- [ ] **Evaluate `test_decoding` or `wal2json` plugin** — `pgoutput` is optimized for physical replication; `wal2json` may provide simpler payloads with fewer parse steps for logical CDC use cases
- [ ] **Horizontally partition by table** — Single replication slot can only be consumed by one process; for multi-table workloads, create per-table publications+slots and run independent consumers

## Architecture

- [ ] **Add backpressure** — No bound on `currentTxn.changes`; implement size limit or streaming flush
- [ ] **Resolve SQLX / GORM mismatch** — Persistence uses raw `sqlx` but entities define GORM tags; pick one pattern
- [ ] **Dead-letter handling** — Bad WAL messages are silently skipped; add DLQ or retry mechanism
- [ ] **Validate slot existence** — Ensure replication slot exists with matching plugin args before starting

## Testing

- [ ] **Unit tests** — `parseLogicalMessage`, `tupleToRow`, `persistCurrentTxn`
- [ ] **Integration tests** — Full WAL stream cycle against a real or testcontainer PG

## Docker Image Distribution

- [ ] **Fix Dockerfile build target** — Currently builds `./cmd/api`; update to build `./cmd/stream-process` (or build both binaries)
- [ ] **Run migrations on startup** — Bundle migrations directory in the image and auto-run goose migrations before starting the application (use embedded filesystem or COPY + `goose up`)
- [ ] **Add `.dockerignore`** — Exclude `.git`, `mocks`, local `docker-compose.yml`, IDE files, etc. from build context
- [ ] **Use a non-root user** — Add `USER 1000` in runtime stage for security
- [ ] **Add `HEALTHCHECK`** — Add `/healthz` HTTP endpoint first, then add `HEALTHCHECK` instruction in Dockerfile
- [ ] **Add standard OCI labels** — `org.opencontainers.image.source`, `org.opencontainers.image.version`, `org.opencontainers.image.title`, `org.opencontainers.image.description`
- [ ] **Set up GitHub Container Registry (GHCR) publishing** — Push images to `ghcr.io/osamakhalid/trail-replay` via GitHub Actions
- [ ] **Multi-arch builds** — Build and push `linux/amd64` and `linux/arm64` using `docker/setup-buildx-action` + `docker/setup-qemu-action`
- [ ] **Version tagging strategy** — Tag images with `latest`, `vX.Y.Z` (semver), and git SHA on every push to main; use `docker/metadata-action`
- [ ] **CI/CD pipeline (GitHub Actions)** — `.github/workflows/docker-publish.yml`:
  - Trigger on: tag push (`v*`), push to `main`, PRs
  - Steps: checkout → set up Go → run tests → login to GHCR → build & push image
- [ ] **Add `docker-compose.prod.yml` example** — A minimal compose file users can reference to run trail-replay + PostgreSQL together
- [ ] **Document usage** — Add README section with `docker run` command, required env vars, volume mounts, and compose example

## Replication Slot Hardening (from [Mastering Postgres Replication Slots](https://www.morling.dev/blog/mastering-postgres-replication-slots/))

- [x] **Use `pgoutput` plugin** — Already in use for logical decoding (binary format, built-in since PG 10+)
- [x] **Consider REPLICA IDENTITY FULL** — Migration `00003` sets it on `events`/`orders`; apply to any new tables added to the publication
- [ ] **Set `max_slot_wal_keep_size`** — Add to `postgresql.conf` (e.g. `50GB`) so PostgreSQL invalidates the slot instead of filling the disk if the consumer falls too far behind; currently unlimited, risking disk exhaustion
- [ ] **Enable heartbeat via `pg_logical_emit_message()`** — Periodically write a logical decoding message to advance the slot even when no data changes occur; prevents WAL retention buildup on low-traffic databases sharing a host with busy ones. Requires `GRANT EXECUTE ON FUNCTION pg_logical_emit_message(...)` for the replication user
- [ ] **Use table-level publications instead of `FOR ALL TABLES`** — Current code does `CREATE PUBLICATION trail_replay_pub FOR ALL TABLES` which streams changes for every table in the database; switch to `CREATE PUBLICATION ... FOR TABLE t1, t2` or `FOR TABLES IN SCHEMA public` to reduce CPU, network I/O, and WAL retention. Also removes superuser requirement for publication creation
- [ ] **Add column lists to publication** — PG 15+ supports `FOR TABLE customers (id, name)` to exclude large/unnecessary columns (e.g., binary blobs) from the replication stream; reduces bandwidth and egress cost
- [ ] **Add row filters to publication** — PG 15+ supports `FOR TABLE customers WHERE (is_test_account IS FALSE)` to exclude test/logically-deleted rows from replication
- [ ] **Enable fail-over replication slots** — PG 17+: create slot with `failover=true`, set `synchronized_standby_slots` on primary, `sync_replication_slots=on` + `hot_standby_feedback=on` on standby, add dbname to `primary_conninfo`; allows seamless consumer resume after promoting a standby without losing slot position
- [ ] **Monitor replication slot metrics** — Track total WAL size, retained WAL per slot, remaining safe WAL per slot, slot status (active/inactive/invalid), and disk spill via `pg_stat_replication_slots`. Use `postgres_exporter` + Grafana dashboard. Set alerts: disk > 60-70%, slot inactive > 30 min, retained WAL > 10-20 GB
- [ ] **Increase `logical_decoding_work_mem`** — Default is 64 MB; if large transactions cause disk spill during WAL decoding (visible via `pg_stat_replication_slots.spill_bytes`), increase this setting to reduce I/O load
- [ ] **Drop unused replication slots** — Ensure slots are cleaned up when no longer needed; currently `cleanup()` drops the slot on shutdown but that's wrong for production. For PG 18+, configure `idle_replication_slot_timeout` (e.g., 48h) to auto-invalidate inactive slots

## Large-Scale Data Reverts (Job/Workflow Engine)

- [ ] **Decide workflow engine** — Evaluate candidates (DBOS workflows, Temporal, River, pg-boss, or a custom job table); capture tradeoffs in an ADR
- [ ] **Design revert job schema** — Persist revert jobs with status (`pending` → `in_progress` → `completed`/`failed`), input LSN range, affected tables, row count, progress tracking
- [ ] **Batch revert execution** — Split large reverts into configurable batch sizes (e.g., 1000 rows per batch) to avoid long-running transactions and excessive locks
- [ ] **Idempotent reverts** — Each revert batch must be safe to retry; use `ON CONFLICT DO NOTHING` or pre-check row state before applying reverse DML
- [ ] **Progress tracking & resume** — Track last successfully reverted change per job so a failed/cancelled job can resume from where it left off
- [ ] **Dry-run mode** — Support previewing revert SQL for a given LSN range without executing, showing affected row count and tables
- [ ] **Table-level filtering** — Allow reverts scoped to specific tables instead of the entire WAL stream
- [ ] **Time-based reverts** — Support reverting changes within a time window (e.g., "revert everything between 14:00 and 14:05") by mapping timestamps to LSN ranges
- [ ] **Concurrency control** — Prevent overlapping reverts on the same table; acquire advisory locks per table before starting a revert job
- [ ] **Revert audit log** — Record every `wal_change` row that was reverted with the job ID, timestamp, and result status for traceability
- [ ] **Validation after revert** — Run optional post-revert checks (row counts, checksums) to verify the revert completed successfully
- [ ] **API endpoints** — `POST /reverts` (create job), `GET /reverts/:id` (status), `GET /reverts/:id/preview` (dry-run), `DELETE /reverts/:id` (cancel)
- [ ] **CLI tooling** — Add a `trail-replay revert` CLI subcommand to submit and monitor revert jobs without the API
