# Trail Replay — Architecture

This project follows **Hexagonal Architecture** (Ports & Adapters), keeping business logic isolated from infrastructure concerns.

---

## Directory Structure

```
trail-replay/                                  # npm workspaces monorepo
├── packages/
│   ├── backend/                               # Go module (module name: trail-replay)
│   │   ├── cmd/
│   │   │   ├── api/main.go                    # API entry point — wires everything together
│   │   │   ├── migrate/main.go                # goose migrations (-wal for the WAL database)
│   │   │   └── stream-process/                # WAL logical-replication streamer
│   │   │
│   │   ├── internal/
│   │   │   ├── core/trail/                    # Business logic — no infra dependencies
│   │   │   │   ├── domain/                    # Pure entities: wal, revert, user, source_database
│   │   │   │   ├── ports/
│   │   │   │   │   ├── inbound/               # Driving ports (auth, wal, revert, source_db services)
│   │   │   │   │   └── outbound/              # Driven ports (repositories, source_db executor)
│   │   │   │   └── services/                  # Business logic, depends only on ports
│   │   │   │
│   │   │   └── adapters/                      # Infra implementations of ports
│   │   │       ├── inbound/
│   │   │       │   └── http/                  # handlers (wal, revert, auth, database) + JWT/CORS middleware
│   │   │       └── outbound/
│   │   │           └── storage/               # postgres/ repositories + in-memory user/source-db repos
│   │   │
│   │   ├── pkg/                               # config, crypto (AES-GCM), database helpers
│   │   └── migrations/                        # goose SQL migrations
│   │
│   └── frontend/                              # React + Vite SPA (HeroUI, React Router)
│
├── Makefile                                   # Root targets cd into packages/backend
└── docker-compose.yml                         # Backend build context: ./packages/backend
```

---

## Hexagonal Diagram

```
         ┌─────────────────────────────────────────┐
         │                  CORE                   │
         │                                         │
 HTTP ──►│  inbound port        outbound port      │──► Storage
 CLI ───►│ (WalQueryService) → (WalQueryRepository)│──► External API
 gRPC ──►│ (RevertService)    (RevertRepository)   │──► Message Queue
         │         domain / services               │
         └─────────────────────────────────────────┘
           ▲ driving adapters       driven adapters ▲
           │  (inbound/http)    (outbound/storage)  │
```

---

## Key Design Decisions

| Decision | Rationale |
|---|---|
| `core/` has zero knowledge of adapters | Imports only `domain` and its own `ports` — never adapter packages |
| Constructors return port interfaces | e.g. `NewAuthService` returns `inbound.AuthService` — callers program-to-interface |
| Swap storage with one line | Implement the outbound port, update the wire-up in `main.go` |
| Tests use real in-memory adapters | No mocks needed — adapters are cheap; avoids mock/prod divergence |

---

## Dependency Rules

```
cmd/api  →  adapters  →  core/ports  →  core/domain
                      →  core/services
```

- Dependencies always point **inward** toward the domain.
- The `core/` package never imports from `adapters/`.
- `pkg/` is shared infrastructure with no business logic.
