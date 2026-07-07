# Trail Replay — Architecture

This project follows **Hexagonal Architecture** (Ports & Adapters), keeping business logic isolated from infrastructure concerns.

---

## Directory Structure

```
trail-replay/
├── cmd/
│   └── api/
│       └── main.go                          # Entry point — wires everything together
│
├── internal/
│   ├── core/                                # Business logic — no infra dependencies
│   │   ├── domain/
│   │   │   └── trail.go                     # Pure domain entities (Trail, Event)
│   │   ├── ports/
│   │   │   ├── inbound/
│   │   │   │   └── trail_service.go         # Driving port — interface the app exposes
│   │   │   └── outbound/
│   │   │       └── trail_repository.go      # Driven port — interface the core needs
│   │   └── services/
│   │       └── trail_service.go             # Business logic, depends only on ports
│   │
│   └── adapters/                            # Infra implementations of ports
│       ├── inbound/
│       │   └── http/
│       │       └── handler.go               # HTTP driving adapter (Go 1.22 routing)
│       └── outbound/
│           └── storage/
│               └── memory_repository.go     # In-memory driven adapter
│
└── pkg/
    └── config/
        └── config.go                        # Shared config (env-based)
```

---

## Hexagonal Diagram

```
         ┌─────────────────────────────────────────┐
         │                  CORE                   │
         │                                         │
 HTTP ──►│  inbound port        outbound port      │──► Storage
 CLI ───►│  (TrailService)  →   (TrailRepository)  │──► External API
 gRPC ──►│                                         │──► Message Queue
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
| `NewTrailService` returns `inbound.TrailService` | Callers always program-to-interface, never to the concrete struct |
| Swap storage with one line | Implement `outbound.TrailRepository`, update the wire-up in `main.go` |
| Tests use real in-memory adapter | No mocks needed — adapters are cheap; avoids mock/prod divergence |

---

## Dependency Rules

```
cmd/api  →  adapters  →  core/ports  →  core/domain
                      →  core/services
```

- Dependencies always point **inward** toward the domain.
- The `core/` package never imports from `adapters/`.
- `pkg/` is shared infrastructure with no business logic.
