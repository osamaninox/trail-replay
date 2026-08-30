# Prior knowledge: client-side replication mechanics solid; slot semantics are the gap

From code evidence in `cmd/stream-process/postgres_stream_process.go`, the user has already
implemented (and completed TODO items for): an LSN checkpoint table with load/save, standby
status updates, pgoutput streaming, and idempotent slot creation ("already exists, continuing").
So: LSN-as-bookmark, WAL streaming, and Go postgres plumbing need no re-teaching.

The identified gap — and the zone of proximal development for lesson 0001 — is *server-side*
slot semantics: the slot as WAL retention anchor, and why a client-side checkpoint cannot
substitute for it (evidenced by `cleanup()` dropping the slot on shutdown).

Implications: future sessions can skip LSN/checkpoint basics and go straight to the other
retention-side topics in TODO (max_slot_wal_keep_size, heartbeats, slot monitoring), which all
build on the same "slot = server-side anchor" mental model.
