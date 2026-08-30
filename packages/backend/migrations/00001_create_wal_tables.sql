-- +goose Up
CREATE TABLE wal_transaction (
    id                bigserial primary key,
    source_slot       text not null,
    source_db         text not null,
    xid               bigint not null,
    begin_lsn         pg_lsn,
    commit_lsn        pg_lsn not null,
    end_lsn           pg_lsn,
    commit_ts         timestamptz not null,
    origin_name       text,
    tenant_id         text,
    change_count      integer not null default 0,
    forward_sql_text  text,
    reverse_sql_text  text,
    forward_sql_hash  bytea,
    reverse_sql_hash  bytea,
    safety_flags      text[] not null default '{}',
    ingest_batch_id   uuid,
    ingested_at       timestamptz not null default now(),
    unique (source_slot, commit_lsn),
    unique (source_slot, xid, commit_lsn)
);

CREATE TABLE wal_change (
    id                    bigserial primary key,
    transaction_id        bigint not null references wal_transaction(id) on delete cascade,
    change_seq_in_txn     integer not null,
    change_lsn            pg_lsn,
    tenant_id             text,
    schema_name           text not null,
    table_name            text not null,
    table_oid             oid,
    op                    char(1) not null check (op in ('I','U','D')),
    replica_identity_mode text,
    row_pk                jsonb,
    old_row               jsonb,
    new_row               jsonb,
    changed_columns       text[] not null default '{}',
    forward_dml_sql       text not null,
    reverse_dml_sql       text not null,
    forward_sql_hash      bytea,
    reverse_sql_hash      bytea,
    reverse_where_sql     text,
    affected_row_count    integer not null default 1,
    undo_status           text not null default 'not_applied',
    undo_applied_at       timestamptz,
    undo_applied_by       text,
    safety_flags          text[] not null default '{}',
    created_at            timestamptz not null default now(),
    unique (transaction_id, change_seq_in_txn)
);

-- +goose Down
DROP TABLE IF EXISTS wal_change;
DROP TABLE IF EXISTS wal_transaction;
