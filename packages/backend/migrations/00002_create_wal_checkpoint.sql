-- +goose Up
CREATE TABLE wal_checkpoint (
    slot_name  text PRIMARY KEY,
    last_lsn   pg_lsn NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE IF EXISTS wal_checkpoint;
