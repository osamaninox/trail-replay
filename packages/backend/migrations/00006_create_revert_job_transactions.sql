-- +goose Up
CREATE TABLE revert_job_transaction (
    job_id         uuid    not null,
    seq            integer not null,
    transaction_id bigint  not null,
    commit_lsn     pg_lsn  not null,
    change_count   integer not null,
    status         text not null default 'pending'
                   check (status in ('pending','in_progress','applied','failed','skipped')),
    error_message  text,
    applied_at     timestamptz,
    primary key (job_id, seq),
    unique (job_id, transaction_id)
);

-- Eligibility anti-join: "was this txn already reverted by any job?"
CREATE INDEX revert_job_transaction_applied_txn
    ON revert_job_transaction (transaction_id) WHERE status = 'applied';

-- Window scans for the plan step
CREATE INDEX wal_transaction_commit_ts ON wal_transaction (commit_ts);

-- +goose Down
DROP INDEX revert_job_transaction_applied_txn;
DROP INDEX wal_transaction_commit_ts;
DROP TABLE revert_job_transaction;
