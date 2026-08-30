-- +goose Up
CREATE TABLE revert_job (
    id              uuid primary key,
    status          text not null default 'pending'
                    check (status in ('pending','in_progress','completed','failed','cancelling','cancelled')),
    input_from      timestamptz not null,
    input_to        timestamptz not null,
    total_changes   int not null default 0,
    completed_count int not null default 0,
    failed_count    int not null default 0,
    last_error      text,
    created_at      timestamptz not null default now(),
    updated_at      timestamptz not null default now(),
    completed_at    timestamptz
);

CREATE TABLE revert_job_change (
    job_id        uuid not null references revert_job(id) on delete cascade,
    change_id     bigint not null references wal_change(id),
    status        text not null default 'pending'
                  check (status in ('pending','applied','failed','skipped')),
    error_message text,
    applied_at    timestamptz,
    primary key (job_id, change_id)
);

-- +goose Down
DROP TABLE IF EXISTS revert_job_change;
DROP TABLE IF EXISTS revert_job;
