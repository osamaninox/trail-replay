-- +goose Up
CREATE TABLE source_databases (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id    UUID NOT NULL,
    name       VARCHAR(255) NOT NULL,
    host       VARCHAR(255) NOT NULL,
    port       INTEGER NOT NULL,
    dbname     VARCHAR(255) NOT NULL,
    username   VARCHAR(255) NOT NULL,
    password   TEXT NOT NULL,
    sslmode    VARCHAR(20) NOT NULL DEFAULT 'disable',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- +goose Down
DROP TABLE IF EXISTS source_databases;
