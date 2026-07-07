-- Database initialization script for trail-replay service

-- Enable logical replication (required for WAL logical decoding)
ALTER SYSTEM SET wal_level = 'logical';
ALTER SYSTEM SET max_replication_slots = 10;
ALTER SYSTEM SET max_wal_senders = 10;

-- Note: In Docker, these settings are applied through postgresql.conf or environment variables
-- The above ALTER SYSTEM commands will take effect after restart

-- Create trails table
CREATE TABLE IF NOT EXISTS trails (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Create events table
CREATE TABLE IF NOT EXISTS events (
    id VARCHAR(255) PRIMARY KEY,
    trail_id VARCHAR(255) NOT NULL REFERENCES trails(id) ON DELETE CASCADE,
    type VARCHAR(50) NOT NULL,
    payload JSONB,
    occured_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    sequence BIGINT NOT NULL,
    UNIQUE(trail_id, sequence)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_events_trail_id ON events(trail_id);
CREATE INDEX IF NOT EXISTS idx_events_trail_id_sequence ON events(trail_id, sequence);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(type);
CREATE INDEX IF NOT EXISTS idx_trails_created_at ON trails(created_at);

-- Create a function to automatically update the updated_at column
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger to automatically update updated_at on trails table
DROP TRIGGER IF EXISTS update_trails_updated_at ON trails;
CREATE TRIGGER update_trails_updated_at
    BEFORE UPDATE ON trails
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Create separate database for WAL change log storage (prevents recursion)
CREATE DATABASE trailwal;