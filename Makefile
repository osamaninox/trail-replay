.PHONY: api stream migrate test

## Build and run the API server
api:
	go run ./cmd/api

## Stream PostgreSQL WAL changes via logical replication
stream:
	go run ./cmd/stream-process

## Run database migrations
migrate:
	go run ./cmd/migrate

## Run all tests
# Default: uses testcontainers (requires Docker running)
test:
	go test ./...

## Run tests excluding integration tests that need Docker
test-unit:
	go test -short ./...
