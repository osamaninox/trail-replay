.PHONY: api stream migrate test test-unit

## Build and run the API server
api:
	cd packages/backend && go run ./cmd/api

## Stream PostgreSQL WAL changes via logical replication
stream:
	cd packages/backend && go run ./cmd/stream-process

## Run database migrations
migrate:
	cd packages/backend && go run ./cmd/migrate

## Run all tests
test:
	cd packages/backend && go test ./...

## Run tests excluding integration tests that need Docker
test-unit:
	cd packages/backend && go test -short ./...
