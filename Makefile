.PHONY: help build run test clean docker-build docker-run migrate

APP_NAME=cargo-tracking-ingestion
VERSION=1.0.0
GO_VERSION=1.24.0

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

deps:
	go mod download
	go mod tidy

build:
	CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o bin/$(APP_NAME) cmd/server/main.go

run:
	go run cmd/server/main.go

test:
	go test -v -race -coverprofile=coverage.out ./...

test-coverage: test
	go tool cover -html=coverage.out

clean:
	rm -rf bin/
	rm -f coverage.out

migrate-up:
	psql -h localhost -U postgres -d cargo_tracking -f migrations/000_extensions.sql
	psql -h localhost -U postgres -d cargo_tracking -f migrations/001_create_device_telemetry.sql
	psql -h localhost -U postgres -d cargo_tracking -f migrations/002_create_device_events.sql
	psql -h localhost -U postgres -d cargo_tracking -f migrations/003_create_shipment_tracking.sql
	psql -h localhost -U postgres -d cargo_tracking -f migrations/004_create_device_heartbeats.sql

docker-build:
	docker build -t $(APP_NAME):$(VERSION) .

docker-run:
	docker run -p 8080:8080 --env-file .env $(APP_NAME):$(VERSION)

docker-compose-up:
	docker-compose up -d

docker-compose-down:
	docker-compose down

dev:
	air