.PHONY: build test test-race vet lint fmt cover migrate-status migrate-up

DB ?= batch-jobs.db

build:
	go build ./...

test:
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./...

fmt:
	gofmt -l .

cover:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

# Schema tooling. Override the database with DB=path, e.g.
#   make migrate-status DB=/var/lib/batch-jobs/jobs.db
migrate-status:
	go run ./cmd/batch-jobs-migrate -db $(DB) status

migrate-up:
	go run ./cmd/batch-jobs-migrate -db $(DB) up
