.PHONY: build test test-race vet lint fmt cover bench bench-queue load migrate-status migrate-up

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

# Benchmarks. Run these on an otherwise idle machine and compare with
# benchstat rather than reading a single run's numbers as fact:
#   make bench > new.txt && benchstat old.txt new.txt
bench:
	go test -run '^$$' -bench . -benchmem ./...

# The queue benchmarks alone, which are the ones worth watching: the
# claim-under-backlog family is what caught dequeue costing time
# proportional to the whole backlog.
bench-queue:
	go test -run '^$$' -bench . -benchtime $(BENCHTIME) ./internal/queue/...

BENCHTIME ?= 200x

# A load test through the whole engine -- producers, workers, and the
# database together -- rather than one operation in isolation. Override
# any of the flags, e.g.
#   make load LOAD_ARGS="-rate 2000 -duration 1m -workers 16 -work 5ms"
LOAD_ARGS ?= -rate 500 -duration 15s -workers 8 -work 5ms

load:
	go run ./cmd/batch-jobs-bench $(LOAD_ARGS)

# Schema tooling. Override the database with DB=path, e.g.
#   make migrate-status DB=/var/lib/batch-jobs/jobs.db
migrate-status:
	go run ./cmd/batch-jobs-migrate -db $(DB) status

migrate-up:
	go run ./cmd/batch-jobs-migrate -db $(DB) up
