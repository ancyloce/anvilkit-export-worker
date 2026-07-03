GO ?= go

.PHONY: all build test vet lint audit

all: vet test build

build:
	$(GO) build ./...

test:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

lint:
	golangci-lint run

audit:
	./scripts/dependency-audit.sh

# Contract bindings under internal/contracts/ are generated from the platform
# repo (anvilkit-platform): bun packages/contracts-codegen/generate.ts
