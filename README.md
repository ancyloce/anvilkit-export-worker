# anvilkit-export-worker

Stateless, queue-driven Go worker that owns the AnvilKit **export stage**: consume
`deployment.export.requested`, load the authoritative deployment record, acquire a
per-`deploymentId` lock, CAS `EXPORT_QUEUED → EXPORTING`, fetch version-pinned HTML from
`anvilkit-render-origin` over internal HTTP, harvest dependencies deterministically, upload
hashed artifacts plus `artifact-manifest.json` to S3-compatible storage, submit the manifest
pointer, CAS `EXPORTING → ARTIFACT_READY | EXPORT_FAILED`, and emit
`deployment.artifact.ready` / `deployment.export.failed`.

Canonical naming (ADR-015): this service is `anvilkit-export-worker` on every surface —
repo, `services/export-worker` submodule path, `export-worker` image/Deployment/consumer
group, `anvilkit_export_worker_*` metrics. The PRDs' `anvilkit-render-worker` and the old
`anvilkit-static-publisher` repo name refer to this same service.

## Status

**Milestone 3 runtime** (PLAN-0001 §5 M3): the full export pipeline is implemented behind
the `Exporter` seam — version-pinned render fetch with all pinning headers and response
classification, FR-008 output guards, deterministic dependency harvesting with CSS
recursion and allowlists, traversal-guarded path mapping, SHA-256-metadata-idempotent
uploads (multipart > 16 MB, never ETag), the schema-self-validated internal-only manifest,
pointer submission (BD-004 interim semantics), and CAS-then-emit outcome events validated
against the frozen contracts. On top of the M2 foundation: five-mechanism queue model,
distributed lock, CAS flow, fail-fast config with the demo guard.

Milestone 4 hardening still to come: full FR-015 idempotent redelivery (manifest-existence
check + ready-event re-emit), OTel spans, complete metric baseline, SIGTERM drain tests,
alert rules, failure-injection suite.

## Layout

```
cmd/export-worker/        entrypoint: config, wiring, consumer pool, dispatcher,
                          reclaim loop, SIGTERM drain skeleton
contracts/                GENERATED from anvilkit-platform contracts/ — do not edit.
                          events, deployment-service + asset-service bindings, and
                          the artifact-manifest bindings. Public on purpose: the
                          platform mocks (and future Go consumers) import them.
internal/buildinfo/       service identity + version stamp (ADR-015)
internal/config/          §14 env config, fail-fast validation, demo guard (FR-019)
internal/errclass/        §13 error registry classification (FR-014)
internal/jsonschema/      contract-subset JSON Schema validator (shared)
internal/obs/             logging + redaction, metrics, health/readyz, lifecycle
internal/queue/           driver seam + Redis Streams driver, validation, DLQ,
                          delayed retry, dispatcher, outcome streams (ADR-003)
internal/lock/            per-deployment lock: SET NX PX, heartbeat, owner-checked
                          release (FR-005)
internal/deployment/      record load + reconciliation, CAS + pointer submission
internal/render/          render-origin client: pinning headers, classification,
                          timeout budget, preview path (FR-007/FR-024)
internal/harvest/         output guards, HTML/CSS parsers, allowlists, path
                          normalization + traversal corpus (FR-008/009/010)
internal/storage/         S3/MinIO adapter, hash idempotency, multipart, manifest
                          builder + cache-control classes (FR-011/012)
internal/emit/            schema-validated outcome-event emission (FR-013)
internal/export/          the Exporter pipeline tying it all together
internal/worker/          job processor: every ack decision + failure branches
internal/testsupport/     integration-test infrastructure (never in the binary)
scripts/dependency-audit.sh   forbidden-dependency gate (AC-002, AC-018)
```

## Commands

```bash
make build          # go build ./...
make test           # go test -race ./...   (integration tests skip without the env below)
make vet            # go vet ./...
make lint           # golangci-lint run
make audit          # dependency audit

# Integration tests against disposable containers:
docker run -d --name test-redis -p 16379:6379 redis:7-alpine
docker run -d --name test-minio -p 19000:9000 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio:latest server /data
REDIS_TEST_URL=redis://localhost:16379 S3_TEST_ENDPOINT=http://localhost:19000 \
  go test -race -count=1 ./...

# Container image (EW-K8S-001):
docker build -t anvilkit-export-worker:dev .
```

The full local stack (Redis, MinIO, all three contract mocks, worker) lives in the platform
repo: `anvilkit-platform/infra/docker-compose.yml` — see `infra/README.md` there for the
happy-path, negative, and multipart scenarios plus artifact verification.

## Boundary rules (hard)

- **No cross-repo source imports** with `anvilkit-studio`, in either direction — integration
  is contract-only (JSON Schema events, OpenAPI internal APIs, versioned in
  `anvilkit-platform/contracts/`). Render output is consumed over HTTP, never via render code.
- Never depend on React, Next.js, Puck, `@anvilkit/render-runtime`, or any `@anvilkit/*`
  frontend package (CI-enforced).
- External services (`deployment-service`, `asset-service`, `cdn-service`, …) are contracts
  and mocks only; the worker is stateless and never touches their databases.
- No CDN upload/purge/verify/activation code paths — delivery beyond artifact storage is
  `cdn-service`'s stage (AC-017). The manifest is internal-only, never public.
- `apps/demo` is never a render target outside local development (startup guard, ADR-010).
- The token and storage credentials never appear in logs (redaction-enforced, §11.1).
- **S3 ETag is never a content hash** — idempotency compares
  `x-amz-meta-content-sha256` only (AC-006).

## Generated contracts

`contracts/` is generated by the platform repo's codegen pipeline
(`bun packages/contracts-codegen/generate.ts` in `anvilkit-platform`) and committed here so
this repo builds standalone. Platform CI regenerates and fails on drift; edit the contract
files in `anvilkit-platform/contracts/`, never the generated Go.
