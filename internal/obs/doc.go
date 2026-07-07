// Package obs owns observability and lifecycle: structured JSON logging with
// the required job-scoped fields (PRD 0010 §15.1) and secret redaction
// (EW-CONFIG-005), the full Prometheus metric baseline in the
// anvilkit_export_worker_* namespace (ADR-015, EW-OBS-002) including the
// §15.4 alert feeds, OpenTelemetry per-job spans (§15.3 vocabulary) with
// trace context forwarded to render-origin (EW-OBS-003), the internal-only
// health/readiness/metrics endpoints on ports 8081/9091 (FR-018), and the
// worker lifecycle state reflected by the readiness probe — including the
// DRAINING state the SIGTERM graceful drain flips through (FR-017).
package obs
