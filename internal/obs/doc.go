// Package obs owns observability and lifecycle: structured JSON logging with
// the required job-scoped fields (PRD 0010 §15.1) and secret redaction
// (EW-CONFIG-005), Prometheus metrics in the anvilkit_export_worker_*
// namespace (ADR-015; M2 partial baseline), the internal-only
// health/readiness/metrics endpoints on ports 8081/9091 (FR-018), and the
// worker lifecycle state reflected by the readiness probe.
//
// Still to land in M4 (EW-OBS-002 final, EW-OBS-003, EW-OBS-005..007): the
// full metric baseline, OpenTelemetry per-job spans with context forwarded
// to render-origin, SIGTERM graceful-drain hardening, and alert rules.
package obs
