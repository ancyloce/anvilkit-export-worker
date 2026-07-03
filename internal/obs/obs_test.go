package obs

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// TestJobLoggerRequiredFields asserts every §15.1 job-scoped field appears on
// a job log entry (AC-015 groundwork; asserted fully at M4).
func TestJobLoggerRequiredFields(t *testing.T) {
	var buf bytes.Buffer
	base := NewLogger(&buf, "info", "anvilkit-export-worker-test-1", nil)
	jl := JobLogger(base, JobFields{
		TraceID: "trace_01", EventID: "evt_01", DeploymentID: "dep_01",
		TeamID: "team_01", SiteID: "site_01", PageID: "page_01",
		Slug: "home", Version: "v12", Environment: "production",
		RenderMode: "fetch_route", Attempt: 0,
	})
	jl.Info("job started", "stage", "consume_job", "status", "EXPORTING", "durationMs", 12, "errorCode", "")

	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("log output is not JSON: %v\n%s", err, buf.String())
	}
	for _, field := range []string{
		"traceId", "eventId", "deploymentId", "teamId", "siteId", "pageId",
		"slug", "version", "environment", "renderMode", "workerId",
		"attempt", "stage", "status", "durationMs", "errorCode",
	} {
		if _, ok := entry[field]; !ok {
			t.Errorf("missing required log field %q (PRD 0010 §15.1): %s", field, buf.String())
		}
	}
}

// TestLoggerRedactsSecrets is the EW-CONFIG-005 groundwork test: token
// material never reaches log output — in messages, attrs, or errors.
func TestLoggerRedactsSecrets(t *testing.T) {
	var buf bytes.Buffer
	secret := "super-secret-token-42"
	l := NewLogger(&buf, "debug", "w1", []string{secret, "s3-secret-key-x"})

	l.Info("call failed with Authorization: Bearer "+secret, "detail", "token="+secret)
	l.Error("request error", "err", errors.New("401 from origin, sent "+secret))
	l.With("ctx", secret).Info("with-attr entry")

	out := buf.String()
	if strings.Contains(out, secret) {
		t.Fatalf("secret leaked into logs:\n%s", out)
	}
	if !strings.Contains(out, "[REDACTED]") {
		t.Fatalf("expected [REDACTED] markers:\n%s", out)
	}
}

func TestReadyzReflectsLifecycle(t *testing.T) {
	lc := &Lifecycle{}
	reg := prometheus.NewRegistry()
	srv := NewOpsServer(0, 0, lc, reg)
	health, metrics := srv.Handlers()

	get := func(h http.Handler, path string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		return rec
	}

	if rec := get(health, "/healthz"); rec.Code != http.StatusOK {
		t.Errorf("healthz = %d, want 200", rec.Code)
	}
	if rec := get(health, "/readyz"); rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "STARTING") {
		t.Errorf("readyz while STARTING = %d %q, want 503 STARTING", rec.Code, rec.Body.String())
	}
	lc.Set(StateReady)
	if rec := get(health, "/readyz"); rec.Code != http.StatusOK {
		t.Errorf("readyz while READY = %d, want 200", rec.Code)
	}
	lc.Set(StateDraining)
	if rec := get(health, "/readyz"); rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "DRAINING") {
		t.Errorf("readyz while DRAINING = %d %q, want 503 DRAINING (FR-018)", rec.Code, rec.Body.String())
	}

	if rec := get(metrics, "/metrics"); rec.Code != http.StatusOK {
		t.Errorf("metrics = %d, want 200", rec.Code)
	}
}

// TestMetricsNamespace pins the ADR-015 metric naming on the M2 set.
func TestMetricsNamespace(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	m.JobsTotal.Inc()
	m.DLQTotal.Inc()
	m.LockConflictTotal.Inc()
	m.RenderDurationMs.Observe(1)
	m.HarvestDurationMs.Observe(1)
	m.UploadDurationMs.Observe(1)
	m.ArtifactBytesTotal.Add(1)
	m.ArtifactFilesTotal.Add(1)
	m.UnparseableTotal.Inc()
	m.AuthFailuresTotal.WithLabelValues("RENDER_ORIGIN_401").Inc()

	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, f := range families {
		got[f.GetName()] = true
	}
	for _, want := range []string{
		"anvilkit_export_worker_jobs_total",
		"anvilkit_export_worker_jobs_success_total",
		"anvilkit_export_worker_jobs_failed_total",
		"anvilkit_export_worker_job_duration_ms",
		"anvilkit_export_worker_render_duration_ms",
		"anvilkit_export_worker_dependency_harvest_duration_ms",
		"anvilkit_export_worker_upload_duration_ms",
		"anvilkit_export_worker_artifact_bytes_total",
		"anvilkit_export_worker_artifact_files_total",
		"anvilkit_export_worker_unparseable_events_total",
		"anvilkit_export_worker_auth_failures_total",
		"anvilkit_export_worker_retry_total",
		"anvilkit_export_worker_dlq_total",
		"anvilkit_export_worker_lock_conflict_total",
		"anvilkit_export_worker_queue_pending",
		"anvilkit_export_worker_retry_dispatch_lag_ms",
	} {
		if !got[want] {
			t.Errorf("missing metric %s (ADR-015 namespace)", want)
		}
	}
	for name := range got {
		if strings.HasPrefix(name, "render_worker_") {
			t.Errorf("superseded render_worker_* metric name found: %s (ADR-015)", name)
		}
	}
}
