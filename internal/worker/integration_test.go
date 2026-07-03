// End-to-end M2 harness test against real Redis (skipped without
// REDIS_TEST_URL) and an in-process deployment-service: the full
// consume → validate → load → lock → CAS → fail → delayed-retry →
// re-dispatch loop, proving the AC-033 four-execution semantics and the
// AC-021 mechanism interplay with the real driver, retry store, and
// dispatcher.
package worker_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/ancyloce/anvilkit-export-worker/contracts/deploymentservice"
	"github.com/ancyloce/anvilkit-export-worker/contracts/events"
	"github.com/ancyloce/anvilkit-export-worker/internal/deployment"
	"github.com/ancyloce/anvilkit-export-worker/internal/errclass"
	"github.com/ancyloce/anvilkit-export-worker/internal/lock"
	"github.com/ancyloce/anvilkit-export-worker/internal/queue"
	"github.com/ancyloce/anvilkit-export-worker/internal/testsupport"
	"github.com/ancyloce/anvilkit-export-worker/internal/worker"
)

// testDeploymentService is a minimal in-process deployment-service with CAS
// semantics (the full contract-conformant mock lives in the platform repo's
// mocks module).
type testDeploymentService struct {
	mu       sync.Mutex
	rec      deploymentservice.DeploymentRecord
	pointers []deploymentservice.ArtifactPointer
}

func (s *testDeploymentService) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/deployments/{id}", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		_ = json.NewEncoder(w).Encode(s.rec)
	})
	mux.HandleFunc("POST /internal/deployments/{id}/artifact", func(w http.ResponseWriter, r *http.Request) {
		var pointer deploymentservice.ArtifactPointer
		if err := json.NewDecoder(r.Body).Decode(&pointer); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		s.pointers = append(s.pointers, pointer)
		s.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("PATCH /internal/deployments/{id}/status", func(w http.ResponseWriter, r *http.Request) {
		var body deploymentservice.StatusUpdateRequest
		_ = json.NewDecoder(r.Body).Decode(&body)
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.rec.Status != body.From {
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(deploymentservice.StatusConflictError{
				ErrorCode: "STATUS_CONFLICT", CurrentStatus: s.rec.Status,
			})
			return
		}
		s.rec.Status = body.To
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

func (s *testDeploymentService) status() deploymentservice.DeploymentStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rec.Status
}

type alwaysRetryableExporter struct{ executions int }

func (e *alwaysRetryableExporter) Export(context.Context, *worker.Job) error {
	e.executions++
	return errclass.New(events.ErrorCodeStorage5xx, events.FailedStageUploadArtifacts,
		errors.New("injected storage outage"))
}

// TestFourExecutionSemanticsEndToEnd drives one deployment through the real
// queue machinery with an always-retryable failure: exactly four executions
// (attempt 0..3), three retry envelopes, one DLQ entry with attempt 3, CAS
// to EXPORT_FAILED, retry structures empty, nothing left pending.
func TestFourExecutionSemanticsEndToEnd(t *testing.T) {
	rdb := testsupport.Redis(t, 3)
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	svc := &testDeploymentService{rec: deploymentservice.DeploymentRecord{
		DeploymentID: "dep_01", TeamID: "team_01", SiteID: "site_01",
		PageID: "page_01", Slug: "home", Version: "v12",
		Status:     deploymentservice.DeploymentStatusExportQueued,
		RenderMode: "fetch_route", TargetID: "target_platform_prod", Environment: "production",
	}}
	srv := httptest.NewServer(svc.handler())
	defer srv.Close()

	driver := queue.NewRedisDriver(rdb, "e2e-worker")
	if err := driver.EnsureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	store := queue.NewRedisRetryStore(rdb)
	exporter := &alwaysRetryableExporter{}
	proc := worker.New(worker.Deps{
		Consumer: driver,
		DLQ:      driver,
		Retries:  store,
		Locker:   worker.LocksFrom(lock.NewLocker(rdb, "e2e-worker", 90*time.Second)),
		Deploy:   deployment.New(srv.URL, "test-token", 5*time.Second),
		Exporter: exporter,
		Log:      log,
		WorkerID: "e2e-worker",
		Backoff:  queue.BackoffPolicy{Base: time.Millisecond, Max: 2 * time.Millisecond, Rand: func() float64 { return 0 }},
	})
	dispatcher := &queue.Dispatcher{Store: store, Pub: driver, Log: log}

	payload, err := json.Marshal(map[string]any{
		"eventId": "evt_e2e", "eventType": "deployment.export.requested",
		"deploymentId": "dep_01", "teamId": "team_01", "siteId": "site_01",
		"pageId": "page_01", "slug": "home", "version": "v12",
		"renderMode": "fetch_route", "targetId": "target_platform_prod",
		"environment": "production", "idempotencyKey": "dep_01",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Publish(ctx, queue.OutgoingMessage{Payload: payload}); err != nil {
		t.Fatal(err)
	}

	var outcomes []worker.Outcome
	var attempts []int
	deadline := time.Now().Add(30 * time.Second)
	for len(outcomes) == 0 || outcomes[len(outcomes)-1] != worker.OutcomeDLQ {
		if time.Now().After(deadline) {
			t.Fatalf("timed out; outcomes=%v attempts=%v", outcomes, attempts)
		}
		msgs, err := driver.Fetch(ctx, 1, 200*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range msgs {
			attempts = append(attempts, m.Attempt)
			outcomes = append(outcomes, proc.Handle(ctx, m))
		}
		// Force any scheduled envelope due (backoff is ~1ms anyway).
		dispatcher.Tick(ctx, time.Now().Add(time.Second))
	}

	if exporter.executions != 4 {
		t.Errorf("executions = %d, want 4 (one initial + three retries, AC-033)", exporter.executions)
	}
	if !reflect.DeepEqual(attempts, []int{0, 1, 2, 3}) {
		t.Errorf("attempts = %v, want [0 1 2 3]", attempts)
	}
	for i, o := range outcomes[:len(outcomes)-1] {
		if o != worker.OutcomeRetryScheduled {
			t.Errorf("outcome[%d] = %s, want retry_scheduled", i, o)
		}
	}

	if got := svc.status(); got != deploymentservice.DeploymentStatusExportFailed {
		t.Errorf("record status = %s, want EXPORT_FAILED", got)
	}
	dlq, err := rdb.XRange(ctx, queue.StreamDLQ, "-", "+").Result()
	if err != nil || len(dlq) != 1 {
		t.Fatalf("dlq entries = %d, err %v", len(dlq), err)
	}
	if got := dlq[0].Values["attempt"]; got != "3" {
		t.Errorf("dlq attempt = %v, want 3", got)
	}
	if got := dlq[0].Values["errorCode"]; got != "STORAGE_5XX" {
		t.Errorf("dlq errorCode = %v", got)
	}
	if n, _ := rdb.HLen(ctx, queue.KeyRetryPayloads).Result(); n != 0 {
		t.Errorf("retry payloads left = %d, want 0", n)
	}
	if n, _ := rdb.ZCard(ctx, queue.KeyRetryZSet).Result(); n != 0 {
		t.Errorf("retry index members left = %d, want 0", n)
	}
	if n, _ := driver.PendingCount(ctx); n != 0 {
		t.Errorf("pending after completion = %d, want 0", n)
	}
}

// TestUnparseableEndToEnd: garbage on the wire lands in the DLQ with an ack
// (handoff-then-ack) and never touches the deployment record.
func TestUnparseableEndToEnd(t *testing.T) {
	rdb := testsupport.Redis(t, 3)
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	var recordTouched bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recordTouched = true
		http.NotFound(w, r)
	}))
	defer srv.Close()

	driver := queue.NewRedisDriver(rdb, "e2e-worker")
	if err := driver.EnsureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	proc := worker.New(worker.Deps{
		Consumer: driver, DLQ: driver, Retries: queue.NewRedisRetryStore(rdb),
		Locker:   worker.LocksFrom(lock.NewLocker(rdb, "e2e-worker", 90*time.Second)),
		Deploy:   deployment.New(srv.URL, "test-token", 5*time.Second),
		Exporter: worker.Unimplemented{},
		Log:      log, WorkerID: "e2e-worker",
	})

	if _, err := driver.Publish(ctx, queue.OutgoingMessage{Payload: []byte(`this is not json`)}); err != nil {
		t.Fatal(err)
	}
	msgs, err := driver.Fetch(ctx, 1, 500*time.Millisecond)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("fetch: %v %v", msgs, err)
	}
	if out := proc.Handle(ctx, msgs[0]); out != worker.OutcomeDLQ {
		t.Fatalf("outcome = %s", out)
	}
	if recordTouched {
		t.Error("unparseable events must never call the deployment service")
	}
	if n, _ := driver.PendingCount(ctx); n != 0 {
		t.Errorf("pending = %d, want 0 (acked after DLQ handoff)", n)
	}
	dlq, _ := rdb.XRange(ctx, queue.StreamDLQ, "-", "+").Result()
	if len(dlq) != 1 {
		t.Fatalf("dlq entries = %d, want 1", len(dlq))
	}
}
