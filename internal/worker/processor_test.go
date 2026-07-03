// Processor branch tests with injected fakes: every ack/no-ack decision of
// the ADR-003 ack rule, the FR-014 retry/DLQ branches, FR-006 stop-safe CAS,
// and the AC-027/AC-028/AC-033 invariants at the unit level (the same
// invariants are re-proven against real Redis in integration_test.go).
package worker_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ancyloce/anvilkit-export-worker/contracts/deploymentservice"
	"github.com/ancyloce/anvilkit-export-worker/contracts/events"
	"github.com/ancyloce/anvilkit-export-worker/internal/errclass"
	"github.com/ancyloce/anvilkit-export-worker/internal/lock"
	"github.com/ancyloce/anvilkit-export-worker/internal/queue"
	"github.com/ancyloce/anvilkit-export-worker/internal/worker"
)

// --- fakes -----------------------------------------------------------------

// seq records the order of side effects so write-then-ack ordering is
// assertable.
type seq struct {
	mu     sync.Mutex
	events []string
}

func (s *seq) add(e string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e)
}

func (s *seq) list() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.events...)
}

type fakeConsumer struct {
	seq    *seq
	ackErr error
	acked  []string
}

func (f *fakeConsumer) Fetch(context.Context, int, time.Duration) ([]queue.Message, error) {
	return nil, nil
}
func (f *fakeConsumer) Ack(_ context.Context, ids ...string) error {
	if f.ackErr != nil {
		return f.ackErr
	}
	f.acked = append(f.acked, ids...)
	f.seq.add("ack")
	return nil
}
func (f *fakeConsumer) Reclaim(context.Context, time.Duration, int) ([]queue.Message, error) {
	return nil, nil
}
func (f *fakeConsumer) PendingCount(context.Context) (int64, error) { return 0, nil }

type fakeDLQ struct {
	seq     *seq
	err     error
	entries []queue.DLQEntry
}

func (f *fakeDLQ) SendToDLQ(_ context.Context, e queue.DLQEntry) error {
	if f.err != nil {
		return f.err
	}
	f.entries = append(f.entries, e)
	f.seq.add("dlq")
	return nil
}

type fakeRetries struct {
	seq  *seq
	err  error
	envs map[string]queue.RetryEnvelope
}

func (f *fakeRetries) Schedule(_ context.Context, env queue.RetryEnvelope) error {
	if f.err != nil {
		return f.err
	}
	if f.envs == nil {
		f.envs = map[string]queue.RetryEnvelope{}
	}
	f.envs[env.RetryEnvelopeID] = env
	f.seq.add("schedule")
	return nil
}
func (f *fakeRetries) Due(context.Context, time.Time, int) ([]queue.RetryEnvelope, error) {
	return nil, nil
}
func (f *fakeRetries) Remove(context.Context, string) error { return nil }
func (f *fakeRetries) OldestDueLag(context.Context, time.Time) (time.Duration, bool, error) {
	return 0, false, nil
}

type transitionCall struct {
	from, to deploymentservice.DeploymentStatus
	reason   string
	stage    events.FailedStage
}

type fakeDeploy struct {
	mu            sync.Mutex
	rec           *deploymentservice.DeploymentRecord
	loadErr       error
	loadCalls     int
	transitionErr error
	transitions   []transitionCall
}

func (f *fakeDeploy) Load(context.Context, string) (*deploymentservice.DeploymentRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loadCalls++
	if f.loadErr != nil {
		return nil, f.loadErr
	}
	cp := *f.rec
	return &cp, nil
}

func (f *fakeDeploy) Transition(_ context.Context, _ string,
	from, to deploymentservice.DeploymentStatus, reason, _ string, stage events.FailedStage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.transitions = append(f.transitions, transitionCall{from, to, reason, stage})
	if f.transitionErr != nil {
		return f.transitionErr
	}
	// Real CAS semantics (FR-006): a stale `from` is a 409, exactly like
	// the deployment-service contract — this caught a live bug once.
	if f.rec.Status != from {
		return &deploymentservice.StatusConflictError{ErrorCode: "STATUS_CONFLICT", CurrentStatus: f.rec.Status}
	}
	f.rec.Status = to
	return nil
}

type fakeLease struct {
	released bool
	lost     chan struct{}
}

func (f *fakeLease) Release(context.Context) error { f.released = true; return nil }
func (f *fakeLease) Lost() <-chan struct{}         { return f.lost }

type fakeLocks struct {
	conflict bool
	lease    *fakeLease
}

func (f *fakeLocks) Acquire(context.Context, string) (worker.Lease, error) {
	if f.conflict {
		return nil, lock.ErrConflict
	}
	f.lease = &fakeLease{lost: make(chan struct{})}
	return f.lease, nil
}

type fakeExporter struct {
	err   error
	calls int
}

func (f *fakeExporter) Export(context.Context, *worker.Job) error {
	f.calls++
	return f.err
}

// --- helpers -----------------------------------------------------------------

type harness struct {
	seq      *seq
	consumer *fakeConsumer
	dlq      *fakeDLQ
	retries  *fakeRetries
	deploy   *fakeDeploy
	locks    *fakeLocks
	exporter *fakeExporter
	proc     *worker.Processor
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	s := &seq{}
	h := &harness{
		seq:      s,
		consumer: &fakeConsumer{seq: s},
		dlq:      &fakeDLQ{seq: s},
		retries:  &fakeRetries{seq: s},
		deploy: &fakeDeploy{rec: &deploymentservice.DeploymentRecord{
			DeploymentID: "dep_01", TeamID: "team_01", SiteID: "site_01",
			PageID: "page_01", Slug: "home", Version: "v12",
			Status:     deploymentservice.DeploymentStatusExportQueued,
			RenderMode: "fetch_route", TargetID: "target_platform_prod",
			Environment: "production",
		}},
		locks:    &fakeLocks{},
		exporter: &fakeExporter{},
	}
	h.proc = worker.New(worker.Deps{
		Consumer: h.consumer,
		DLQ:      h.dlq,
		Retries:  h.retries,
		Locker:   h.locks,
		Deploy:   h.deploy,
		Exporter: h.exporter,
		Log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		WorkerID: "test-worker",
		Backoff:  queue.BackoffPolicy{Base: time.Millisecond, Max: 2 * time.Millisecond, Rand: func() float64 { return 0 }},
	})
	return h
}

func validPayload(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile("../../contracts/events/testdata/deployment.export.requested.json")
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func msgWith(payload []byte, attempt int) queue.Message {
	return queue.Message{ID: "1751400000000-0", Payload: payload, Attempt: attempt}
}

// --- tests -------------------------------------------------------------------

func TestSuccessPathCASesExportingAndAcks(t *testing.T) {
	h := newHarness(t)
	out := h.proc.Handle(context.Background(), msgWith(validPayload(t), 0))
	if out != worker.OutcomeSuccess {
		t.Fatalf("outcome = %s", out)
	}
	if len(h.deploy.transitions) != 1 ||
		h.deploy.transitions[0].from != deploymentservice.DeploymentStatusExportQueued ||
		h.deploy.transitions[0].to != deploymentservice.DeploymentStatusExporting ||
		h.deploy.transitions[0].reason != "worker_started" {
		t.Errorf("transitions = %+v", h.deploy.transitions)
	}
	if h.exporter.calls != 1 || len(h.consumer.acked) != 1 {
		t.Errorf("exporter calls = %d, acked = %v", h.exporter.calls, h.consumer.acked)
	}
	if h.locks.lease == nil || !h.locks.lease.released {
		t.Error("lock must be acquired and released")
	}
}

// AC-011 companion at the pipeline level: dry-run performs no status writes
// and no export, but validates/loads/locks and acks.
func TestDryRunNoStatusWritesNoExport(t *testing.T) {
	h := newHarness(t)
	h.proc = worker.New(worker.Deps{
		Consumer: h.consumer, DLQ: h.dlq, Retries: h.retries, Locker: h.locks,
		Deploy: h.deploy, Exporter: h.exporter, DryRun: true,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), WorkerID: "test-worker",
	})
	out := h.proc.Handle(context.Background(), msgWith(validPayload(t), 0))
	if out != worker.OutcomeDryRun {
		t.Fatalf("outcome = %s", out)
	}
	if len(h.deploy.transitions) != 0 {
		t.Errorf("dry-run must not write status: %+v", h.deploy.transitions)
	}
	if h.exporter.calls != 0 {
		t.Error("dry-run must not export")
	}
	if len(h.consumer.acked) != 1 {
		t.Error("dry-run must ack")
	}
}

// FR-015 initial: redelivery after completion acks without re-processing.
func TestNonActionableRedeliveryAcksWithoutWork(t *testing.T) {
	for _, status := range []deploymentservice.DeploymentStatus{
		deploymentservice.DeploymentStatusArtifactReady,
		deploymentservice.DeploymentStatusExportFailed,
		deploymentservice.DeploymentStatusCancelled,
		deploymentservice.DeploymentStatusActive,
	} {
		h := newHarness(t)
		h.deploy.rec.Status = status
		out := h.proc.Handle(context.Background(), msgWith(validPayload(t), 0))
		if out != worker.OutcomeAckedTerminal {
			t.Fatalf("status %s: outcome = %s", status, out)
		}
		if h.exporter.calls != 0 || len(h.deploy.transitions) != 0 {
			t.Errorf("status %s: worker must not act", status)
		}
		if len(h.consumer.acked) != 1 {
			t.Errorf("status %s: must ack", status)
		}
	}
}

// AC-007/AC-028 initial: a lock conflict on an active deployment NEVER acks.
func TestLockConflictNeverAcks(t *testing.T) {
	h := newHarness(t)
	h.locks.conflict = true
	out := h.proc.Handle(context.Background(), msgWith(validPayload(t), 0))
	if out != worker.OutcomeLockConflict {
		t.Fatalf("outcome = %s", out)
	}
	if len(h.consumer.acked) != 0 {
		t.Fatal("lock conflict must leave the message pending, never ack (AC-007)")
	}
	if len(h.deploy.transitions) != 0 || h.exporter.calls != 0 {
		t.Error("lock conflict must not process")
	}
}

// AC-012: 409 stop-safe — ack only when the conflicting status is
// terminal/non-actionable.
func TestCASConflictStopSafe(t *testing.T) {
	h := newHarness(t)
	h.deploy.transitionErr = &deploymentservice.StatusConflictError{
		ErrorCode: "STATUS_CONFLICT", CurrentStatus: deploymentservice.DeploymentStatusArtifactReady,
	}
	out := h.proc.Handle(context.Background(), msgWith(validPayload(t), 0))
	if out != worker.OutcomeStopSafe || len(h.consumer.acked) != 1 {
		t.Fatalf("conflict→ARTIFACT_READY: outcome=%s acked=%v (want stop_safe + ack)", out, h.consumer.acked)
	}
	if h.exporter.calls != 0 {
		t.Error("stop-safe must not continue the pipeline")
	}

	h2 := newHarness(t)
	h2.deploy.transitionErr = &deploymentservice.StatusConflictError{
		ErrorCode: "STATUS_CONFLICT", CurrentStatus: deploymentservice.DeploymentStatusExporting,
	}
	out2 := h2.proc.Handle(context.Background(), msgWith(validPayload(t), 0))
	if out2 != worker.OutcomeStopSafe || len(h2.consumer.acked) != 0 {
		t.Fatalf("conflict→EXPORTING: outcome=%s acked=%v (want stop_safe, NO ack)", out2, h2.consumer.acked)
	}
}

func TestReconcileMismatchFailsTerminally(t *testing.T) {
	h := newHarness(t)
	h.deploy.rec.Version = "v13" // event says v12
	out := h.proc.Handle(context.Background(), msgWith(validPayload(t), 0))
	if out != worker.OutcomeFailedTerminal {
		t.Fatalf("outcome = %s", out)
	}
	last := h.deploy.transitions[len(h.deploy.transitions)-1]
	if last.to != deploymentservice.DeploymentStatusExportFailed ||
		last.reason != string(events.ErrorCodeValidationFailed) {
		t.Errorf("terminal CAS = %+v", last)
	}
	if h.exporter.calls != 0 {
		t.Error("mismatch must fail before the export seam")
	}
	if len(h.consumer.acked) != 1 {
		t.Error("terminal failure must ack")
	}
}

// AC-026/write-then-ack: a retryable failure schedules the envelope BEFORE
// the ack, with the ADR-003 id derivation.
func TestRetryableSchedulesEnvelopeWriteThenAck(t *testing.T) {
	h := newHarness(t)
	h.exporter.err = errclass.New(events.ErrorCodeStorage5xx, events.FailedStageUploadArtifacts, errors.New("minio down"))
	out := h.proc.Handle(context.Background(), msgWith(validPayload(t), 0))
	if out != worker.OutcomeRetryScheduled {
		t.Fatalf("outcome = %s", out)
	}
	env, ok := h.retries.envs["dep_01:1:STORAGE_5XX"]
	if !ok {
		t.Fatalf("envelope id missing; envs = %v", h.retries.envs)
	}
	if env.Attempt != 1 || env.LastErrorCode != events.ErrorCodeStorage5xx {
		t.Errorf("envelope = %+v", env)
	}
	order := strings.Join(h.seq.list(), ",")
	if !strings.Contains(order, "schedule,ack") {
		t.Errorf("write-then-ack violated: %s", order)
	}
}

// AC-033: retryable failure at attempt 3 = exhaustion → DLQ + CAS
// EXPORT_FAILED; DLQ handoff precedes the ack.
func TestExhaustionAtAttemptThree(t *testing.T) {
	h := newHarness(t)
	h.exporter.err = errclass.New(events.ErrorCodeRenderOriginTimeout, events.FailedStageRenderHtml, errors.New("slow origin"))
	out := h.proc.Handle(context.Background(), msgWith(validPayload(t), 3))
	if out != worker.OutcomeDLQ {
		t.Fatalf("outcome = %s", out)
	}
	if len(h.retries.envs) != 0 {
		t.Error("attempt 3 must not schedule a fourth retry (four executions max)")
	}
	if len(h.dlq.entries) != 1 || h.dlq.entries[0].Attempt != 3 ||
		h.dlq.entries[0].ErrorCode != events.ErrorCodeRenderOriginTimeout ||
		h.dlq.entries[0].FailedStage != events.FailedStageRenderHtml {
		t.Errorf("dlq entries = %+v", h.dlq.entries)
	}
	last := h.deploy.transitions[len(h.deploy.transitions)-1]
	if last.to != deploymentservice.DeploymentStatusExportFailed {
		t.Errorf("exhaustion must CAS EXPORT_FAILED, got %+v", last)
	}
	order := strings.Join(h.seq.list(), ",")
	if !strings.Contains(order, "dlq,ack") {
		t.Errorf("DLQ-handoff-then-ack violated: %s", order)
	}
}

// AC-027 at the processor level: crash-before-ack (simulated by a failing
// Ack) followed by redelivery produces ONE envelope, not two.
func TestEnvelopeIdempotentAcrossCrashBeforeAck(t *testing.T) {
	h := newHarness(t)
	h.exporter.err = errclass.New(events.ErrorCodeStorage5xx, events.FailedStageUploadArtifacts, errors.New("down"))
	h.consumer.ackErr = errors.New("injected crash before ack")

	if out := h.proc.Handle(context.Background(), msgWith(validPayload(t), 0)); out != worker.OutcomeRetryScheduled {
		t.Fatalf("first handle outcome = %s", out)
	}
	// Reclaim redelivers the same message (attempt unchanged); ack works now.
	h.consumer.ackErr = nil
	if out := h.proc.Handle(context.Background(), msgWith(validPayload(t), 0)); out != worker.OutcomeRetryScheduled {
		t.Fatalf("second handle outcome = %s", out)
	}
	if len(h.retries.envs) != 1 {
		t.Fatalf("envelopes after crash+reclaim = %d, want 1 (AC-027)", len(h.retries.envs))
	}
}

// FR-003: unparseable input → DLQ handoff THEN ack; a failed handoff leaves
// the message pending; no status update is ever attempted.
func TestUnparseableDLQHandoffThenAck(t *testing.T) {
	h := newHarness(t)
	h.dlq.err = errors.New("dlq write refused")
	out := h.proc.Handle(context.Background(), msgWith([]byte(`{broken`), 0))
	if out != worker.OutcomeNoAck || len(h.consumer.acked) != 0 {
		t.Fatalf("failed DLQ handoff must not ack: outcome=%s acked=%v", out, h.consumer.acked)
	}

	h.dlq.err = nil
	out = h.proc.Handle(context.Background(), msgWith([]byte(`{broken`), 0))
	if out != worker.OutcomeDLQ || len(h.consumer.acked) != 1 {
		t.Fatalf("outcome=%s acked=%v", out, h.consumer.acked)
	}
	if h.deploy.loadCalls != 0 || len(h.deploy.transitions) != 0 {
		t.Fatal("unparseable events must never touch the deployment record")
	}
	order := strings.Join(h.seq.list(), ",")
	if !strings.Contains(order, "dlq,ack") {
		t.Errorf("handoff-then-ack violated: %s", order)
	}
}

// Schema-invalid but deploymentId extractable: terminal VALIDATION_FAILED
// without the DLQ (that route is for unparseable input only).
func TestSchemaInvalidFailsTerminallyWithoutDLQ(t *testing.T) {
	h := newHarness(t)
	payload := []byte(`{"deploymentId":"dep_01","eventType":"nope"}`)
	out := h.proc.Handle(context.Background(), msgWith(payload, 0))
	if out != worker.OutcomeFailedTerminal {
		t.Fatalf("outcome = %s", out)
	}
	if len(h.dlq.entries) != 0 {
		t.Error("schema-invalid is not the DLQ route")
	}
	if len(h.consumer.acked) != 1 {
		t.Error("terminal failure must ack")
	}
}

// Retryable envelope-write failure leaves the message pending (write-then-ack:
// no handoff, no ack).
func TestScheduleFailureLeavesPending(t *testing.T) {
	h := newHarness(t)
	h.exporter.err = errclass.New(events.ErrorCodeStorage5xx, events.FailedStageUploadArtifacts, errors.New("down"))
	h.retries.err = errors.New("redis write refused")
	out := h.proc.Handle(context.Background(), msgWith(validPayload(t), 0))
	if out != worker.OutcomeNoAck || len(h.consumer.acked) != 0 {
		t.Fatalf("failed envelope handoff must not ack: outcome=%s acked=%v", out, h.consumer.acked)
	}
}

// PENDING record: transient ordering — bounded retry, not a terminal failure.
func TestPendingRecordSchedulesRetry(t *testing.T) {
	h := newHarness(t)
	h.deploy.rec.Status = deploymentservice.DeploymentStatusPending
	out := h.proc.Handle(context.Background(), msgWith(validPayload(t), 0))
	if out != worker.OutcomeRetryScheduled {
		t.Fatalf("outcome = %s", out)
	}
	if _, ok := h.retries.envs["dep_01:1:QUEUE_TEMPORARY_FAILURE"]; !ok {
		t.Errorf("envs = %v", h.retries.envs)
	}
}
