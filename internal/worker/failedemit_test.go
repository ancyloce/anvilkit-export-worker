// Failed-event emission tests (FR-013 in the processor's terminal branches):
// CAS-then-emit ordering, exhaustion semantics, and silence on non-terminal
// outcomes.
package worker_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/ancyloce/anvilkit-export-worker/contracts/events"
	"github.com/ancyloce/anvilkit-export-worker/internal/errclass"
	"github.com/ancyloce/anvilkit-export-worker/internal/jsonschema"
	"github.com/ancyloce/anvilkit-export-worker/internal/queue"
	"github.com/ancyloce/anvilkit-export-worker/internal/worker"
)

type fakeFailedEmit struct {
	seq    *seq
	events []events.ExportFailed
}

func (f *fakeFailedEmit) EmitFailed(_ context.Context, ev events.ExportFailed) error {
	f.events = append(f.events, ev)
	f.seq.add("emit_failed")
	return nil
}

func harnessWithEmitter(t *testing.T) (*harness, *fakeFailedEmit) {
	t.Helper()
	h := newHarness(t)
	fe := &fakeFailedEmit{seq: h.seq}
	h.proc = worker.New(worker.Deps{
		Consumer: h.consumer, DLQ: h.dlq, Retries: h.retries, Locker: h.locks,
		Deploy: h.deploy, Exporter: h.exporter, FailedEmit: fe,
		Log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		WorkerID: "test-worker",
		Backoff:  queue.BackoffPolicy{Base: 1, Max: 2, Rand: func() float64 { return 0 }},
	})
	return h, fe
}

// TestNonRetryableEmitsFailedAfterCAS: terminal failure → CAS EXPORT_FAILED,
// then exactly one schema-valid failed event, then the ack.
func TestNonRetryableEmitsFailedAfterCAS(t *testing.T) {
	h, fe := harnessWithEmitter(t)
	h.exporter.err = errclass.New(events.ErrorCodeUnresolvedAssetRef, events.FailedStageHarvestDependencies,
		errors.New("asset:// residue"))

	if out := h.proc.Handle(context.Background(), msgWith(validPayload(t), 0)); out != worker.OutcomeFailedTerminal {
		t.Fatalf("outcome = %s", out)
	}
	if len(fe.events) != 1 {
		t.Fatalf("failed events = %d, want 1", len(fe.events))
	}
	ev := fe.events[0]
	if ev.ErrorCode != events.ErrorCodeUnresolvedAssetRef ||
		ev.FailedStage != events.FailedStageHarvestDependencies ||
		ev.Attempt != 0 || ev.RetryExhausted {
		t.Errorf("failed event = %+v", ev)
	}
	payload, _ := json.Marshal(ev)
	if violations := jsonschema.ValidateBytes(events.SchemaExportFailed, payload); len(violations) > 0 {
		t.Fatalf("failed event violates the frozen schema: %v", violations)
	}

	// Ordering: CAS happened (fake mutated the record), then emit, then ack.
	order := ""
	for _, e := range h.seq.list() {
		order += e + ","
	}
	if want := "emit_failed,ack,"; order != want {
		t.Errorf("side-effect order = %q, want %q (CAS-then-emit-then-ack)", order, want)
	}
}

// TestExhaustionEmitsRetryExhausted: attempt 3 retryable → failed event with
// attempt 3 + retryExhausted true (§10.3.2 exhaustion payload rule).
func TestExhaustionEmitsRetryExhausted(t *testing.T) {
	h, fe := harnessWithEmitter(t)
	h.exporter.err = errclass.New(events.ErrorCodeStorage5xx, events.FailedStageUploadArtifacts, errors.New("down"))

	if out := h.proc.Handle(context.Background(), msgWith(validPayload(t), 3)); out != worker.OutcomeDLQ {
		t.Fatalf("outcome = %s", out)
	}
	if len(fe.events) != 1 {
		t.Fatalf("failed events = %d, want 1", len(fe.events))
	}
	if fe.events[0].Attempt != 3 || !fe.events[0].RetryExhausted {
		t.Errorf("exhaustion event = %+v, want attempt 3 + retryExhausted", fe.events[0])
	}
}

// TestNoFailedEventOnRetryOrSuccess: scheduled retries and successes emit
// nothing on the failed stream.
func TestNoFailedEventOnRetryOrSuccess(t *testing.T) {
	h, fe := harnessWithEmitter(t)
	h.exporter.err = errclass.New(events.ErrorCodeStorage5xx, events.FailedStageUploadArtifacts, errors.New("down"))
	if out := h.proc.Handle(context.Background(), msgWith(validPayload(t), 0)); out != worker.OutcomeRetryScheduled {
		t.Fatalf("outcome = %s", out)
	}

	h2, fe2 := harnessWithEmitter(t)
	if out := h2.proc.Handle(context.Background(), msgWith(validPayload(t), 0)); out != worker.OutcomeSuccess {
		t.Fatalf("outcome = %s", out)
	}
	if len(fe.events)+len(fe2.events) != 0 {
		t.Fatalf("unexpected failed events: %v %v", fe.events, fe2.events)
	}
}
