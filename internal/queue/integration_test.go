// Five-mechanism queue integration tests (EW-TEST-003; AC-021, AC-026,
// AC-027 storage side). Each mechanism of ADR-003 is proven distinctly
// against a real Redis (skipped without REDIS_TEST_URL).
package queue_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/ancyloce/anvilkit-export-worker/contracts/events"
	"github.com/ancyloce/anvilkit-export-worker/internal/queue"
	"github.com/ancyloce/anvilkit-export-worker/internal/testsupport"
)

func discardLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// Mechanism 1 — delivery: publish → fetch → ack → nothing pending.
func TestDeliveryFetchAck(t *testing.T) {
	rdb := testsupport.Redis(t, 1)
	ctx := context.Background()
	d := queue.NewRedisDriver(rdb, "itest-a")
	if err := d.EnsureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	id, err := d.Publish(ctx, queue.OutgoingMessage{Payload: []byte(`{"deploymentId":"dep_m1"}`)})
	if err != nil {
		t.Fatal(err)
	}
	msgs, err := d.Fetch(ctx, 10, 500*time.Millisecond)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("fetch = %v msgs, err %v", len(msgs), err)
	}
	if msgs[0].ID != id || msgs[0].Attempt != 0 {
		t.Errorf("msg = %+v, want id %s attempt 0", msgs[0], id)
	}
	if err := d.Ack(ctx, msgs[0].ID); err != nil {
		t.Fatal(err)
	}
	if n, _ := d.PendingCount(ctx); n != 0 {
		t.Errorf("pending after ack = %d, want 0", n)
	}
}

// Mechanism 2 — pending recovery: a delivered-but-unacked message is
// reclaimed by another consumer with its business attempt UNCHANGED.
func TestPendingReclaimKeepsAttempt(t *testing.T) {
	rdb := testsupport.Redis(t, 1)
	ctx := context.Background()
	a := queue.NewRedisDriver(rdb, "itest-a")
	b := queue.NewRedisDriver(rdb, "itest-b")
	if err := a.EnsureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	// A message mid-retry (attempt 2) that a crashed worker never acked.
	if _, err := a.Publish(ctx, queue.OutgoingMessage{
		Payload: []byte(`{"deploymentId":"dep_m2"}`), Attempt: 2,
		LastErrorCode: "STORAGE_5XX", TraceID: "trace_m2",
	}); err != nil {
		t.Fatal(err)
	}
	if msgs, err := a.Fetch(ctx, 1, 500*time.Millisecond); err != nil || len(msgs) != 1 {
		t.Fatalf("A fetch: %v %v", msgs, err)
	}
	// A "crashes" (no ack). B reclaims.
	reclaimed, err := b.Reclaim(ctx, 0, 10)
	if err != nil || len(reclaimed) != 1 {
		t.Fatalf("reclaim = %d msgs, err %v", len(reclaimed), err)
	}
	m := reclaimed[0]
	if m.Attempt != 2 || m.LastErrorCode != "STORAGE_5XX" || m.TraceID != "trace_m2" {
		t.Errorf("reclaim must redeliver unchanged (attempt NOT incremented): %+v", m)
	}
	if err := b.Ack(ctx, m.ID); err != nil {
		t.Fatal(err)
	}
}

// Mechanisms 3+4 — business retry storage and delayed dispatch: not-due
// envelopes are held; due envelopes re-enqueue with attempt metadata and are
// removed only after successful re-enqueue.
func TestRetryScheduleAndDueDispatch(t *testing.T) {
	rdb := testsupport.Redis(t, 1)
	ctx := context.Background()
	d := queue.NewRedisDriver(rdb, "itest-a")
	if err := d.EnsureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	store := queue.NewRedisRetryStore(rdb)
	now := time.Now()
	env := queue.RetryEnvelope{
		RetryEnvelopeID: queue.EnvelopeID("dep_m3", 2, events.ErrorCodeRenderOriginTimeout),
		DeploymentID:    "dep_m3",
		Attempt:         2,
		NextAttemptAt:   now.Add(30 * time.Second).UnixMilli(),
		LastErrorCode:   events.ErrorCodeRenderOriginTimeout,
		TraceID:         "trace_m3",
		Payload:         json.RawMessage(`{"deploymentId":"dep_m3"}`),
	}
	if err := store.Schedule(ctx, env); err != nil {
		t.Fatal(err)
	}

	// Not yet due: held.
	if due, err := store.Due(ctx, now, 100); err != nil || len(due) != 0 {
		t.Fatalf("not-due envelope dispatched early: %v %v", due, err)
	}
	dispatcher := &queue.Dispatcher{Store: store, Pub: d, Log: discardLog()}
	dispatcher.Tick(ctx, now)
	if msgs, _ := d.Fetch(ctx, 1, 100*time.Millisecond); len(msgs) != 0 {
		t.Fatal("dispatcher re-enqueued a not-due envelope")
	}

	// Due: dispatched with attempt metadata, then removed.
	after := now.Add(31 * time.Second)
	dispatcher.Tick(ctx, after)
	msgs, err := d.Fetch(ctx, 1, 500*time.Millisecond)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("due envelope not dispatched: %v %v", msgs, err)
	}
	m := msgs[0]
	if m.Attempt != 2 || m.LastErrorCode != string(events.ErrorCodeRenderOriginTimeout) || m.TraceID != "trace_m3" {
		t.Errorf("dispatched message lost retry metadata: %+v", m)
	}
	if due, _ := store.Due(ctx, after, 100); len(due) != 0 {
		t.Error("envelope not removed after successful re-enqueue")
	}
	if n, _ := rdb.HExists(ctx, queue.KeyRetryPayloads, env.RetryEnvelopeID).Result(); n {
		t.Error("payload hash entry not removed after successful re-enqueue")
	}
	_ = d.Ack(ctx, m.ID)
}

type failingPublisher struct{}

func (failingPublisher) Publish(context.Context, queue.OutgoingMessage) (string, error) {
	return "", errors.New("injected re-enqueue failure")
}

// Removal-after-success invariant (EW-QUEUE-006 DoD): a failed re-enqueue
// must leave the envelope in place; a later successful pass dispatches it.
func TestDispatcherRetainsEnvelopeWhenReenqueueFails(t *testing.T) {
	rdb := testsupport.Redis(t, 1)
	ctx := context.Background()
	d := queue.NewRedisDriver(rdb, "itest-a")
	if err := d.EnsureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	store := queue.NewRedisRetryStore(rdb)
	now := time.Now()
	env := queue.RetryEnvelope{
		RetryEnvelopeID: queue.EnvelopeID("dep_m4", 1, events.ErrorCodeStorage5xx),
		DeploymentID:    "dep_m4",
		Attempt:         1,
		NextAttemptAt:   now.Add(-time.Second).UnixMilli(), // already due
		LastErrorCode:   events.ErrorCodeStorage5xx,
		Payload:         json.RawMessage(`{"deploymentId":"dep_m4"}`),
	}
	if err := store.Schedule(ctx, env); err != nil {
		t.Fatal(err)
	}

	broken := &queue.Dispatcher{Store: store, Pub: failingPublisher{}, Log: discardLog()}
	broken.Tick(ctx, now)
	if due, _ := store.Due(ctx, now, 100); len(due) != 1 {
		t.Fatal("envelope must be retained when re-enqueue fails")
	}

	working := &queue.Dispatcher{Store: store, Pub: d, Log: discardLog()}
	working.Tick(ctx, now)
	if due, _ := store.Due(ctx, now, 100); len(due) != 0 {
		t.Fatal("envelope must dispatch once the publisher recovers")
	}
	msgs, _ := d.Fetch(ctx, 1, 500*time.Millisecond)
	if len(msgs) != 1 {
		t.Fatal("recovered dispatch did not reach the main stream")
	}
	_ = d.Ack(ctx, msgs[0].ID)
}

// Envelope idempotency, storage side (AC-026/AC-027): rewriting the same
// retryEnvelopeId is a harmless overwrite — one payload, one index member.
func TestEnvelopeRewriteIsHarmlessOverwrite(t *testing.T) {
	rdb := testsupport.Redis(t, 1)
	ctx := context.Background()
	store := queue.NewRedisRetryStore(rdb)
	id := queue.EnvelopeID("dep_m5", 1, events.ErrorCodeRenderOriginTimeout)
	base := queue.RetryEnvelope{
		RetryEnvelopeID: id, DeploymentID: "dep_m5", Attempt: 1,
		LastErrorCode: events.ErrorCodeRenderOriginTimeout,
		Payload:       json.RawMessage(`{"deploymentId":"dep_m5"}`),
	}
	base.NextAttemptAt = time.Now().Add(10 * time.Second).UnixMilli()
	if err := store.Schedule(ctx, base); err != nil {
		t.Fatal(err)
	}
	base.NextAttemptAt = time.Now().Add(20 * time.Second).UnixMilli()
	if err := store.Schedule(ctx, base); err != nil {
		t.Fatal(err)
	}
	if n, _ := rdb.HLen(ctx, queue.KeyRetryPayloads).Result(); n != 1 {
		t.Errorf("payload count after rewrite = %d, want 1 (never a second envelope)", n)
	}
	if n, _ := rdb.ZCard(ctx, queue.KeyRetryZSet).Result(); n != 1 {
		t.Errorf("index member count after rewrite = %d, want 1", n)
	}
}

// Mechanism 5 — DLQ: the entry preserves every §10.3.3 field.
func TestDLQEntryPreservesAllFields(t *testing.T) {
	rdb := testsupport.Redis(t, 1)
	ctx := context.Background()
	d := queue.NewRedisDriver(rdb, "itest-a")
	enqueued := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	failed := time.Now().Truncate(time.Millisecond)
	entry := queue.DLQEntry{
		Payload:     []byte(`{"deploymentId":"dep_m6"}`),
		ErrorCode:   events.ErrorCodeRenderOriginTimeout,
		FailedStage: events.FailedStageRenderHtml,
		Attempt:     3,
		TraceID:     "trace_m6",
		WorkerID:    "itest-a",
		EnqueuedAt:  enqueued,
		FailedAt:    failed,
	}
	if err := d.SendToDLQ(ctx, entry); err != nil {
		t.Fatal(err)
	}
	items, err := rdb.XRange(ctx, queue.StreamDLQ, "-", "+").Result()
	if err != nil || len(items) != 1 {
		t.Fatalf("dlq entries = %d, err %v", len(items), err)
	}
	v := items[0].Values
	want := map[string]string{
		"payload":     `{"deploymentId":"dep_m6"}`,
		"errorCode":   "RENDER_ORIGIN_TIMEOUT",
		"failedStage": "render_html",
		"attempt":     "3",
		"traceId":     "trace_m6",
		"workerId":    "itest-a",
	}
	for key, expected := range want {
		if got, _ := v[key].(string); got != expected {
			t.Errorf("dlq field %s = %q, want %q", key, v[key], expected)
		}
	}
	for _, key := range []string{"enqueuedAt", "failedAt"} {
		raw, _ := v[key].(string)
		if _, err := time.Parse(time.RFC3339Nano, raw); err != nil {
			t.Errorf("dlq field %s = %q not RFC3339: %v", key, raw, err)
		}
	}
}
