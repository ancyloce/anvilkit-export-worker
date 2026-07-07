// Stream retention integration tests (ADR-011 enforcement, M5 hardening):
// horizon trimming on DLQ/outcome streams and the main-stream ack-safety
// invariant — a delivered-but-unacked or undelivered entry is never trimmed,
// regardless of age. Skipped without REDIS_TEST_URL (queue package DB 1).
package queue_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ancyloce/anvilkit-export-worker/internal/queue"
	"github.com/ancyloce/anvilkit-export-worker/internal/testsupport"
)

// addAt appends an entry with an explicit (old) stream ID.
func addAt(t *testing.T, rdb redis.UniversalClient, stream, id string) {
	t.Helper()
	err := rdb.XAdd(context.Background(), &redis.XAddArgs{
		Stream: stream,
		ID:     id,
		Values: map[string]any{"payload": fmt.Sprintf(`{"deploymentId":"dep_%s"}`, id)},
	}).Err()
	if err != nil {
		t.Fatal(err)
	}
}

func streamIDs(t *testing.T, rdb redis.UniversalClient, stream string) []string {
	t.Helper()
	items, err := rdb.XRange(context.Background(), stream, "-", "+").Result()
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, len(items))
	for _, it := range items {
		ids = append(ids, it.ID)
	}
	return ids
}

// TestTrimStreamHorizon: DLQ/outcome-style trimming removes entries older
// than the horizon and keeps newer ones.
func TestTrimStreamHorizon(t *testing.T) {
	rdb := testsupport.Redis(t, 1)
	ctx := context.Background()
	d := queue.NewRedisDriver(rdb, "itest-trim")

	horizon := time.Now().Add(-time.Hour)
	oldID := fmt.Sprintf("%d-0", horizon.Add(-time.Minute).UnixMilli())
	newID := fmt.Sprintf("%d-0", horizon.Add(time.Minute).UnixMilli())
	for _, stream := range []string{queue.StreamDLQ, queue.StreamArtifactReady, queue.StreamExportFailed} {
		addAt(t, rdb, stream, oldID)
		addAt(t, rdb, stream, newID)

		n, err := d.TrimStream(ctx, stream, horizon)
		if err != nil {
			t.Fatalf("TrimStream(%s): %v", stream, err)
		}
		if n != 1 {
			t.Errorf("TrimStream(%s) removed %d entries, want 1", stream, n)
		}
		if ids := streamIDs(t, rdb, stream); len(ids) != 1 || ids[0] != newID {
			t.Errorf("%s after trim = %v, want [%s]", stream, ids, newID)
		}
	}
}

// TestTrimMainAckSafety: with three ancient entries — acked, pending
// (delivered, unacked), and undelivered — a trim at "now" removes only the
// acked one. Retention never outranks the ADR-003 ack rule.
func TestTrimMainAckSafety(t *testing.T) {
	rdb := testsupport.Redis(t, 1)
	ctx := context.Background()
	d := queue.NewRedisDriver(rdb, "itest-trim")
	if err := d.EnsureGroup(ctx); err != nil {
		t.Fatal(err)
	}

	// Three entries far older than any sane retention horizon.
	acked, pending, undelivered := "1000-0", "1000-1", "1000-2"
	addAt(t, rdb, queue.StreamMain, acked)
	addAt(t, rdb, queue.StreamMain, pending)
	addAt(t, rdb, queue.StreamMain, undelivered)

	// Deliver the first two; ack only the first. The third stays undelivered.
	msgs, err := d.Fetch(ctx, 2, 500*time.Millisecond)
	if err != nil || len(msgs) != 2 {
		t.Fatalf("fetch = %d msgs, err %v", len(msgs), err)
	}
	if err := d.Ack(ctx, acked); err != nil {
		t.Fatal(err)
	}

	n, err := d.TrimMain(ctx, time.Now())
	if err != nil {
		t.Fatalf("TrimMain: %v", err)
	}
	if n != 1 {
		t.Errorf("TrimMain removed %d entries, want 1 (only the acked entry)", n)
	}
	ids := streamIDs(t, rdb, queue.StreamMain)
	if len(ids) != 2 || ids[0] != pending || ids[1] != undelivered {
		t.Fatalf("main stream after trim = %v, want [%s %s]", ids, pending, undelivered)
	}

	// The pending entry must still be reclaimable after the trim.
	reclaimed, err := d.Reclaim(ctx, 0, 10)
	if err != nil || len(reclaimed) != 1 || reclaimed[0].ID != pending {
		t.Fatalf("reclaim after trim = %v, err %v (pending entry must survive trimming)", reclaimed, err)
	}

	// Once acked and delivered respectively, both remaining entries become
	// trimmable — the bound moves with the group state, not the clock.
	if err := d.Ack(ctx, pending); err != nil {
		t.Fatal(err)
	}
	msgs, err = d.Fetch(ctx, 1, 500*time.Millisecond)
	if err != nil || len(msgs) != 1 || msgs[0].ID != undelivered {
		t.Fatalf("fetch undelivered = %v, err %v", msgs, err)
	}
	if err := d.Ack(ctx, undelivered); err != nil {
		t.Fatal(err)
	}
	if n, err = d.TrimMain(ctx, time.Now()); err != nil || n != 2 {
		t.Fatalf("TrimMain after acks removed %d, err %v, want 2", n, err)
	}
}

// TestTrimMainWithoutTraffic: an empty/uncreated stream is a no-op, never an
// error (the retention loop must be safe from boot).
func TestTrimMainWithoutTraffic(t *testing.T) {
	rdb := testsupport.Redis(t, 1)
	ctx := context.Background()
	d := queue.NewRedisDriver(rdb, "itest-trim")

	// No stream, no group at all.
	if n, err := d.TrimMain(ctx, time.Now()); err != nil || n != 0 {
		t.Fatalf("TrimMain on missing stream = %d, %v; want 0, nil", n, err)
	}

	// Stream + group exist, nothing ever delivered: ancient entries are
	// undelivered and must survive.
	if err := d.EnsureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	addAt(t, rdb, queue.StreamMain, "1000-0")
	if n, err := d.TrimMain(ctx, time.Now()); err != nil || n != 0 {
		t.Fatalf("TrimMain with undelivered backlog = %d, %v; want 0, nil", n, err)
	}
	if ids := streamIDs(t, rdb, queue.StreamMain); len(ids) != 1 {
		t.Fatalf("undelivered backlog trimmed: %v", ids)
	}
}
