package lock_test

import (
	"context"
	"testing"
	"time"

	"github.com/ancyloce/anvilkit-export-worker/internal/lock"
	"github.com/ancyloce/anvilkit-export-worker/internal/testsupport"
)

// TestTTLFor pins the FR-005 formula: renderTimeout + uploadTimeout + 60s,
// minimum 90s.
func TestTTLFor(t *testing.T) {
	if got := lock.TTLFor(15*time.Second, 60*time.Second); got != 135*time.Second {
		t.Errorf("TTLFor(15s,60s) = %v, want 135s", got)
	}
	if got := lock.TTLFor(5*time.Second, 5*time.Second); got != 90*time.Second {
		t.Errorf("TTLFor(5s,5s) = %v, want the 90s floor", got)
	}
}

func TestAcquireConflictAndHandover(t *testing.T) {
	rdb := testsupport.Redis(t, 2)
	ctx := context.Background()
	a := lock.NewLocker(rdb, "worker-a", 90*time.Second)
	b := lock.NewLocker(rdb, "worker-b", 90*time.Second)

	leaseA, err := a.Acquire(ctx, "dep_lock_1")
	if err != nil {
		t.Fatalf("A acquire: %v", err)
	}
	if _, err := b.Acquire(ctx, "dep_lock_1"); err != lock.ErrConflict {
		t.Fatalf("B acquire while A holds = %v, want ErrConflict", err)
	}
	if err := leaseA.Release(ctx); err != nil {
		t.Fatalf("A release: %v", err)
	}
	leaseB, err := b.Acquire(ctx, "dep_lock_1")
	if err != nil {
		t.Fatalf("B acquire after release: %v", err)
	}
	_ = leaseB.Release(ctx)
}

// TestOwnerCheckedRelease: a worker must never release a lock it no longer
// owns (PRD 0008 §10.4) — simulated TTL-expiry takeover.
func TestOwnerCheckedRelease(t *testing.T) {
	rdb := testsupport.Redis(t, 2)
	ctx := context.Background()
	a := lock.NewLocker(rdb, "worker-a", 90*time.Second)

	leaseA, err := a.Acquire(ctx, "dep_lock_2")
	if err != nil {
		t.Fatal(err)
	}
	// Simulate: A's TTL expired and worker-b took the lock over.
	if err := rdb.Set(ctx, lock.KeyPrefix+"dep_lock_2", "worker-b", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if err := leaseA.Release(ctx); err != nil {
		t.Fatalf("release must be a no-op on foreign lock, got %v", err)
	}
	val, err := rdb.Get(ctx, lock.KeyPrefix+"dep_lock_2").Result()
	if err != nil || val != "worker-b" {
		t.Fatalf("foreign lock must survive: val=%q err=%v", val, err)
	}
}

// TestHeartbeatKeepsLockAlive: a long job outlives the initial TTL because
// the heartbeat renews it (EW-LOCK-002).
func TestHeartbeatKeepsLockAlive(t *testing.T) {
	rdb := testsupport.Redis(t, 2)
	ctx := context.Background()
	// TTL 300ms → heartbeat every 100ms.
	a := lock.NewLocker(rdb, "worker-a", 300*time.Millisecond)
	lease, err := a.Acquire(ctx, "dep_lock_3")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lease.Release(ctx) }()

	time.Sleep(900 * time.Millisecond) // 3× the TTL
	val, err := rdb.Get(ctx, lock.KeyPrefix+"dep_lock_3").Result()
	if err != nil || val != "worker-a" {
		t.Fatalf("lock must still be held after 3×TTL: val=%q err=%v", val, err)
	}
}

// TestLostOwnershipSignalled: when ownership disappears (simulated expiry +
// takeover), the heartbeat closes Lost so the job aborts.
func TestLostOwnershipSignalled(t *testing.T) {
	rdb := testsupport.Redis(t, 2)
	ctx := context.Background()
	a := lock.NewLocker(rdb, "worker-a", 300*time.Millisecond)
	lease, err := a.Acquire(ctx, "dep_lock_4")
	if err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(ctx, lock.KeyPrefix+"dep_lock_4", "worker-b", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-lease.Lost():
		// expected
	case <-time.After(2 * time.Second):
		t.Fatal("Lost() not signalled within 2s of ownership loss")
	}
	_ = lease.Release(ctx)
	val, _ := rdb.Get(ctx, lock.KeyPrefix+"dep_lock_4").Result()
	if val != "worker-b" {
		t.Fatalf("foreign lock must survive release-after-loss, got %q", val)
	}
}
