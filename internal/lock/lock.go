// Package lock owns the per-deploymentId distributed lock (FR-005,
// EW-LOCK-001..004): SET lock:deployment:{deploymentId} {workerId} NX PX
// with TTL = renderTimeout + uploadTimeout + 60s (min 90s), heartbeat
// renewal while the job runs, and owner-checked release. A lock conflict on
// an active deployment must never ack the message — the conflict policy
// lives in the processor; this package only reports ErrConflict.
package lock

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// KeyPrefix is the canonical lock key prefix.
const KeyPrefix = "lock:deployment:"

// ErrConflict is returned when another worker holds the deployment lock.
var ErrConflict = errors.New("deployment lock held by another worker")

// renewScript renews the TTL only while this worker still owns the lock.
var renewScript = redis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("pexpire", KEYS[1], ARGV[2])
end
return 0
`)

// releaseScript deletes the lock only while this worker still owns it —
// a worker must never release a lock it no longer owns after TTL expiry
// (PRD 0008 §10.4).
var releaseScript = redis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("del", KEYS[1])
end
return 0
`)

// TTLFor computes the lock TTL: renderTimeout + uploadTimeout + 60s, minimum
// 90s (FR-005). The job's hard deadline is bounded by this TTL.
func TTLFor(renderTimeout, uploadTimeout time.Duration) time.Duration {
	ttl := renderTimeout + uploadTimeout + 60*time.Second
	if ttl < 90*time.Second {
		ttl = 90 * time.Second
	}
	return ttl
}

// Locker acquires per-deployment locks for one worker identity.
type Locker struct {
	rdb      redis.UniversalClient
	workerID string
	ttl      time.Duration
	// heartbeatEvery defaults to ttl/3.
	heartbeatEvery time.Duration
}

// NewLocker builds a Locker. ttl should come from TTLFor.
func NewLocker(rdb redis.UniversalClient, workerID string, ttl time.Duration) *Locker {
	return &Locker{rdb: rdb, workerID: workerID, ttl: ttl, heartbeatEvery: ttl / 3}
}

// Lease is one held lock. Release stops the heartbeat and performs the
// owner-checked delete. Lost is closed if a heartbeat discovers the lock is
// no longer owned (TTL expired or taken over) — the job should abort.
type Lease struct {
	locker *Locker
	key    string
	stop   chan struct{}
	done   chan struct{}
	lost   chan struct{}
}

// Acquire takes the lock for deploymentID or returns ErrConflict.
func (l *Locker) Acquire(ctx context.Context, deploymentID string) (*Lease, error) {
	key := KeyPrefix + deploymentID
	ok, err := l.rdb.SetNX(ctx, key, l.workerID, l.ttl).Result()
	if err != nil {
		return nil, fmt.Errorf("lock acquire: %w", err)
	}
	if !ok {
		return nil, ErrConflict
	}
	lease := &Lease{
		locker: l,
		key:    key,
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
		lost:   make(chan struct{}),
	}
	go lease.heartbeat()
	return lease, nil
}

// Lost is closed when lock ownership was lost mid-job.
func (le *Lease) Lost() <-chan struct{} { return le.lost }

func (le *Lease) heartbeat() {
	defer close(le.done)
	every := le.locker.heartbeatEvery
	if every <= 0 {
		every = le.locker.ttl / 3
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-le.stop:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			renewed, err := renewScript.Run(ctx, le.locker.rdb,
				[]string{le.key}, le.locker.workerID, le.locker.ttl.Milliseconds()).Int()
			cancel()
			if err == nil && renewed == 0 {
				// Ownership gone: crash-recovery path took over after TTL
				// expiry. Signal the job to abort; never re-take here.
				close(le.lost)
				return
			}
			// Transient renewal errors are tolerated; the TTL still covers
			// several heartbeat intervals.
		}
	}
}

// Release stops the heartbeat and deletes the lock only if still owned.
func (le *Lease) Release(ctx context.Context) error {
	select {
	case <-le.stop:
		// already released
	default:
		close(le.stop)
	}
	<-le.done
	if err := releaseScript.Run(ctx, le.locker.rdb, []string{le.key}, le.locker.workerID).Err(); err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("lock release: %w", err)
	}
	return nil
}
