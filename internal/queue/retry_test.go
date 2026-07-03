package queue_test

import (
	"testing"
	"time"

	"github.com/ancyloce/anvilkit-export-worker/contracts/events"
	"github.com/ancyloce/anvilkit-export-worker/internal/queue"
)

// TestEnvelopeID pins the ADR-003 derivation:
// deploymentId + ":" + attempt + ":" + lastErrorCode.
func TestEnvelopeID(t *testing.T) {
	got := queue.EnvelopeID("dep_01", 1, events.ErrorCodeRenderOriginTimeout)
	if got != "dep_01:1:RENDER_ORIGIN_TIMEOUT" {
		t.Fatalf("EnvelopeID = %q", got)
	}
}

// TestBackoffBounds: exponential base 10s doubling per retry, capped at 5m,
// equal jitter (half fixed + half random) — PRD 0008 §12.2.
func TestBackoffBounds(t *testing.T) {
	atZero := queue.BackoffPolicy{Base: 10 * time.Second, Max: 5 * time.Minute, Rand: func() float64 { return 0 }}
	atMax := queue.BackoffPolicy{Base: 10 * time.Second, Max: 5 * time.Minute, Rand: func() float64 { return 0.999999 }}

	cases := []struct {
		attempt  int
		min, max time.Duration
	}{
		{1, 5 * time.Second, 10 * time.Second},
		{2, 10 * time.Second, 20 * time.Second},
		{3, 20 * time.Second, 40 * time.Second},
		{10, 150 * time.Second, 300 * time.Second}, // capped at Max
	}
	for _, c := range cases {
		lo := atZero.Delay(c.attempt)
		hi := atMax.Delay(c.attempt)
		if lo != c.min {
			t.Errorf("attempt %d: low-jitter delay = %v, want %v", c.attempt, lo, c.min)
		}
		if hi < lo || hi >= c.max+time.Second {
			t.Errorf("attempt %d: high-jitter delay = %v, want < %v", c.attempt, hi, c.max)
		}
	}
}

func TestBackoffDeterministicWithInjectedRand(t *testing.T) {
	p := queue.BackoffPolicy{Base: 10 * time.Second, Max: 5 * time.Minute, Rand: func() float64 { return 0.5 }}
	if p.Delay(1) != p.Delay(1) {
		t.Fatal("injected Rand must make Delay deterministic")
	}
	if p.Delay(1) != 7500*time.Millisecond {
		t.Fatalf("Delay(1) with r=0.5 = %v, want 7.5s", p.Delay(1))
	}
}
