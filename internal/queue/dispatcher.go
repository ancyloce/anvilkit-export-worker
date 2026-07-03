package queue

import (
	"context"
	"log/slog"
	"time"

	"github.com/ancyloce/anvilkit-export-worker/internal/obs"
)

// Dispatcher is the delayed-retry dispatcher loop (EW-QUEUE-006): every
// Interval it looks up due envelopes, re-enqueues them to the main stream,
// and removes each envelope only after its re-enqueue succeeded. A failed
// re-enqueue leaves the envelope in place for the next tick — an envelope
// can be dispatched twice under crash timing, which is safe: redelivery is
// at-least-once and processing is idempotent by deploymentId.
type Dispatcher struct {
	Store    RetryStore
	Pub      Publisher
	Log      *slog.Logger
	Metrics  *obs.Metrics
	Interval time.Duration // default 1s
	Batch    int           // default 100
}

// Run loops until ctx is done.
func (d *Dispatcher) Run(ctx context.Context) {
	interval := d.Interval
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.Tick(ctx, time.Now())
		}
	}
}

// Tick performs one dispatch pass (exported for deterministic tests).
func (d *Dispatcher) Tick(ctx context.Context, now time.Time) {
	batch := d.Batch
	if batch <= 0 {
		batch = 100
	}
	envs, err := d.Store.Due(ctx, now, batch)
	if err != nil {
		d.Log.Error("retry dispatcher: due lookup failed", "err", err)
		return
	}
	for _, env := range envs {
		_, err := d.Pub.Publish(ctx, OutgoingMessage{
			Payload:       env.Payload,
			Attempt:       env.Attempt,
			LastErrorCode: string(env.LastErrorCode),
			TraceID:       env.TraceID,
		})
		if err != nil {
			// Removal only after successful re-enqueue: keep the envelope.
			d.Log.Error("retry dispatcher: re-enqueue failed; envelope retained",
				"retryEnvelopeId", env.RetryEnvelopeID, "deploymentId", env.DeploymentID, "err", err)
			continue
		}
		if err := d.Store.Remove(ctx, env.RetryEnvelopeID); err != nil {
			// Envelope re-dispatches next tick; consumers are idempotent.
			d.Log.Error("retry dispatcher: envelope removal failed after re-enqueue",
				"retryEnvelopeId", env.RetryEnvelopeID, "err", err)
			continue
		}
		d.Log.Info("retry dispatched",
			"retryEnvelopeId", env.RetryEnvelopeID,
			"deploymentId", env.DeploymentID,
			"attempt", env.Attempt,
			"lastErrorCode", string(env.LastErrorCode),
			"traceId", env.TraceID)
	}
	if d.Metrics != nil {
		if lag, ok, err := d.Store.OldestDueLag(ctx, time.Now()); err == nil {
			if ok {
				d.Metrics.RetryDispatchLagMs.Set(float64(lag.Milliseconds()))
			} else {
				d.Metrics.RetryDispatchLagMs.Set(0)
			}
		}
	}
}
