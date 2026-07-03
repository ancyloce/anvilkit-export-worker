package queue

import (
	"context"
	"encoding/json"
	"time"

	"github.com/ancyloce/anvilkit-export-worker/contracts/events"
)

// Canonical Redis key names (ADR-003; consumer group per ADR-015).
const (
	StreamMain       = "anvilkit:deployment.export.requested"
	StreamDLQ        = "anvilkit:deployment.export.dlq"
	KeyRetryPayloads = "anvilkit:deployment.export.retry:payloads"
	KeyRetryZSet     = "anvilkit:deployment.export.retry:zset"
	ConsumerGroup    = "export-worker"
)

// Message is one delivered queue message. Attempt is the business retry
// counter carried in entry metadata: 0 on original publishes, N on
// dispatcher re-enqueues. Pending reclaim redelivers the entry unchanged, so
// reclaim never increments Attempt (mechanism 2 vs 3).
type Message struct {
	ID            string // driver-native id (Redis stream entry ID)
	Payload       []byte // raw deployment.export.requested JSON
	Attempt       int
	LastErrorCode string // error code that scheduled this retry ("" on originals)
	TraceID       string // trace continuity across retries ("" on originals)
}

// OutgoingMessage is a publish request to the main stream.
type OutgoingMessage struct {
	Payload       []byte
	Attempt       int
	LastErrorCode string
	TraceID       string
}

// Consumer delivers messages at-least-once and acks them. Ack is called only
// under the ADR-003 ack rule: successful completion, confirmed
// terminal/non-actionable deployment state, or successful write-then-ack
// handoff to retry storage or the DLQ.
type Consumer interface {
	// Fetch blocks up to block for at most max new messages.
	Fetch(ctx context.Context, max int, block time.Duration) ([]Message, error)
	// Ack acknowledges processed messages.
	Ack(ctx context.Context, ids ...string) error
	// Reclaim takes over messages delivered to any consumer but idle
	// (unacked) for at least minIdle — mechanism 2, XAUTOCLAIM.
	Reclaim(ctx context.Context, minIdle time.Duration, count int) ([]Message, error)
	// PendingCount reports delivered-but-unacked messages in the group.
	PendingCount(ctx context.Context) (int64, error)
}

// Publisher appends messages to the main stream (dispatcher re-enqueues and
// tooling).
type Publisher interface {
	Publish(ctx context.Context, msg OutgoingMessage) (string, error)
}

// DLQEntry preserves everything PRD 0010 §10.3.3 requires for inspection and
// manual replay.
type DLQEntry struct {
	Payload     []byte
	ErrorCode   events.ErrorCode
	FailedStage events.FailedStage
	Attempt     int
	TraceID     string
	WorkerID    string
	EnqueuedAt  time.Time // original main-stream enqueue time
	FailedAt    time.Time // terminal failure time
}

// DeadLetterer routes terminally failed or unparseable messages to the DLQ.
type DeadLetterer interface {
	SendToDLQ(ctx context.Context, entry DLQEntry) error
}

// RetryEnvelope schedules one business retry execution. Attempt is the
// execution it schedules (1..3). Idempotent by RetryEnvelopeID: a repeated
// write of the same envelope is a harmless overwrite, never a second
// envelope (AC-027).
type RetryEnvelope struct {
	RetryEnvelopeID string           `json:"retryEnvelopeId"`
	DeploymentID    string           `json:"deploymentId"`
	Attempt         int              `json:"attempt"`
	NextAttemptAt   int64            `json:"nextAttemptAt"` // epoch millis
	LastErrorCode   events.ErrorCode `json:"lastErrorCode"`
	TraceID         string           `json:"traceId"`
	Payload         json.RawMessage  `json:"payload"` // original event payload
}

// RetryStore is the delayed-retry storage (mechanism 4): Hash payloads +
// ZSET delay index.
type RetryStore interface {
	// Schedule writes the envelope (HSET + ZADD), idempotent by
	// RetryEnvelopeID.
	Schedule(ctx context.Context, env RetryEnvelope) error
	// Due returns up to batch envelopes with NextAttemptAt <= now.
	Due(ctx context.Context, now time.Time, batch int) ([]RetryEnvelope, error)
	// Remove deletes an envelope — called only after successful re-enqueue.
	Remove(ctx context.Context, retryEnvelopeID string) error
	// OldestDueLag reports how long the oldest due envelope has waited
	// (0, false when none is due). Feeds the retry-dispatch-lag gauge.
	OldestDueLag(ctx context.Context, now time.Time) (time.Duration, bool, error)
}
