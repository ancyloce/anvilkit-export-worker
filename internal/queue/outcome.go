package queue

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Outcome-event streams (FR-013). Consumers (cdn-service, observers) are
// duplicate-tolerant keyed by deploymentId — emission is at-least-once under
// the CAS-then-emit ordering (ADR-005 default).
const (
	StreamArtifactReady = "anvilkit:deployment.artifact.ready"
	StreamExportFailed  = "anvilkit:deployment.export.failed"
)

// AppendOutcome appends one outcome-event payload to the given stream. It is
// the emitter's transport (internal/emit); payloads are schema-validated
// before they reach here.
func (d *RedisDriver) AppendOutcome(ctx context.Context, stream string, payload []byte) (string, error) {
	id, err := d.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]any{"payload": string(payload)},
	}).Result()
	if err != nil {
		return "", fmt.Errorf("append outcome to %s: %w", stream, err)
	}
	return id, nil
}
