package queue

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisDriver implements Consumer, Publisher, and DeadLetterer on Redis
// Streams (the MVP driver behind the FR-021 seam).
type RedisDriver struct {
	rdb      redis.UniversalClient
	consumer string // consumer name within the export-worker group (= workerId)
}

// NewRedisDriver builds the driver. Call EnsureGroup once before consuming.
func NewRedisDriver(rdb redis.UniversalClient, consumerName string) *RedisDriver {
	return &RedisDriver{rdb: rdb, consumer: consumerName}
}

// EnsureGroup creates the consumer group (and stream) if absent.
func (d *RedisDriver) EnsureGroup(ctx context.Context) error {
	err := d.rdb.XGroupCreateMkStream(ctx, StreamMain, ConsumerGroup, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("create consumer group: %w", err)
	}
	return nil
}

// Publish appends to the main stream. Attempt metadata travels in entry
// fields so business retries (mechanism 3) are distinguishable from pending
// reclaims (mechanism 2), which redeliver the entry unchanged.
func (d *RedisDriver) Publish(ctx context.Context, msg OutgoingMessage) (string, error) {
	values := map[string]any{
		"payload": string(msg.Payload),
		"attempt": strconv.Itoa(msg.Attempt),
	}
	if msg.LastErrorCode != "" {
		values["lastErrorCode"] = msg.LastErrorCode
	}
	if msg.TraceID != "" {
		values["traceId"] = msg.TraceID
	}
	id, err := d.rdb.XAdd(ctx, &redis.XAddArgs{Stream: StreamMain, Values: values}).Result()
	if err != nil {
		return "", fmt.Errorf("publish to %s: %w", StreamMain, err)
	}
	return id, nil
}

// Fetch reads new messages for this consumer (XREADGROUP >).
func (d *RedisDriver) Fetch(ctx context.Context, max int, block time.Duration) ([]Message, error) {
	streams, err := d.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    ConsumerGroup,
		Consumer: d.consumer,
		Streams:  []string{StreamMain, ">"},
		Count:    int64(max),
		Block:    block,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil // block timeout, nothing delivered
		}
		return nil, fmt.Errorf("xreadgroup: %w", err)
	}
	var out []Message
	for _, s := range streams {
		for _, m := range s.Messages {
			out = append(out, fromXMessage(m))
		}
	}
	return out, nil
}

// Ack acknowledges processed messages (ADR-003 ack rule applies at the call
// site, never here).
func (d *RedisDriver) Ack(ctx context.Context, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}
	if err := d.rdb.XAck(ctx, StreamMain, ConsumerGroup, ids...).Err(); err != nil {
		return fmt.Errorf("xack: %w", err)
	}
	return nil
}

// Reclaim takes over messages idle for at least minIdle (XAUTOCLAIM),
// redelivering them unchanged — the business attempt counter is untouched.
func (d *RedisDriver) Reclaim(ctx context.Context, minIdle time.Duration, count int) ([]Message, error) {
	msgs, _, err := d.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   StreamMain,
		Group:    ConsumerGroup,
		Consumer: d.consumer,
		MinIdle:  minIdle,
		Start:    "0-0",
		Count:    int64(count),
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("xautoclaim: %w", err)
	}
	out := make([]Message, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, fromXMessage(m))
	}
	return out, nil
}

// PendingCount reports delivered-but-unacked messages in the group.
func (d *RedisDriver) PendingCount(ctx context.Context) (int64, error) {
	res, err := d.rdb.XPending(ctx, StreamMain, ConsumerGroup).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, nil
		}
		return 0, fmt.Errorf("xpending: %w", err)
	}
	return res.Count, nil
}

// SendToDLQ appends the full DLQEntry to the dead-letter stream (mechanism
// 5). The caller acks the original only after this returns nil
// (write-then-ack, PRD 0008 G-13).
func (d *RedisDriver) SendToDLQ(ctx context.Context, entry DLQEntry) error {
	err := d.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: StreamDLQ,
		Values: map[string]any{
			"payload":     string(entry.Payload),
			"errorCode":   string(entry.ErrorCode),
			"failedStage": string(entry.FailedStage),
			"attempt":     strconv.Itoa(entry.Attempt),
			"traceId":     entry.TraceID,
			"workerId":    entry.WorkerID,
			"enqueuedAt":  entry.EnqueuedAt.UTC().Format(time.RFC3339Nano),
			"failedAt":    entry.FailedAt.UTC().Format(time.RFC3339Nano),
		},
	}).Err()
	if err != nil {
		return fmt.Errorf("dlq handoff: %w", err)
	}
	return nil
}

// EnqueueTimeOf derives the original enqueue time from a stream entry ID
// (epoch-millis prefix).
func EnqueueTimeOf(entryID string) time.Time {
	ms, _, ok := strings.Cut(entryID, "-")
	if !ok {
		return time.Time{}
	}
	n, err := strconv.ParseInt(ms, 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.UnixMilli(n)
}

func fromXMessage(m redis.XMessage) Message {
	msg := Message{ID: m.ID}
	if v, ok := m.Values["payload"].(string); ok {
		msg.Payload = []byte(v)
	}
	if v, ok := m.Values["attempt"].(string); ok {
		if n, err := strconv.Atoi(v); err == nil {
			msg.Attempt = n
		}
	}
	if v, ok := m.Values["lastErrorCode"].(string); ok {
		msg.LastErrorCode = v
	}
	if v, ok := m.Values["traceId"].(string); ok {
		msg.TraceID = v
	}
	return msg
}
