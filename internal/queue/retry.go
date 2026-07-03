package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ancyloce/anvilkit-export-worker/contracts/events"
)

// EnvelopeID derives the idempotency key (ADR-003):
//
//	retryEnvelopeId = deploymentId + ":" + attempt + ":" + lastErrorCode
//
// where attempt is the retry execution the envelope schedules (1..3). A
// reclaimed message that fails the same way after a crash-before-ack derives
// the same id, so the re-write is a harmless overwrite (AC-027).
func EnvelopeID(deploymentID string, attempt int, lastErrorCode events.ErrorCode) string {
	return deploymentID + ":" + strconv.Itoa(attempt) + ":" + string(lastErrorCode)
}

// BackoffPolicy computes delayed-retry backoff: exponential from Base capped
// at Max, with equal jitter (half fixed, half uniform-random) so retry storms
// decorrelate (PRD 0008 §12.2: base 10 s, max 5 m, jitter true).
type BackoffPolicy struct {
	Base time.Duration
	Max  time.Duration
	// Rand yields uniform [0,1); nil uses math/rand/v2. Injectable for tests.
	Rand func() float64
}

// DefaultBackoff is the PRD 0008 §12.2 policy.
var DefaultBackoff = BackoffPolicy{Base: 10 * time.Second, Max: 5 * time.Minute}

// Delay returns the backoff before retry execution attempt (1-based).
func (p BackoffPolicy) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := p.Base << (attempt - 1)
	if d > p.Max || d <= 0 {
		d = p.Max
	}
	r := p.Rand
	if r == nil {
		r = rand.Float64
	}
	half := d / 2
	return half + time.Duration(r()*float64(half))
}

// RedisRetryStore implements RetryStore on a Redis Hash (payloads) + ZSET
// (delay index) per ADR-003.
type RedisRetryStore struct {
	rdb redis.UniversalClient
}

func NewRedisRetryStore(rdb redis.UniversalClient) *RedisRetryStore {
	return &RedisRetryStore{rdb: rdb}
}

// Schedule writes the envelope: HSET payload + ZADD delay index, atomically.
// Repeated writes of the same RetryEnvelopeID overwrite in place.
func (s *RedisRetryStore) Schedule(ctx context.Context, env RetryEnvelope) error {
	raw, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal retry envelope: %w", err)
	}
	pipe := s.rdb.TxPipeline()
	pipe.HSet(ctx, KeyRetryPayloads, env.RetryEnvelopeID, string(raw))
	pipe.ZAdd(ctx, KeyRetryZSet, redis.Z{Score: float64(env.NextAttemptAt), Member: env.RetryEnvelopeID})
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("schedule retry envelope: %w", err)
	}
	return nil
}

// Due returns up to batch envelopes whose NextAttemptAt has passed.
func (s *RedisRetryStore) Due(ctx context.Context, now time.Time, batch int) ([]RetryEnvelope, error) {
	ids, err := s.rdb.ZRangeByScore(ctx, KeyRetryZSet, &redis.ZRangeBy{
		Min: "-inf", Max: strconv.FormatInt(now.UnixMilli(), 10),
		Offset: 0, Count: int64(batch),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("due lookup: %w", err)
	}
	if len(ids) == 0 {
		return nil, nil
	}
	raws, err := s.rdb.HMGet(ctx, KeyRetryPayloads, ids...).Result()
	if err != nil {
		return nil, fmt.Errorf("load retry payloads: %w", err)
	}
	out := make([]RetryEnvelope, 0, len(ids))
	for i, raw := range raws {
		str, ok := raw.(string)
		if !ok {
			// Orphaned index member (payload missing) — drop the member so it
			// cannot wedge the dispatcher.
			_ = s.rdb.ZRem(ctx, KeyRetryZSet, ids[i]).Err()
			continue
		}
		var env RetryEnvelope
		if err := json.Unmarshal([]byte(str), &env); err != nil {
			_ = s.rdb.ZRem(ctx, KeyRetryZSet, ids[i]).Err()
			_ = s.rdb.HDel(ctx, KeyRetryPayloads, ids[i]).Err()
			continue
		}
		out = append(out, env)
	}
	return out, nil
}

// Remove deletes the envelope — called only after successful re-enqueue
// (ADR-003 removal-after-success rule).
func (s *RedisRetryStore) Remove(ctx context.Context, retryEnvelopeID string) error {
	pipe := s.rdb.TxPipeline()
	pipe.ZRem(ctx, KeyRetryZSet, retryEnvelopeID)
	pipe.HDel(ctx, KeyRetryPayloads, retryEnvelopeID)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("remove retry envelope: %w", err)
	}
	return nil
}

// OldestDueLag reports the age of the oldest due envelope.
func (s *RedisRetryStore) OldestDueLag(ctx context.Context, now time.Time) (time.Duration, bool, error) {
	zs, err := s.rdb.ZRangeWithScores(ctx, KeyRetryZSet, 0, 0).Result()
	if err != nil {
		return 0, false, fmt.Errorf("oldest due: %w", err)
	}
	if len(zs) == 0 {
		return 0, false, nil
	}
	due := time.UnixMilli(int64(zs[0].Score))
	if due.After(now) {
		return 0, false, nil
	}
	return now.Sub(due), true, nil
}
