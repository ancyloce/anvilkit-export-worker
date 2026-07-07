package queue

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Retention trimming (ADR-011): Redis Streams have no TTL, so the worker
// enforces the documented retention floors itself with XTRIM MINID. Exact
// (not approximate "~") trimming is deliberate: MINID "~" trims only whole
// macro-nodes, so at this queue's low write rates a node never fills and
// nothing would ever be trimmed — the floor would silently not be enforced
// exactly where it matters. Exact MINID is cheap at export volumes and
// keeps the ack-safety bound precise. DLQ and outcome streams trim on the
// pure time horizon — the retention window IS the documented
// evidence/consumption window. The main stream additionally respects the
// consumer group's state: an entry that is delivered-but-unacked (its ack
// decision has not completed) or not yet delivered is never trimmed, no
// matter how old — the ADR-003 ack rule outranks retention. Trimming is
// idempotent, so multiple workers running the retention loop concurrently
// is safe.

// TrimStream removes entries older than horizon from stream. Entries at or
// after the horizon are never removed. Returns entries removed.
func (d *RedisDriver) TrimStream(ctx context.Context, stream string, horizon time.Time) (int64, error) {
	n, err := d.rdb.XTrimMinID(ctx, stream, minIDAt(horizon)).Result()
	if err != nil {
		return 0, fmt.Errorf("trim %s: %w", stream, err)
	}
	return n, nil
}

// TrimMain trims the main stream to the retention horizon, capped so that
// no delivered-but-unacked (pending) entry and no undelivered entry is ever
// removed: the effective bound is the smallest of the horizon, the oldest
// pending entry, and the first entry past the group's last-delivered ID.
func (d *RedisDriver) TrimMain(ctx context.Context, horizon time.Time) (int64, error) {
	minID := minIDAt(horizon)

	// Never trim at or past the oldest pending entry — its ack logic has
	// not completed (ADR-003).
	pending, err := d.rdb.XPending(ctx, StreamMain, ConsumerGroup).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		if isNoStreamOrGroup(err) {
			return 0, nil // nothing consumed yet — nothing safe to trim
		}
		return 0, fmt.Errorf("trim %s: pending bound: %w", StreamMain, err)
	}
	if pending != nil && pending.Count > 0 && pending.Lower != "" {
		minID = minStreamID(minID, pending.Lower)
	}

	// Never trim entries the group has not been delivered yet (IDs greater
	// than last-delivered): cap at the entry right after last-delivered.
	// Everything below that cap is delivered, and — not being pending —
	// already acked.
	groups, err := d.rdb.XInfoGroups(ctx, StreamMain).Result()
	if err != nil {
		if isNoStreamOrGroup(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("trim %s: group bound: %w", StreamMain, err)
	}
	found := false
	for _, g := range groups {
		if g.Name == ConsumerGroup {
			minID = minStreamID(minID, nextStreamID(g.LastDeliveredID))
			found = true
		}
	}
	if !found {
		return 0, nil // group missing: no delivery state to bound by — do not trim
	}

	n, err := d.rdb.XTrimMinID(ctx, StreamMain, minID).Result()
	if err != nil {
		return 0, fmt.Errorf("trim %s: %w", StreamMain, err)
	}
	return n, nil
}

// minIDAt renders the stream ID horizon for a wall-clock time (entry IDs
// are epoch-millis prefixed).
func minIDAt(horizon time.Time) string {
	return strconv.FormatInt(horizon.UnixMilli(), 10) + "-0"
}

// parseStreamID splits a Redis stream ID ("ms-seq"; a bare "ms" means seq 0).
func parseStreamID(id string) (ms, seq uint64, ok bool) {
	msPart, seqPart, hasSeq := strings.Cut(id, "-")
	if !hasSeq {
		seqPart = "0"
	}
	m, err := strconv.ParseUint(msPart, 10, 64)
	if err != nil {
		return 0, 0, false
	}
	s, err := strconv.ParseUint(seqPart, 10, 64)
	if err != nil {
		return 0, 0, false
	}
	return m, s, true
}

// minStreamID returns the numerically smaller of two stream IDs; an
// unparseable side loses (conservative — the parseable bound applies).
func minStreamID(a, b string) string {
	am, as, aok := parseStreamID(a)
	bm, bs, bok := parseStreamID(b)
	if !aok {
		return b
	}
	if !bok {
		return a
	}
	if bm < am || (bm == am && bs < as) {
		return b
	}
	return a
}

// nextStreamID returns the smallest stream ID strictly greater than id.
func nextStreamID(id string) string {
	ms, seq, ok := parseStreamID(id)
	if !ok {
		return id
	}
	if seq == math.MaxUint64 {
		return strconv.FormatUint(ms+1, 10) + "-0"
	}
	return fmt.Sprintf("%d-%d", ms, seq+1)
}

// isNoStreamOrGroup matches the Redis errors for a missing stream or
// consumer group — both mean there is no delivery state to trim against.
func isNoStreamOrGroup(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "no such key") || strings.Contains(msg, "NOGROUP")
}
