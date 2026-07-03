// Package queue owns the queue driver abstraction and its Redis Streams MVP
// implementation (FR-003, FR-014, FR-016; ADR-003 five-mechanism model):
//
//  1. Message delivery — at-least-once via consumer group export-worker
//     (ADR-015) on anvilkit:deployment.export.requested.
//  2. Pending recovery — XPENDING/XAUTOCLAIM reclaim of delivered-but-unacked
//     messages; never increments the business attempt counter.
//  3. Business retry — a worker decision after a classified retryable
//     failure; attempt 0..3, maxRetries = 3 → four executions max.
//  4. Delayed retry — exponential backoff (base 10 s, max 5 m, jitter)
//     enforced by a Redis Hash (payloads) + ZSET (delay index) and a
//     dispatcher loop that removes an envelope only after successful
//     re-enqueue. Envelopes are idempotent by retryEnvelopeId.
//  5. Dead-letter routing — anvilkit:deployment.export.dlq after exhaustion
//     or for unparseable input, with full DLQEntry metadata.
//
// Kafka-ready seam (FR-021): job logic imports only the Consumer, Publisher,
// RetryStore, and DeadLetterer interfaces — never Redis primitives. At GA the
// Redis driver is swapped for Kafka topics keyed by deploymentId (main,
// retry, DLQ), with delayed retry moving to a scheduler/delayed-topic
// pattern; the interfaces stay unchanged.
package queue
