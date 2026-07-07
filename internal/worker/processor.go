// Package worker wires the job pipeline (PRD 0010 §5.1): consume →
// validate → load record → reconcile → lock → CAS EXPORT_QUEUED→EXPORTING →
// Exporter seam (render/harvest/upload/manifest/emit live in
// internal/export). It owns every ack decision under the ADR-003 ack rule
// and the retry / DLQ / terminal-failure branches (FR-014).
package worker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/ancyloce/anvilkit-export-worker/contracts/deploymentservice"
	"github.com/ancyloce/anvilkit-export-worker/contracts/events"
	"github.com/ancyloce/anvilkit-export-worker/internal/deployment"
	"github.com/ancyloce/anvilkit-export-worker/internal/emit"
	"github.com/ancyloce/anvilkit-export-worker/internal/errclass"
	"github.com/ancyloce/anvilkit-export-worker/internal/lock"
	"github.com/ancyloce/anvilkit-export-worker/internal/obs"
	"github.com/ancyloce/anvilkit-export-worker/internal/queue"
)

// Outcome makes every ack/no-ack decision observable to tests (AC-021,
// AC-027, AC-028, AC-033 assertions).
type Outcome string

const (
	OutcomeSuccess        Outcome = "success"         // exporter completed; acked
	OutcomeDryRun         Outcome = "dry_run"         // local scaffold mode; acked, no status writes
	OutcomeAckedTerminal  Outcome = "acked_terminal"  // deployment terminal/non-actionable; acked without work
	OutcomeRetryScheduled Outcome = "retry_scheduled" // envelope written (write-then-ack)
	OutcomeDLQ            Outcome = "dlq"             // DLQ handoff succeeded; acked
	OutcomeFailedTerminal Outcome = "failed_terminal" // non-retryable; CAS EXPORT_FAILED; acked
	OutcomeLockConflict   Outcome = "lock_conflict"   // active deployment; NOT acked, left pending
	OutcomeStopSafe       Outcome = "stop_safe"       // 409 STATUS_CONFLICT resolution
	OutcomeNoAck          Outcome = "no_ack"          // handoff failed; left pending for reclaim
)

// Deps are the processor's collaborators — interfaces only, so the queue
// driver stays swappable (FR-021) and tests inject fakes.
type Deps struct {
	Consumer queue.Consumer
	DLQ      queue.DeadLetterer
	Retries  queue.RetryStore
	Locker   Locks
	Deploy   DeploymentAPI
	Exporter Exporter
	// FailedEmit emits deployment.export.failed after terminal failures
	// (FR-013, CAS-then-emit); nil disables emission.
	FailedEmit FailedEmitter
	// ReadyRedeliver runs the FR-015 manifest-check + ready re-emit on
	// redelivery after ARTIFACT_READY; nil falls back to a plain ack.
	ReadyRedeliver ReadyRedeliverer
	Metrics        *obs.Metrics
	Log            *slog.Logger
	Backoff        queue.BackoffPolicy
	WorkerID       string
	DryRun         bool          // local-only scaffold mode (config-guarded)
	MaxRetries     int           // business retries; default 3 (four executions)
	JobTimeout     time.Duration // per-job hard deadline, bounded by the lock TTL
	Now            func() time.Time
}

// Processor executes one message at a time; it is safe for concurrent use by
// the consumer pool.
type Processor struct {
	d Deps
}

func New(d Deps) *Processor {
	if d.MaxRetries == 0 {
		d.MaxRetries = 3
	}
	if d.Backoff.Base == 0 {
		d.Backoff = queue.DefaultBackoff
	}
	if d.JobTimeout <= 0 {
		d.JobTimeout = 90 * time.Second
	}
	if d.Now == nil {
		d.Now = time.Now
	}
	return &Processor{d: d}
}

// Handle processes one delivered message end to end and returns the ack
// decision that was taken.
func (p *Processor) Handle(ctx context.Context, msg queue.Message) Outcome {
	start := p.d.Now()
	if p.d.Metrics != nil {
		p.d.Metrics.JobsTotal.Inc()
	}

	ev, perr := queue.ParseEvent(msg.Payload)
	if perr != nil {
		if errors.Is(perr, queue.ErrUnparseable) {
			return p.unparseableToDLQ(ctx, msg, perr)
		}
		// Schema-invalid but deploymentId extractable: classified
		// VALIDATION_FAILED (§13) — terminal-failure branch without a
		// loaded record.
		log := p.d.Log.With("deploymentId", extractDeploymentID(msg.Payload), "attempt", msg.Attempt)
		return p.fail(ctx, msg, nil, extractDeploymentID(msg.Payload), nil, "", errclass.From(perr, events.FailedStageConsumeJob), log, start)
	}

	traceID := msg.TraceID
	if traceID == "" {
		traceID = newTraceID()
	}
	log := obs.JobLogger(p.d.Log, obs.JobFields{
		TraceID: traceID, EventID: ev.EventID, DeploymentID: ev.DeploymentID,
		TeamID: ev.TeamID, SiteID: ev.SiteID, PageID: ev.PageID,
		Slug: ev.Slug, Version: ev.Version, Environment: string(ev.Environment),
		RenderMode: string(ev.RenderMode), Attempt: msg.Attempt,
	})
	log.Info("job started", "stage", string(events.FailedStageConsumeJob), "messageId", msg.ID)

	jobCtx, cancel := context.WithTimeout(ctx, p.d.JobTimeout)
	defer cancel()
	// §15.3 root span; stage spans nest under it and the context is
	// forwarded to render-origin (EW-OBS-003).
	jobCtx, jobSpan := obs.StartSpan(jobCtx, "consume_job",
		attribute.String("anvilkit.deployment_id", ev.DeploymentID),
		attribute.String("anvilkit.trace_id", traceID),
		attribute.Int("anvilkit.attempt", msg.Attempt))
	defer jobSpan.End()

	// Load the authoritative record — the record drives the job (FR-004).
	loadCtx, loadSpan := obs.StartSpan(jobCtx, "load_deployment")
	rec, err := p.d.Deploy.Load(loadCtx, ev.DeploymentID)
	obs.EndSpan(loadSpan, err)
	if err != nil {
		return p.fail(ctx, msg, ev, ev.DeploymentID, nil, traceID, errclass.From(err, events.FailedStageLoadDeployment), log, start)
	}

	// Redelivery after completion (or any non-actionable state): ack without
	// re-rendering (FR-015). For ARTIFACT_READY the stored manifest is
	// verified and the ready event re-emitted — a crash between the CAS and
	// the emit therefore repeats the emit on redelivery (doc 0010 §12);
	// consumers dedupe by deploymentId.
	if deployment.NonActionable(rec.Status) {
		if rec.Status == deploymentservice.DeploymentStatusArtifactReady && p.d.ReadyRedeliver != nil {
			found, rerr := p.d.ReadyRedeliver.ReemitReady(jobCtx, rec, traceID)
			if rerr != nil {
				log.Error("ready re-emit failed on redelivery; leaving message pending",
					"stage", string(events.FailedStageEmitReady), "err", rerr)
				return OutcomeNoAck
			}
			if !found {
				log.Error("deployment ARTIFACT_READY but no manifest in storage", "alert", true,
					"stage", string(events.FailedStageEmitReady))
			} else {
				log.Info("redelivery after ARTIFACT_READY: ready event re-emitted without re-render",
					"stage", string(events.FailedStageEmitReady))
			}
		}
		log.Info("deployment non-actionable; acking redelivery",
			"stage", string(events.FailedStageLoadDeployment), "status", string(rec.Status))
		p.ack(ctx, msg, log)
		return OutcomeAckedTerminal
	}
	if !deployment.Actionable(rec.Status) {
		// PENDING: the event outran the record's EXPORT_QUEUED transition —
		// transient ordering, bounded retries (Recommended Approach).
		return p.fail(ctx, msg, ev, ev.DeploymentID, rec, traceID,
			errclass.New(events.ErrorCodeQueueTemporaryFailure, events.FailedStageLoadDeployment,
				errors.New("deployment record not yet EXPORT_QUEUED (status "+string(rec.Status)+")")), log, start)
	}

	if err := deployment.Reconcile(rec, ev); err != nil {
		return p.fail(ctx, msg, ev, ev.DeploymentID, rec, traceID, errclass.From(err, events.FailedStageLoadDeployment), log, start)
	}

	// Per-deployment lock (FR-005). A conflict on an active deployment is
	// never acked — the message stays pending and pending recovery
	// redelivers it later (AC-007/AC-028).
	lockCtx, lockSpan := obs.StartSpan(jobCtx, "acquire_lock")
	lease, err := p.d.Locker.Acquire(lockCtx, ev.DeploymentID)
	obs.EndSpan(lockSpan, err)
	if errors.Is(err, lock.ErrConflict) {
		if p.d.Metrics != nil {
			p.d.Metrics.LockConflictTotal.Inc()
		}
		log.Warn("lock conflict on active deployment; leaving message pending",
			"stage", string(events.FailedStageAcquireLock))
		return OutcomeLockConflict
	}
	if err != nil {
		return p.fail(ctx, msg, ev, ev.DeploymentID, rec, traceID,
			errclass.New(events.ErrorCodeQueueTemporaryFailure, events.FailedStageAcquireLock, err), log, start)
	}
	defer func() {
		releaseCtx, releaseCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer releaseCancel()
		if rerr := lease.Release(releaseCtx); rerr != nil {
			log.Error("lock release failed (TTL will expire it)", "err", rerr)
		}
	}()
	// Lost lock ownership aborts the job (TTL bounds the hard deadline).
	go func() {
		select {
		case <-lease.Lost():
			log.Error("lock ownership lost mid-job; aborting", "stage", string(events.FailedStageAcquireLock))
			cancel()
		case <-jobCtx.Done():
		}
	}()

	if p.d.DryRun {
		log.Info("dry-run complete: validated, reconciled, locked — no status writes, exporter not invoked (local scaffold mode)",
			"stage", string(events.FailedStageAcquireLock), "status", string(rec.Status))
		p.ack(ctx, msg, log)
		return OutcomeDryRun
	}

	// CAS EXPORT_QUEUED → EXPORTING (skipped when resuming a crashed
	// EXPORTING job via reclaim — idempotent re-run).
	if rec.Status == deploymentservice.DeploymentStatusExportQueued {
		casCtx, casSpan := obs.StartSpan(jobCtx, "update_status_exporting")
		err := p.d.Deploy.Transition(casCtx, ev.DeploymentID,
			deploymentservice.DeploymentStatusExportQueued,
			deploymentservice.DeploymentStatusExporting,
			"worker_started", traceID, events.FailedStageUpdateStatusExporting)
		obs.EndSpan(casSpan, err)
		if conflict, ok := deployment.AsStatusConflict(err); ok {
			// Stop safely: another actor advanced the deployment (FR-006).
			if deployment.NonActionable(conflict.CurrentStatus) {
				log.Info("CAS conflict with non-actionable status; acking",
					"stage", string(events.FailedStageUpdateStatusExporting),
					"currentStatus", string(conflict.CurrentStatus))
				p.ack(ctx, msg, log)
				return OutcomeStopSafe
			}
			log.Warn("CAS conflict with active status; leaving message pending",
				"stage", string(events.FailedStageUpdateStatusExporting),
				"currentStatus", string(conflict.CurrentStatus))
			return OutcomeStopSafe
		}
		if err != nil {
			return p.fail(ctx, msg, ev, ev.DeploymentID, rec, traceID,
				errclass.From(err, events.FailedStageUpdateStatusExporting), log, start)
		}
		// Track the transition locally: a later terminal CAS must send
		// from=EXPORTING, not the stale loaded snapshot.
		rec.Status = deploymentservice.DeploymentStatusExporting
		log.Info("status EXPORTING", "stage", string(events.FailedStageUpdateStatusExporting),
			"status", string(deploymentservice.DeploymentStatusExporting))
	}

	// Export seam: render → harvest → upload → manifest → submit →
	// CAS ARTIFACT_READY → emit ready (internal/export).
	if err := p.d.Exporter.Export(jobCtx, &Job{Event: ev, Record: rec, Msg: msg, TraceID: traceID, Log: log}); err != nil {
		return p.fail(ctx, msg, ev, ev.DeploymentID, rec, traceID,
			errclass.From(err, events.FailedStageRenderHtml), log, start)
	}

	if p.d.Metrics != nil {
		p.d.Metrics.JobsSuccessTotal.Inc()
		p.d.Metrics.JobDurationMs.Observe(float64(p.d.Now().Sub(start).Milliseconds()))
	}
	log.Info("job succeeded", "stage", string(events.FailedStageAckMessage),
		"status", string(deploymentservice.DeploymentStatusArtifactReady),
		"durationMs", p.d.Now().Sub(start).Milliseconds(), "errorCode", "")
	p.ack(ctx, msg, log)
	return OutcomeSuccess
}

// unparseableToDLQ implements the FR-003 rule: DLQ with alert, ack the
// original only after successful DLQ handoff, never a status update.
func (p *Processor) unparseableToDLQ(ctx context.Context, msg queue.Message, cause error) Outcome {
	log := p.d.Log.With("messageId", msg.ID, "attempt", msg.Attempt)
	entry := queue.DLQEntry{
		Payload:     msg.Payload,
		ErrorCode:   events.ErrorCodeValidationFailed,
		FailedStage: events.FailedStageConsumeJob,
		Attempt:     msg.Attempt,
		TraceID:     msg.TraceID,
		WorkerID:    p.d.WorkerID,
		EnqueuedAt:  queue.EnqueueTimeOf(msg.ID),
		FailedAt:    p.d.Now(),
	}
	if err := p.d.DLQ.SendToDLQ(ctx, entry); err != nil {
		log.Error("DLQ handoff failed for unparseable event; leaving message pending", "err", err, "cause", cause)
		return OutcomeNoAck
	}
	if p.d.Metrics != nil {
		p.d.Metrics.DLQTotal.Inc()
		p.d.Metrics.UnparseableTotal.Inc()
	}
	// alert=true is the log-based alert hook (§15.4 unparseable-event alert).
	log.Error("unparseable event routed to DLQ", "alert", true, "err", cause,
		"stage", string(events.FailedStageConsumeJob), "errorCode", string(events.ErrorCodeValidationFailed))
	p.ack(ctx, msg, log)
	return OutcomeDLQ
}

// fail runs the classified failure branches of FR-014.
func (p *Processor) fail(ctx context.Context, msg queue.Message, ev *events.ExportRequested, deploymentID string,
	rec *deploymentservice.DeploymentRecord, traceID string, ce *errclass.Error,
	log *slog.Logger, start time.Time) Outcome {

	if p.d.Metrics != nil && errclass.IsAuthCode(ce.Code) {
		// §15.4 auth-failure alert feed: any per-service 401/403 occurrence.
		p.d.Metrics.AuthFailuresTotal.WithLabelValues(string(ce.Code)).Inc()
	}

	if ce.Retryable() {
		if msg.Attempt >= p.d.MaxRetries {
			return p.exhausted(ctx, msg, ev, deploymentID, rec, traceID, ce, log)
		}
		return p.scheduleRetry(ctx, msg, deploymentID, traceID, ce, log)
	}

	// Non-retryable: CAS to EXPORT_FAILED, then ack (terminal state rule).
	casErr := p.casFailed(ctx, deploymentID, rec, traceID, ce, log)
	if casErr != nil {
		if errclass.From(casErr, ce.Stage).Retryable() {
			// Transient CAS failure: leave pending so reclaim retries the
			// whole (deterministic) failure path including the CAS.
			log.Error("terminal-failure CAS failed transiently; leaving message pending",
				"err", casErr, "errorCode", string(ce.Code))
			return OutcomeNoAck
		}
		// Persistent CAS failure: preserve evidence in the DLQ, then ack —
		// bounded, never an infinite reclaim loop.
		log.Error("terminal-failure CAS failed persistently; routing to DLQ", "err", casErr, "alert", true)
		entry := p.dlqEntry(msg, ce)
		if dlqErr := p.d.DLQ.SendToDLQ(ctx, entry); dlqErr != nil {
			log.Error("DLQ handoff failed; leaving message pending", "err", dlqErr)
			return OutcomeNoAck
		}
		if p.d.Metrics != nil {
			p.d.Metrics.DLQTotal.Inc()
		}
		p.ack(ctx, msg, log)
		return OutcomeDLQ
	}
	// CAS-then-emit (FR-013): the failure event follows the successful
	// EXPORT_FAILED transition.
	p.emitFailed(ctx, ev, ce, msg.Attempt, false, traceID, log)
	if p.d.Metrics != nil {
		p.d.Metrics.JobsFailedTotal.Inc()
		p.d.Metrics.JobDurationMs.Observe(float64(p.d.Now().Sub(start).Milliseconds()))
	}
	log.Error("job failed terminally", "stage", string(ce.Stage), "errorCode", string(ce.Code),
		"status", string(deploymentservice.DeploymentStatusExportFailed),
		"durationMs", p.d.Now().Sub(start).Milliseconds(), "err", ce.Cause)
	p.ack(ctx, msg, log)
	return OutcomeFailedTerminal
}

// scheduleRetry writes the idempotent retry envelope, then acks
// (write-then-ack; AC-026/AC-027).
func (p *Processor) scheduleRetry(ctx context.Context, msg queue.Message, deploymentID, traceID string,
	ce *errclass.Error, log *slog.Logger) Outcome {

	next := msg.Attempt + 1
	env := queue.RetryEnvelope{
		RetryEnvelopeID: queue.EnvelopeID(deploymentID, next, ce.Code),
		DeploymentID:    deploymentID,
		Attempt:         next,
		NextAttemptAt:   p.d.Now().Add(p.d.Backoff.Delay(next)).UnixMilli(),
		LastErrorCode:   ce.Code,
		TraceID:         traceID,
		Payload:         json.RawMessage(msg.Payload),
	}
	if err := p.d.Retries.Schedule(ctx, env); err != nil {
		log.Error("retry-envelope handoff failed; leaving message pending", "err", err,
			"retryEnvelopeId", env.RetryEnvelopeID)
		return OutcomeNoAck
	}
	if p.d.Metrics != nil {
		p.d.Metrics.RetryTotal.Inc()
	}
	log.Warn("retry scheduled", "stage", string(ce.Stage), "errorCode", string(ce.Code),
		"retryEnvelopeId", env.RetryEnvelopeID, "attempt", next,
		"nextAttemptAt", env.NextAttemptAt, "err", ce.Cause)
	p.ack(ctx, msg, log)
	return OutcomeRetryScheduled
}

// exhausted implements required behavior 4: retryable failure at attempt 3 →
// DLQ + CAS EXPORT_FAILED (+ failed event, which lands with the M3 emitter).
func (p *Processor) exhausted(ctx context.Context, msg queue.Message, ev *events.ExportRequested, deploymentID string,
	rec *deploymentservice.DeploymentRecord, traceID string, ce *errclass.Error, log *slog.Logger) Outcome {

	if casErr := p.casFailed(ctx, deploymentID, rec, traceID, ce, log); casErr != nil {
		// Best effort at exhaustion: four executions already burned on a
		// retryable error; the DLQ entry is the bounded evidence trail.
		log.Error("exhaustion CAS EXPORT_FAILED failed; continuing to DLQ", "err", casErr, "alert", true)
	}
	if err := p.d.DLQ.SendToDLQ(ctx, p.dlqEntry(msg, ce)); err != nil {
		log.Error("DLQ handoff failed at exhaustion; leaving message pending", "err", err)
		return OutcomeNoAck
	}
	p.emitFailed(ctx, ev, ce, msg.Attempt, true, traceID, log)
	if p.d.Metrics != nil {
		p.d.Metrics.DLQTotal.Inc()
		p.d.Metrics.JobsFailedTotal.Inc()
	}
	log.Error("retry exhausted; routed to DLQ", "alert", true,
		"stage", string(ce.Stage), "errorCode", string(ce.Code),
		"attempt", msg.Attempt, "retryExhausted", true, "err", ce.Cause)
	p.ack(ctx, msg, log)
	return OutcomeDLQ
}

// casFailed CASes the record to EXPORT_FAILED from its current status.
// A 409 whose currentStatus is non-actionable counts as success (another
// actor already finished it).
func (p *Processor) casFailed(ctx context.Context, deploymentID string,
	rec *deploymentservice.DeploymentRecord, traceID string, ce *errclass.Error, log *slog.Logger) error {

	if rec == nil || !deployment.Actionable(rec.Status) {
		// No loaded record (load failed / schema-invalid payload) or a
		// non-actionable record: no CAS is possible or needed.
		return nil
	}
	err := p.d.Deploy.Transition(ctx, deploymentID, rec.Status,
		deploymentservice.DeploymentStatusExportFailed,
		string(ce.Code), traceID, ce.Stage)
	if conflict, ok := deployment.AsStatusConflict(err); ok {
		if deployment.NonActionable(conflict.CurrentStatus) {
			return nil
		}
		return conflict
	}
	return err
}

// emitFailed emits deployment.export.failed after a terminal failure
// (FR-013). Emission failures are logged with an alert and never block the
// ack: consumers are duplicate-tolerant and the DLQ + record carry the
// authoritative failure.
func (p *Processor) emitFailed(ctx context.Context, ev *events.ExportRequested, ce *errclass.Error,
	attempt int, retryExhausted bool, traceID string, log *slog.Logger) {
	if p.d.FailedEmit == nil || ev == nil {
		return
	}
	failed := emit.BuildFailed(ev, ce, attempt, retryExhausted, traceID, p.d.Now())
	emitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := p.d.FailedEmit.EmitFailed(emitCtx, failed); err != nil {
		log.Error("deployment.export.failed emission failed", "alert", true, "err", err)
	}
}

func (p *Processor) dlqEntry(msg queue.Message, ce *errclass.Error) queue.DLQEntry {
	return queue.DLQEntry{
		Payload:     msg.Payload,
		ErrorCode:   ce.Code,
		FailedStage: ce.Stage,
		Attempt:     msg.Attempt,
		TraceID:     msg.TraceID,
		WorkerID:    p.d.WorkerID,
		EnqueuedAt:  queue.EnqueueTimeOf(msg.ID),
		FailedAt:    p.d.Now(),
	}
}

// ack acknowledges under the ADR-003 ack rule; failures are logged and left
// to pending recovery (idempotent reprocessing absorbs the redelivery).
func (p *Processor) ack(ctx context.Context, msg queue.Message, log *slog.Logger) {
	ackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	ackCtx, span := obs.StartSpan(ackCtx, "ack_message")
	err := p.d.Consumer.Ack(ackCtx, msg.ID)
	obs.EndSpan(span, err)
	if err != nil {
		log.Error("ack failed; pending recovery will redeliver (idempotent)", "err", err, "messageId", msg.ID)
	}
}

func extractDeploymentID(payload []byte) string {
	var probe struct {
		DeploymentID string `json:"deploymentId"`
	}
	_ = json.Unmarshal(payload, &probe)
	return probe.DeploymentID
}

func newTraceID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
