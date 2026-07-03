// Package emit owns outcome-event construction and emission (FR-013,
// EW-EVENT-001..003): deployment.artifact.ready after the CAS to
// ARTIFACT_READY succeeds, deployment.export.failed after a terminal
// failure. Every payload is validated against its frozen v1 schema before
// emission (AC-023) — the worker never puts a contract-invalid event on the
// wire. Ordering is CAS-then-emit with at-least-once delivery; consumers
// dedupe by deploymentId (ADR-005 default; full resolution gates cdn-service
// integration tests, AC-034).
package emit

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ancyloce/anvilkit-export-worker/contracts/deploymentservice"
	"github.com/ancyloce/anvilkit-export-worker/contracts/events"
	"github.com/ancyloce/anvilkit-export-worker/internal/errclass"
	"github.com/ancyloce/anvilkit-export-worker/internal/jsonschema"
	"github.com/ancyloce/anvilkit-export-worker/internal/queue"
)

// Appender is the transport seam (satisfied by *queue.RedisDriver).
type Appender interface {
	AppendOutcome(ctx context.Context, stream string, payload []byte) (string, error)
}

// Emitter validates and emits outcome events.
type Emitter struct {
	Append Appender
}

// NewEventID mints an outbound event id.
func NewEventID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return "evt_" + hex.EncodeToString(b[:])
}

// ReadyInput carries everything the ready payload needs beyond the record.
type ReadyInput struct {
	Record             *deploymentservice.DeploymentRecord
	ArtifactBasePath   string
	ManifestStorageKey string
	ManifestDigest     string
	Entry              string
	FilesCount         int64
	TotalBytes         int64
	TraceID            string
	Now                time.Time
}

// BuildReady assembles the minimum deployment.artifact.ready payload
// (PRD 0010 §10.3.2; routes[] intentionally omitted per ADR-001).
func BuildReady(in ReadyInput) events.ArtifactReady {
	return events.ArtifactReady{
		SchemaVersion:      1,
		EventID:            NewEventID(),
		EventType:          events.EventTypeArtifactReady,
		DeploymentID:       in.Record.DeploymentID,
		TeamID:             in.Record.TeamID,
		SiteID:             in.Record.SiteID,
		PageID:             in.Record.PageID,
		Slug:               in.Record.Slug,
		Version:            in.Record.Version,
		Environment:        events.Environment(in.Record.Environment),
		RenderMode:         events.RenderMode(in.Record.RenderMode),
		ArtifactBasePath:   in.ArtifactBasePath,
		ManifestStorageKey: in.ManifestStorageKey,
		ManifestDigest:     in.ManifestDigest,
		Entry:              in.Entry,
		FilesCount:         in.FilesCount,
		TotalBytes:         in.TotalBytes,
		TraceID:            in.TraceID,
		CreatedAt:          in.Now.UTC().Format(time.RFC3339),
	}
}

// BuildFailed assembles the minimum deployment.export.failed payload
// (PRD 0010 §10.3.2) from the inbound event and the classified failure.
func BuildFailed(ev *events.ExportRequested, ce *errclass.Error, attempt int, retryExhausted bool,
	traceID string, now time.Time) events.ExportFailed {
	return events.ExportFailed{
		SchemaVersion:       1,
		EventID:             NewEventID(),
		EventType:           events.EventTypeExportFailed,
		DeploymentID:        ev.DeploymentID,
		TeamID:              ev.TeamID,
		SiteID:              ev.SiteID,
		PageID:              ev.PageID,
		Slug:                ev.Slug,
		Version:             ev.Version,
		Environment:         ev.Environment,
		RenderMode:          ev.RenderMode,
		ErrorCode:           ce.Code,
		ErrorClassification: errclass.Classification(ce.Code),
		FailedStage:         ce.Stage,
		Attempt:             int64(attempt),
		RetryExhausted:      retryExhausted,
		TraceID:             traceID,
		CreatedAt:           now.UTC().Format(time.RFC3339),
	}
}

// EmitReady validates and emits deployment.artifact.ready.
func (e *Emitter) EmitReady(ctx context.Context, ev events.ArtifactReady) error {
	return e.emit(ctx, queue.StreamArtifactReady, events.SchemaArtifactReady, ev, events.FailedStageEmitReady)
}

// EmitFailed validates and emits deployment.export.failed.
func (e *Emitter) EmitFailed(ctx context.Context, ev events.ExportFailed) error {
	return e.emit(ctx, queue.StreamExportFailed, events.SchemaExportFailed, ev, events.FailedStageEmitReady)
}

func (e *Emitter) emit(ctx context.Context, stream, schema string, ev any, stage events.FailedStage) error {
	payload, err := json.Marshal(ev)
	if err != nil {
		return errclass.New(events.ErrorCodeValidationFailed, stage, err)
	}
	if violations := jsonschema.ValidateBytes(schema, payload); len(violations) > 0 {
		// Bug guard: an event the worker cannot validate never reaches the
		// wire (AC-023).
		return errclass.New(events.ErrorCodeValidationFailed, stage,
			fmt.Errorf("outbound event failed schema self-validation: %s", strings.Join(violations, "; ")))
	}
	if _, err := e.Append.AppendOutcome(ctx, stream, payload); err != nil {
		return errclass.New(events.ErrorCodeQueueTemporaryFailure, stage, err)
	}
	return nil
}
