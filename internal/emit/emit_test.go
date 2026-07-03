// Contract tests for outcome-event emission (AC-023, AC-029): every emitted
// payload validates against the frozen v1 schemas, and the ready event
// carries no routes[] (ADR-001).
package emit_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/ancyloce/anvilkit-export-worker/contracts/deploymentservice"
	"github.com/ancyloce/anvilkit-export-worker/contracts/events"
	"github.com/ancyloce/anvilkit-export-worker/internal/emit"
	"github.com/ancyloce/anvilkit-export-worker/internal/errclass"
	"github.com/ancyloce/anvilkit-export-worker/internal/jsonschema"
	"github.com/ancyloce/anvilkit-export-worker/internal/queue"
)

type capturedAppend struct {
	stream  string
	payload []byte
}

type fakeAppender struct {
	appends []capturedAppend
	err     error
}

func (f *fakeAppender) AppendOutcome(_ context.Context, stream string, payload []byte) (string, error) {
	if f.err != nil {
		return "", f.err
	}
	f.appends = append(f.appends, capturedAppend{stream, payload})
	return "1-0", nil
}

func testRecord() *deploymentservice.DeploymentRecord {
	return &deploymentservice.DeploymentRecord{
		DeploymentID: "dep_01", TeamID: "team_01", SiteID: "site_01",
		PageID: "page_01", Slug: "home", Version: "v12",
		Status: deploymentservice.DeploymentStatusArtifactReady, RenderMode: "fetch_route",
		TargetID: "target_platform_prod", Environment: "production",
	}
}

func testEvent() *events.ExportRequested {
	return &events.ExportRequested{
		EventID: "evt_01", EventType: events.EventTypeExportRequested,
		DeploymentID: "dep_01", TeamID: "team_01", SiteID: "site_01",
		PageID: "page_01", Slug: "home", Version: "v12",
		RenderMode: events.RenderModeFetchRoute, TargetID: "target_platform_prod",
		Environment: events.EnvironmentProduction, IdempotencyKey: "dep_01",
	}
}

// TestEmitReadyValidatesAndOmitsRoutes (AC-023 + AC-029).
func TestEmitReadyValidatesAndOmitsRoutes(t *testing.T) {
	appender := &fakeAppender{}
	emitter := &emit.Emitter{Append: appender}

	ready := emit.BuildReady(emit.ReadyInput{
		Record:             testRecord(),
		ArtifactBasePath:   "sites/site_01/deployments/dep_01",
		ManifestStorageKey: "sites/site_01/deployments/dep_01/artifact-manifest.json",
		ManifestDigest:     "sha256-abc",
		Entry:              "/home/index.html",
		FilesCount:         8,
		TotalBytes:         241022,
		TraceID:            "trace_01",
		Now:                time.Now(),
	})
	if err := emitter.EmitReady(context.Background(), ready); err != nil {
		t.Fatalf("EmitReady: %v", err)
	}
	if len(appender.appends) != 1 || appender.appends[0].stream != queue.StreamArtifactReady {
		t.Fatalf("appends = %+v", appender.appends)
	}
	payload := appender.appends[0].payload
	if violations := jsonschema.ValidateBytes(events.SchemaArtifactReady, payload); len(violations) > 0 {
		t.Fatalf("emitted ready payload fails the frozen schema: %v", violations)
	}
	var asMap map[string]any
	_ = json.Unmarshal(payload, &asMap)
	if _, hasRoutes := asMap["routes"]; hasRoutes {
		t.Fatal("ready event must not carry routes[] (ADR-001/AC-029)")
	}
	if asMap["eventType"] != "deployment.artifact.ready" || asMap["schemaVersion"] != float64(1) {
		t.Errorf("payload discriminators wrong: %v", asMap)
	}
}

// TestEmitFailedValidates (AC-023): the failed payload carries the full §13
// classification vocabulary and validates against the frozen schema.
func TestEmitFailedValidates(t *testing.T) {
	appender := &fakeAppender{}
	emitter := &emit.Emitter{Append: appender}

	ce := errclass.New(events.ErrorCodeRenderOriginTimeout, events.FailedStageRenderHtml, errors.New("slow"))
	failed := emit.BuildFailed(testEvent(), ce, 3, true, "trace_01", time.Now())
	if err := emitter.EmitFailed(context.Background(), failed); err != nil {
		t.Fatalf("EmitFailed: %v", err)
	}
	if appender.appends[0].stream != queue.StreamExportFailed {
		t.Fatalf("stream = %s", appender.appends[0].stream)
	}
	payload := appender.appends[0].payload
	if violations := jsonschema.ValidateBytes(events.SchemaExportFailed, payload); len(violations) > 0 {
		t.Fatalf("emitted failed payload fails the frozen schema: %v", violations)
	}
	var decoded events.ExportFailed
	_ = json.Unmarshal(payload, &decoded)
	if decoded.ErrorCode != events.ErrorCodeRenderOriginTimeout ||
		decoded.ErrorClassification != events.ErrorClassificationRetryable ||
		decoded.FailedStage != events.FailedStageRenderHtml ||
		decoded.Attempt != 3 || !decoded.RetryExhausted {
		t.Errorf("decoded failed event = %+v", decoded)
	}
}

// TestEmitSelfValidationGuard: a payload that violates the frozen schema
// never reaches the wire (bug guard).
func TestEmitSelfValidationGuard(t *testing.T) {
	appender := &fakeAppender{}
	emitter := &emit.Emitter{Append: appender}

	rec := testRecord()
	rec.DeploymentID = "" // violates minLength 1
	ready := emit.BuildReady(emit.ReadyInput{
		Record: rec, ArtifactBasePath: "b", ManifestStorageKey: "k",
		ManifestDigest: "sha256-x", Entry: "/e", TraceID: "t", Now: time.Now(),
	})
	err := emitter.EmitReady(context.Background(), ready)
	if err == nil {
		t.Fatal("schema-invalid payload must not be emitted")
	}
	if len(appender.appends) != 0 {
		t.Fatal("invalid payload reached the wire")
	}
}

// TestEmitTransportFailureClassifiesRetryable: a Redis append failure is a
// transient condition.
func TestEmitTransportFailureClassifiesRetryable(t *testing.T) {
	emitter := &emit.Emitter{Append: &fakeAppender{err: errors.New("redis down")}}
	ce := errclass.New(events.ErrorCodeStorage5xx, events.FailedStageUploadArtifacts, errors.New("x"))
	err := emitter.EmitFailed(context.Background(), emit.BuildFailed(testEvent(), ce, 0, false, "t", time.Now()))
	var classified *errclass.Error
	if !errors.As(err, &classified) || !classified.Retryable() {
		t.Fatalf("transport failure must classify retryable, got %v", err)
	}
}
