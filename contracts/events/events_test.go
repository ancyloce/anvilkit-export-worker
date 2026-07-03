// Contract tests (EW-CONTRACT-005): the generated bindings must round-trip
// the fixture payloads of contracts/events/v1 without loss. Fixture testdata
// is copied by the codegen pipeline; platform CI regenerates and fails on
// drift, so a schema change without matching bindings breaks this suite.
package events_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/ancyloce/anvilkit-export-worker/contracts/events"
)

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return raw
}

// roundTrip unmarshals a fixture into the generated type, marshals it back,
// and requires semantic equality — a dropped, renamed, or mistyped field on
// the Go side fails here.
func roundTrip[T any](t *testing.T, fixture string) T {
	t.Helper()
	raw := readFixture(t, fixture)
	var typed T
	if err := json.Unmarshal(raw, &typed); err != nil {
		t.Fatalf("unmarshal %s into %T: %v", fixture, typed, err)
	}
	remarshaled, err := json.Marshal(typed)
	if err != nil {
		t.Fatalf("marshal %T: %v", typed, err)
	}
	var want, got map[string]any
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatalf("parse fixture %s: %v", fixture, err)
	}
	if err := json.Unmarshal(remarshaled, &got); err != nil {
		t.Fatalf("parse remarshaled %T: %v", typed, err)
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("%s does not round-trip through %T:\nfixture: %v\nresult:  %v", fixture, typed, want, got)
	}
	return typed
}

func TestExportRequestedRoundTrip(t *testing.T) {
	ev := roundTrip[events.ExportRequested](t, "deployment.export.requested.json")
	if ev.EventType != events.EventTypeExportRequested {
		t.Errorf("EventType = %q, want %q", ev.EventType, events.EventTypeExportRequested)
	}
	if ev.DeploymentID != "dep_01" {
		t.Errorf("DeploymentID = %q, want dep_01", ev.DeploymentID)
	}
	if ev.RenderMode != events.RenderModeFetchRoute {
		t.Errorf("RenderMode = %q, want %q", ev.RenderMode, events.RenderModeFetchRoute)
	}
	if ev.IdempotencyKey == "" {
		t.Error("IdempotencyKey must be present (required by the schema)")
	}
}

func TestArtifactReadyRoundTrip(t *testing.T) {
	ev := roundTrip[events.ArtifactReady](t, "deployment.artifact.ready.json")
	if ev.EventType != events.EventTypeArtifactReady {
		t.Errorf("EventType = %q, want %q", ev.EventType, events.EventTypeArtifactReady)
	}
	if ev.SchemaVersion != 1 {
		t.Errorf("SchemaVersion = %d, want 1", ev.SchemaVersion)
	}
	if ev.FilesCount != 8 || ev.TotalBytes != 241022 {
		t.Errorf("FilesCount/TotalBytes = %d/%d, want 8/241022", ev.FilesCount, ev.TotalBytes)
	}
	if ev.ManifestStorageKey != "sites/site_xxx/deployments/dep_xxx/artifact-manifest.json" {
		t.Errorf("ManifestStorageKey = %q", ev.ManifestStorageKey)
	}
}

// TestArtifactReadyOmitsRoutes pins the ADR-001 / AC-029 decision: the ready
// event carries no routes[] — route data lives in artifact-manifest.json.
func TestArtifactReadyOmitsRoutes(t *testing.T) {
	var asMap map[string]any
	if err := json.Unmarshal(readFixture(t, "deployment.artifact.ready.json"), &asMap); err != nil {
		t.Fatal(err)
	}
	if _, ok := asMap["routes"]; ok {
		t.Fatal("deployment.artifact.ready fixture contains routes[] — forbidden by ADR-001 (BD-001)")
	}
	if _, ok := reflect.TypeOf(events.ArtifactReady{}).FieldByName("Routes"); ok {
		t.Fatal("events.ArtifactReady has a Routes field — forbidden by ADR-001 (BD-001)")
	}
}

func TestExportFailedRoundTrip(t *testing.T) {
	ev := roundTrip[events.ExportFailed](t, "deployment.export.failed.json")
	if ev.EventType != events.EventTypeExportFailed {
		t.Errorf("EventType = %q, want %q", ev.EventType, events.EventTypeExportFailed)
	}
	if ev.ErrorCode != events.ErrorCodeRenderOriginTimeout {
		t.Errorf("ErrorCode = %q, want %q", ev.ErrorCode, events.ErrorCodeRenderOriginTimeout)
	}
	if ev.ErrorClassification != events.ErrorClassificationRetryable {
		t.Errorf("ErrorClassification = %q, want %q", ev.ErrorClassification, events.ErrorClassificationRetryable)
	}
	if ev.FailedStage != events.FailedStageRenderHtml {
		t.Errorf("FailedStage = %q, want %q", ev.FailedStage, events.FailedStageRenderHtml)
	}
	// Exhaustion semantics of the fixture: attempt 3 + retryExhausted true
	// (maxRetries = 3 → attempt values 0..3, four executions max).
	if ev.Attempt != 3 || !ev.RetryExhausted {
		t.Errorf("Attempt/RetryExhausted = %d/%v, want 3/true", ev.Attempt, ev.RetryExhausted)
	}
}

func TestEventTypeDiscriminators(t *testing.T) {
	want := map[string]string{
		events.EventTypeExportRequested: "deployment.export.requested",
		events.EventTypeArtifactReady:   "deployment.artifact.ready",
		events.EventTypeExportFailed:    "deployment.export.failed",
	}
	for got, expected := range want {
		if got != expected {
			t.Errorf("event type constant = %q, want %q", got, expected)
		}
	}
}

func TestEnumVocabulary(t *testing.T) {
	cases := []struct {
		got, want string
	}{
		{string(events.RenderModeFetchRoute), "fetch_route"},
		{string(events.RenderModeReactSsr), "react_ssr"},
		{string(events.RenderModeHtmlExport), "html_export"},
		{string(events.EnvironmentProduction), "production"},
		{string(events.EnvironmentPreview), "preview"},
		{string(events.ErrorClassificationRetryable), "RETRYABLE"},
		{string(events.ErrorClassificationNonRetryable), "NON_RETRYABLE"},
		{string(events.FailedStageConsumeJob), "consume_job"},
		{string(events.FailedStageAckMessage), "ack_message"},
		{string(events.ErrorCodeValidationFailed), "VALIDATION_FAILED"},
		{string(events.ErrorCodeRenderOrigin5xx), "RENDER_ORIGIN_5XX"},
		{string(events.ErrorCodeUnsupportedDynamicImageOptimizer), "UNSUPPORTED_DYNAMIC_IMAGE_OPTIMIZER"},
		{string(events.ErrorCodePathTraversalDetected), "PATH_TRAVERSAL_DETECTED"},
		{string(events.ErrorCodeAssetService403), "ASSET_SERVICE_403"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("enum constant = %q, want %q", c.got, c.want)
		}
	}
}

// TestSchemaSourcesEmbedded verifies the embedded schema sources are intact
// JSON documents so the worker can validate payloads offline.
func TestSchemaSourcesEmbedded(t *testing.T) {
	for name, src := range map[string]string{
		"SchemaExportRequested": events.SchemaExportRequested,
		"SchemaArtifactReady":   events.SchemaArtifactReady,
		"SchemaExportFailed":    events.SchemaExportFailed,
	} {
		var doc map[string]any
		if err := json.Unmarshal([]byte(src), &doc); err != nil {
			t.Errorf("%s is not valid JSON: %v", name, err)
			continue
		}
		for _, key := range []string{"$id", "title", "required", "properties"} {
			if _, ok := doc[key]; !ok {
				t.Errorf("%s missing %q", name, key)
			}
		}
	}
}
