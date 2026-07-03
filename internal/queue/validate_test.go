package queue_test

import (
	"errors"
	"os"
	"testing"

	"github.com/ancyloce/anvilkit-export-worker/contracts/events"
	"github.com/ancyloce/anvilkit-export-worker/internal/errclass"
	"github.com/ancyloce/anvilkit-export-worker/internal/queue"
)

func validPayload(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile("../../contracts/events/testdata/deployment.export.requested.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return raw
}

func TestParseEventValid(t *testing.T) {
	ev, err := queue.ParseEvent(validPayload(t))
	if err != nil {
		t.Fatalf("ParseEvent: %v", err)
	}
	if ev.DeploymentID != "dep_01" || ev.RenderMode != events.RenderModeFetchRoute {
		t.Errorf("decoded event = %+v", ev)
	}
}

// TestParseEventUnparseable: no extractable deploymentId → the DLQ route
// (FR-003), distinguished from schema-invalid.
func TestParseEventUnparseable(t *testing.T) {
	cases := map[string][]byte{
		"invalid JSON":            []byte(`{not json`),
		"not an object":           []byte(`[1,2,3]`),
		"missing deploymentId":    []byte(`{"eventType":"deployment.export.requested"}`),
		"non-string deploymentId": []byte(`{"deploymentId": 42}`),
		"empty deploymentId":      []byte(`{"deploymentId": "  "}`),
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := queue.ParseEvent(payload)
			if !errors.Is(err, queue.ErrUnparseable) {
				t.Fatalf("want ErrUnparseable, got %v", err)
			}
		})
	}
}

// TestParseEventSchemaInvalid: deploymentId extractable but schema-invalid →
// classified non-retryable VALIDATION_FAILED (§13), never the DLQ route.
func TestParseEventSchemaInvalid(t *testing.T) {
	payload := []byte(`{
		"eventId":"evt_01","eventType":"deployment.export.requested",
		"deploymentId":"dep_01","teamId":"team_01","siteId":"site_01",
		"pageId":"page_01","slug":"home","version":"v12",
		"renderMode":"paint","targetId":"t1","environment":"production",
		"idempotencyKey":"k1"}`)
	_, err := queue.ParseEvent(payload)
	if errors.Is(err, queue.ErrUnparseable) {
		t.Fatal("schema-invalid with deploymentId must not be the unparseable route")
	}
	var ce *errclass.Error
	if !errors.As(err, &ce) {
		t.Fatalf("want classified error, got %v", err)
	}
	if ce.Code != events.ErrorCodeValidationFailed || ce.Retryable() {
		t.Errorf("want non-retryable VALIDATION_FAILED, got %s retryable=%v", ce.Code, ce.Retryable())
	}
	if ce.Stage != events.FailedStageConsumeJob {
		t.Errorf("stage = %s, want consume_job", ce.Stage)
	}
}

func TestParseEventMissingRequiredField(t *testing.T) {
	payload := []byte(`{
		"eventId":"evt_01","eventType":"deployment.export.requested",
		"deploymentId":"dep_01","teamId":"team_01","siteId":"site_01",
		"pageId":"page_01","version":"v12",
		"renderMode":"fetch_route","targetId":"t1","environment":"production",
		"idempotencyKey":"k1"}`)
	var ce *errclass.Error
	if _, err := queue.ParseEvent(payload); !errors.As(err, &ce) || ce.Code != events.ErrorCodeValidationFailed {
		t.Fatalf("missing slug must classify VALIDATION_FAILED, got %v", err)
	}
}
