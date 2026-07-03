package errclass

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/ancyloce/anvilkit-export-worker/contracts/events"
)

// registryCodes extracts the full errorCode enum from the embedded frozen
// schema, so this test tracks the contract of record, not a hand-copied list.
func registryCodes(t *testing.T) []events.ErrorCode {
	t.Helper()
	var schema struct {
		Properties struct {
			ErrorCode struct {
				Enum []events.ErrorCode `json:"enum"`
			} `json:"errorCode"`
		} `json:"properties"`
	}
	if err := json.Unmarshal([]byte(events.SchemaExportFailed), &schema); err != nil {
		t.Fatalf("parse embedded schema: %v", err)
	}
	if len(schema.Properties.ErrorCode.Enum) == 0 {
		t.Fatal("no errorCode enum in embedded schema")
	}
	return schema.Properties.ErrorCode.Enum
}

// TestClassificationCompleteness: every registry code has a decided
// classification, and the retryable set is exactly PRD 0008 §12.2.
func TestClassificationCompleteness(t *testing.T) {
	wantRetryable := map[events.ErrorCode]bool{
		events.ErrorCodeRenderOriginTimeout:      true,
		events.ErrorCodeRenderOrigin5xx:          true,
		events.ErrorCodeStorageTimeout:           true,
		events.ErrorCodeStorage5xx:               true,
		events.ErrorCodeQueueTemporaryFailure:    true,
		events.ErrorCodeDeploymentServiceTimeout: true,
		events.ErrorCodeArtifactUploadTimeout:    true,
	}
	retryableCount := 0
	for _, code := range registryCodes(t) {
		got := Retryable(code)
		if got != wantRetryable[code] {
			t.Errorf("Retryable(%s) = %v, want %v", code, got, wantRetryable[code])
		}
		if got {
			retryableCount++
		}
	}
	if retryableCount != len(wantRetryable) {
		t.Errorf("retryable set size = %d, want %d (PRD 0008 §12.2)", retryableCount, len(wantRetryable))
	}
}

// TestAuthCodesNonRetryable pins the §11.1 rule: every per-service auth
// failure is non-retryable (ops alert, never a retry storm on a bad token).
func TestAuthCodesNonRetryable(t *testing.T) {
	for _, code := range []events.ErrorCode{
		events.ErrorCodeRenderOrigin401, events.ErrorCodeRenderOrigin403,
		events.ErrorCodeDeploymentService401, events.ErrorCodeDeploymentService403,
		events.ErrorCodeAssetService401, events.ErrorCodeAssetService403,
	} {
		if Retryable(code) {
			t.Errorf("%s must be non-retryable (§11.1)", code)
		}
	}
}

func TestClassificationValues(t *testing.T) {
	if Classification(events.ErrorCodeRenderOriginTimeout) != events.ErrorClassificationRetryable {
		t.Error("RENDER_ORIGIN_TIMEOUT must classify RETRYABLE")
	}
	if Classification(events.ErrorCodeValidationFailed) != events.ErrorClassificationNonRetryable {
		t.Error("VALIDATION_FAILED must classify NON_RETRYABLE")
	}
}

func TestFromExtractsClassifiedError(t *testing.T) {
	orig := New(events.ErrorCodeValidationFailed, events.FailedStageLoadDeployment, errors.New("hint mismatch"))
	wrapped := fmt.Errorf("job failed: %w", orig)
	got := From(wrapped, events.FailedStageConsumeJob)
	if got.Code != events.ErrorCodeValidationFailed || got.Stage != events.FailedStageLoadDeployment {
		t.Errorf("From lost classification: %+v", got)
	}
}

// TestFromDefaultsUnknownToRetryable: an unclassified bug gets bounded
// retries + DLQ visibility instead of an immediate terminal failure.
func TestFromDefaultsUnknownToRetryable(t *testing.T) {
	got := From(errors.New("some bug"), events.FailedStageRenderHtml)
	if got.Code != events.ErrorCodeQueueTemporaryFailure {
		t.Errorf("unknown error code = %s, want QUEUE_TEMPORARY_FAILURE", got.Code)
	}
	if got.Stage != events.FailedStageRenderHtml {
		t.Errorf("stage = %s, want render_html", got.Stage)
	}
	if !got.Retryable() {
		t.Error("unknown errors must default retryable")
	}
}
