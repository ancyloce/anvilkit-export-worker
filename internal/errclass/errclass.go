// Package errclass owns job-level error classification (FR-014; PRD 0010 §13,
// PRD 0008 §12.2): every failure carries a registry error code, the pipeline
// stage where it occurred (trace-span vocabulary), and a retryable /
// non-retryable classification that drives the retry-vs-fail branch.
package errclass

import (
	"errors"
	"fmt"

	"github.com/ancyloce/anvilkit-export-worker/contracts/events"
)

// retryable is the exact PRD 0008 §12.2 retryable set. Every other registry
// code is non-retryable; STATUS_CONFLICT is stop-safe (not a failure branch)
// and CONFIG_MISSING is a boot-time failure — both classified non-retryable
// here for completeness.
var retryable = map[events.ErrorCode]bool{
	events.ErrorCodeRenderOriginTimeout:      true,
	events.ErrorCodeRenderOrigin5xx:          true,
	events.ErrorCodeStorageTimeout:           true,
	events.ErrorCodeStorage5xx:               true,
	events.ErrorCodeQueueTemporaryFailure:    true,
	events.ErrorCodeDeploymentServiceTimeout: true,
	events.ErrorCodeArtifactUploadTimeout:    true,
}

// authCodes are the six per-service auth classifications (§11.1) — every
// occurrence raises an ops alert (token rotation/scope misconfiguration).
var authCodes = map[events.ErrorCode]bool{
	events.ErrorCodeRenderOrigin401:      true,
	events.ErrorCodeRenderOrigin403:      true,
	events.ErrorCodeDeploymentService401: true,
	events.ErrorCodeDeploymentService403: true,
	events.ErrorCodeAssetService401:      true,
	events.ErrorCodeAssetService403:      true,
}

// IsAuthCode reports whether code is a per-service auth failure (§11.1).
func IsAuthCode(code events.ErrorCode) bool {
	return authCodes[code]
}

// Retryable reports whether code is in the PRD 0008 §12.2 retryable set.
func Retryable(code events.ErrorCode) bool {
	return retryable[code]
}

// Classification returns the outbound-event classification value for code.
func Classification(code events.ErrorCode) events.ErrorClassification {
	if Retryable(code) {
		return events.ErrorClassificationRetryable
	}
	return events.ErrorClassificationNonRetryable
}

// Error is a classified job failure: registry code + failed stage + cause.
type Error struct {
	Code  events.ErrorCode
	Stage events.FailedStage
	Cause error
}

func (e *Error) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s at %s: %v", e.Code, e.Stage, e.Cause)
	}
	return fmt.Sprintf("%s at %s", e.Code, e.Stage)
}

func (e *Error) Unwrap() error { return e.Cause }

// Retryable reports the classification of this error's code.
func (e *Error) Retryable() bool { return Retryable(e.Code) }

// New builds a classified error.
func New(code events.ErrorCode, stage events.FailedStage, cause error) *Error {
	return &Error{Code: code, Stage: stage, Cause: cause}
}

// From extracts the classified error from err's chain. Unclassified errors
// (bugs, unexpected conditions) default to QUEUE_TEMPORARY_FAILURE at the
// given stage — retryable, so a transient unknown gets its bounded retries
// and then surfaces in the DLQ rather than silently failing a deployment on
// first occurrence (Recommended Approach; §13 registry has no "unknown").
func From(err error, stage events.FailedStage) *Error {
	var ce *Error
	if errors.As(err, &ce) {
		return ce
	}
	return New(events.ErrorCodeQueueTemporaryFailure, stage, err)
}
