// Package deployment owns the worker-side wrapper around the generated
// deploymentservice bindings (FR-004, FR-006; EW-DEPLOY-001..003/005):
// authoritative record load with event-hint reconciliation, CAS status
// transitions restricted to the worker-owned targets, DEPLOYMENT_SERVICE_*
// error classification, and the terminal / non-actionable state decisions
// that gate acks.
package deployment

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/ancyloce/anvilkit-export-worker/contracts/deploymentservice"
	"github.com/ancyloce/anvilkit-export-worker/contracts/events"
	"github.com/ancyloce/anvilkit-export-worker/internal/errclass"
)

// workerOwnedTargets are the only statuses the worker may CAS to (FR-006).
var workerOwnedTargets = map[deploymentservice.DeploymentStatus]bool{
	deploymentservice.DeploymentStatusExporting:     true,
	deploymentservice.DeploymentStatusArtifactReady: true,
	deploymentservice.DeploymentStatusExportFailed:  true,
}

// Client wraps the generated deployment-service client with classification
// and CAS guards.
type Client struct {
	api *deploymentservice.Client
}

// New builds the wrapper. timeout bounds every call (mapped to the retryable
// DEPLOYMENT_SERVICE_TIMEOUT class on expiry).
func New(baseURL, token string, timeout time.Duration) *Client {
	return &Client{api: &deploymentservice.Client{
		BaseURL:    baseURL,
		Token:      token,
		HTTPClient: &http.Client{Timeout: timeout},
	}}
}

// Load fetches the authoritative deployment record — the record, not the
// event, drives the job (FR-004).
func (c *Client) Load(ctx context.Context, deploymentID string) (*deploymentservice.DeploymentRecord, error) {
	rec, err := c.api.GetDeployment(ctx, deploymentID)
	if err != nil {
		return nil, classify(err, events.FailedStageLoadDeployment)
	}
	return rec, nil
}

// Reconcile checks the event hints against the record: the authoritative
// source of truth for renderMode/version/slug is the deployment record
// (PRD 0008 §8.1 G-1); any mismatch is a non-retryable VALIDATION_FAILED.
func Reconcile(rec *deploymentservice.DeploymentRecord, ev *events.ExportRequested) error {
	var mismatches []string
	if string(ev.RenderMode) != rec.RenderMode {
		mismatches = append(mismatches, fmt.Sprintf("renderMode event=%q record=%q", ev.RenderMode, rec.RenderMode))
	}
	if ev.Version != rec.Version {
		mismatches = append(mismatches, fmt.Sprintf("version event=%q record=%q", ev.Version, rec.Version))
	}
	if ev.Slug != rec.Slug {
		mismatches = append(mismatches, fmt.Sprintf("slug event=%q record=%q", ev.Slug, rec.Slug))
	}
	if len(mismatches) > 0 {
		return errclass.New(events.ErrorCodeValidationFailed, events.FailedStageLoadDeployment,
			fmt.Errorf("event hints mismatch the authoritative record: %v", mismatches))
	}
	return nil
}

// Transition performs one CAS status write. Only the worker-owned targets
// are permitted. A 409 comes back as *deploymentservice.StatusConflictError
// (stop-safe handling is the caller's decision); everything else is
// classified at the given stage.
func (c *Client) Transition(ctx context.Context, deploymentID string,
	from, to deploymentservice.DeploymentStatus, reason, traceID string,
	stage events.FailedStage) error {

	if !workerOwnedTargets[to] {
		return fmt.Errorf("BUG: worker attempted CAS to %s — it owns only EXPORTING/ARTIFACT_READY/EXPORT_FAILED (FR-006)", to)
	}
	err := c.api.UpdateDeploymentStatus(ctx, deploymentID, deploymentservice.StatusUpdateRequest{
		From:    from,
		To:      to,
		Reason:  reason,
		TraceID: traceID,
	})
	if err == nil {
		return nil
	}
	var conflict *deploymentservice.StatusConflictError
	if errors.As(err, &conflict) {
		return conflict
	}
	return classify(err, stage)
}

// AsStatusConflict extracts the stop-safe 409 branch.
func AsStatusConflict(err error) (*deploymentservice.StatusConflictError, bool) {
	var conflict *deploymentservice.StatusConflictError
	ok := errors.As(err, &conflict)
	return conflict, ok
}

// NonActionable reports whether the worker must not act on the deployment
// and may ack the message: terminal states (ARTIFACT_READY, EXPORT_FAILED,
// CANCELLED — PRD 0008 §10.4) plus the post-worker pipeline states, which
// imply ARTIFACT_READY already happened.
func NonActionable(s deploymentservice.DeploymentStatus) bool {
	switch s {
	case deploymentservice.DeploymentStatusArtifactReady,
		deploymentservice.DeploymentStatusExportFailed,
		deploymentservice.DeploymentStatusCancelled,
		deploymentservice.DeploymentStatusCdnUploading,
		deploymentservice.DeploymentStatusCdnPurging,
		deploymentservice.DeploymentStatusVerifying,
		deploymentservice.DeploymentStatusActive:
		return true
	}
	return false
}

// Actionable reports whether the worker owns the next step for this status.
func Actionable(s deploymentservice.DeploymentStatus) bool {
	return s == deploymentservice.DeploymentStatusExportQueued ||
		s == deploymentservice.DeploymentStatusExporting
}

// classify maps transport and HTTP failures to the §13 registry:
// timeout/transport/5xx → DEPLOYMENT_SERVICE_TIMEOUT (retryable);
// 401/403 → DEPLOYMENT_SERVICE_401/403 (non-retryable, ops alert);
// 404 and unexpected 4xx → VALIDATION_FAILED (non-retryable).
func classify(err error, stage events.FailedStage) error {
	var apiErr *deploymentservice.APIError
	if errors.As(err, &apiErr) {
		switch {
		case apiErr.StatusCode == http.StatusUnauthorized:
			return errclass.New(events.ErrorCodeDeploymentService401, stage, err)
		case apiErr.StatusCode == http.StatusForbidden:
			return errclass.New(events.ErrorCodeDeploymentService403, stage, err)
		case apiErr.StatusCode == http.StatusNotFound:
			return errclass.New(events.ErrorCodeValidationFailed, stage,
				fmt.Errorf("deployment record not found: %w", err))
		case apiErr.StatusCode >= 500:
			return errclass.New(events.ErrorCodeDeploymentServiceTimeout, stage, err)
		default:
			return errclass.New(events.ErrorCodeValidationFailed, stage, err)
		}
	}
	// Transport error, connection refusal, or client timeout.
	return errclass.New(events.ErrorCodeDeploymentServiceTimeout, stage, err)
}

// SubmitArtifact submits the manifest pointer (FR-012, EW-DEPLOY-004; one
// deploymentId → at most one manifest). Under the BD-004 interim semantics
// (ADR-004) an identical re-POST is accepted idempotently; a 409 therefore
// signals a DIFFERENT pointer already registered — a determinism bug, not a
// transient condition — and classifies non-retryable.
func (c *Client) SubmitArtifact(ctx context.Context, deploymentID string,
	pointer deploymentservice.ArtifactPointer) error {

	err := c.api.SubmitArtifact(ctx, deploymentID, pointer)
	if err == nil {
		return nil
	}
	var apiErr *deploymentservice.APIError
	if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusConflict {
		return errclass.New(events.ErrorCodeValidationFailed, events.FailedStageSubmitArtifact,
			fmt.Errorf("artifact pointer conflict (BD-004/ADR-004: a different pointer is already registered): %w", err))
	}
	return classify(err, events.FailedStageSubmitArtifact)
}
