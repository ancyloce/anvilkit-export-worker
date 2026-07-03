// Permission and classification tests for the deployment-service wrapper
// (EW-TEST-008, AC-022): per-service 401/403 codes are non-retryable;
// timeouts/5xx retryable; 404 and pointer conflicts terminal.
package deployment_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ancyloce/anvilkit-export-worker/contracts/deploymentservice"
	"github.com/ancyloce/anvilkit-export-worker/contracts/events"
	"github.com/ancyloce/anvilkit-export-worker/internal/deployment"
	"github.com/ancyloce/anvilkit-export-worker/internal/errclass"
)

func statusServer(t *testing.T, status int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", status)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func classifiedLoad(t *testing.T, srvURL string) *errclass.Error {
	t.Helper()
	c := deployment.New(srvURL, "test-token", 2*time.Second)
	_, err := c.Load(context.Background(), "dep_01")
	var ce *errclass.Error
	if !errors.As(err, &ce) {
		t.Fatalf("err = %v, want classified", err)
	}
	return ce
}

// TestAuthClassification (AC-022): 401/403 from deployment-service classify
// as the per-service auth codes, non-retryable (no retry storms on a bad or
// rotated-out token — the alert feed fires instead).
func TestAuthClassification(t *testing.T) {
	ce := classifiedLoad(t, statusServer(t, http.StatusUnauthorized).URL)
	if ce.Code != events.ErrorCodeDeploymentService401 || ce.Retryable() {
		t.Errorf("401: code=%s retryable=%v, want DEPLOYMENT_SERVICE_401 non-retryable", ce.Code, ce.Retryable())
	}
	if !errclass.IsAuthCode(ce.Code) {
		t.Error("DEPLOYMENT_SERVICE_401 must feed the auth-failure alert (§15.4)")
	}

	ce = classifiedLoad(t, statusServer(t, http.StatusForbidden).URL)
	if ce.Code != events.ErrorCodeDeploymentService403 || ce.Retryable() {
		t.Errorf("403: code=%s retryable=%v, want DEPLOYMENT_SERVICE_403 non-retryable", ce.Code, ce.Retryable())
	}
}

func TestTransientClassification(t *testing.T) {
	ce := classifiedLoad(t, statusServer(t, http.StatusInternalServerError).URL)
	if ce.Code != events.ErrorCodeDeploymentServiceTimeout || !ce.Retryable() {
		t.Errorf("5xx: code=%s retryable=%v, want DEPLOYMENT_SERVICE_TIMEOUT retryable", ce.Code, ce.Retryable())
	}

	// Connection refused behaves like an unreachable service.
	c := deployment.New("http://127.0.0.1:1", "test-token", 300*time.Millisecond)
	_, err := c.Load(context.Background(), "dep_01")
	var ce2 *errclass.Error
	if !errors.As(err, &ce2) || ce2.Code != events.ErrorCodeDeploymentServiceTimeout || !ce2.Retryable() {
		t.Errorf("refused: %v, want retryable DEPLOYMENT_SERVICE_TIMEOUT", err)
	}
}

func TestRecordNotFoundIsTerminal(t *testing.T) {
	ce := classifiedLoad(t, statusServer(t, http.StatusNotFound).URL)
	if ce.Code != events.ErrorCodeValidationFailed || ce.Retryable() {
		t.Errorf("404: code=%s retryable=%v, want non-retryable VALIDATION_FAILED", ce.Code, ce.Retryable())
	}
}

// TestSubmitArtifactConflictIsTerminal: under BD-004/ADR-004 idempotent
// accept, a 409 means a DIFFERENT pointer exists — a determinism bug, never
// retried.
func TestSubmitArtifactConflictIsTerminal(t *testing.T) {
	srv := statusServer(t, http.StatusConflict)
	c := deployment.New(srv.URL, "test-token", 2*time.Second)
	err := c.SubmitArtifact(context.Background(), "dep_01", deploymentservice.ArtifactPointer{
		ManifestStorageKey: "k", ArtifactBasePath: "b", ManifestDigest: "sha256-x",
		Entry: "/e", FilesCount: 1, TotalBytes: 1, Routes: []deploymentservice.ArtifactRoute{},
	})
	var ce *errclass.Error
	if !errors.As(err, &ce) || ce.Code != events.ErrorCodeValidationFailed || ce.Retryable() {
		t.Fatalf("pointer 409: %v, want non-retryable VALIDATION_FAILED", err)
	}
	if ce.Stage != events.FailedStageSubmitArtifact {
		t.Errorf("stage = %s, want submit_artifact", ce.Stage)
	}
}
