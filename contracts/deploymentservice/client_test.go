// Contract tests (EW-CONTRACT-005): generated deployment-service bindings and
// client against the PRD 0010 §8.2 fixture shapes, including bearer auth and
// the 409 STATUS_CONFLICT stop-safe branch (FR-006 groundwork).
package deploymentservice_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/ancyloce/anvilkit-export-worker/contracts/deploymentservice"
)

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return raw
}

func jsonEqual(t *testing.T, want, got []byte) {
	t.Helper()
	var w, g any
	if err := json.Unmarshal(want, &w); err != nil {
		t.Fatalf("parse want: %v", err)
	}
	if err := json.Unmarshal(got, &g); err != nil {
		t.Fatalf("parse got: %v", err)
	}
	if !reflect.DeepEqual(w, g) {
		t.Fatalf("JSON mismatch:\nwant: %s\ngot:  %s", want, got)
	}
}

func TestGetDeployment(t *testing.T) {
	fixture := readFixture(t, "deployment-record.json")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
		if r.URL.Path != "/internal/deployments/dep_01" {
			t.Errorf("path = %s, want /internal/deployments/dep_01", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Errorf("Authorization = %q, want Bearer test-token", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(fixture)
	}))
	defer srv.Close()

	c := &deploymentservice.Client{BaseURL: srv.URL, Token: "test-token"}
	rec, err := c.GetDeployment(context.Background(), "dep_01")
	if err != nil {
		t.Fatalf("GetDeployment: %v", err)
	}
	if rec.DeploymentID != "dep_01" || rec.TeamID != "team_01" || rec.SiteID != "site_01" {
		t.Errorf("record ids = %s/%s/%s", rec.DeploymentID, rec.TeamID, rec.SiteID)
	}
	if rec.Status != deploymentservice.DeploymentStatusExportQueued {
		t.Errorf("Status = %q, want %q", rec.Status, deploymentservice.DeploymentStatusExportQueued)
	}
	if rec.TargetID != "target_platform_prod" {
		t.Errorf("TargetID = %q, want target_platform_prod", rec.TargetID)
	}

	// The record must round-trip losslessly (byte-match discipline, PRD 0010 §8.2).
	remarshaled, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	jsonEqual(t, fixture, remarshaled)
}

func TestUpdateDeploymentStatusCASApplied(t *testing.T) {
	fixture := readFixture(t, "status-update.json")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch {
			t.Errorf("method = %s, want PATCH", r.Method)
		}
		if r.URL.Path != "/internal/deployments/dep_01/status" {
			t.Errorf("path = %s", r.URL.Path)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type = %q", got)
		}
		body, _ := io.ReadAll(r.Body)
		jsonEqual(t, fixture, body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	c := &deploymentservice.Client{BaseURL: srv.URL, Token: "test-token"}
	err := c.UpdateDeploymentStatus(context.Background(), "dep_01", deploymentservice.StatusUpdateRequest{
		From:    deploymentservice.DeploymentStatusExportQueued,
		To:      deploymentservice.DeploymentStatusExporting,
		Reason:  "worker_started",
		TraceID: "trace_01",
	})
	if err != nil {
		t.Fatalf("UpdateDeploymentStatus: %v", err)
	}
}

// TestUpdateDeploymentStatusConflict pins the stop-safe CAS branch: a 409
// decodes into a typed STATUS_CONFLICT error carrying currentStatus
// (PRD 0008 §8.2; the worker stops safely and acks only on terminal states).
func TestUpdateDeploymentStatusConflict(t *testing.T) {
	fixture := readFixture(t, "status-conflict.json")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write(fixture)
	}))
	defer srv.Close()

	c := &deploymentservice.Client{BaseURL: srv.URL, Token: "test-token"}
	err := c.UpdateDeploymentStatus(context.Background(), "dep_01", deploymentservice.StatusUpdateRequest{
		From:    deploymentservice.DeploymentStatusExportQueued,
		To:      deploymentservice.DeploymentStatusExporting,
		Reason:  "worker_started",
		TraceID: "trace_01",
	})
	var conflict *deploymentservice.StatusConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("err = %v, want *StatusConflictError", err)
	}
	if conflict.CurrentStatus != deploymentservice.DeploymentStatusArtifactReady {
		t.Errorf("CurrentStatus = %q, want ARTIFACT_READY", conflict.CurrentStatus)
	}
	if conflict.Error() == "" {
		t.Error("StatusConflictError must satisfy the error interface with a message")
	}
}

func TestSubmitArtifact(t *testing.T) {
	fixture := readFixture(t, "artifact-pointer.json")
	var pointer deploymentservice.ArtifactPointer
	if err := json.Unmarshal(fixture, &pointer); err != nil {
		t.Fatalf("unmarshal artifact-pointer fixture: %v", err)
	}
	// routes[] is always an array in the submission (FR-012 invariant) —
	// unlike the ready event, which omits it (ADR-001).
	if pointer.Routes == nil {
		t.Fatal("ArtifactPointer.Routes must be present in the fixture")
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/internal/deployments/dep_01/artifact" {
			t.Errorf("path = %s", r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		jsonEqual(t, fixture, body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	c := &deploymentservice.Client{BaseURL: srv.URL, Token: "test-token"}
	if err := c.SubmitArtifact(context.Background(), "dep_01", pointer); err != nil {
		t.Fatalf("SubmitArtifact: %v", err)
	}
}

func TestUndeclaredErrorIsAPIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := &deploymentservice.Client{BaseURL: srv.URL, Token: "test-token"}
	_, err := c.GetDeployment(context.Background(), "dep_01")
	var apiErr *deploymentservice.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("err = %v, want *APIError", err)
	}
	if apiErr.StatusCode != http.StatusInternalServerError {
		t.Errorf("StatusCode = %d, want 500", apiErr.StatusCode)
	}
}
