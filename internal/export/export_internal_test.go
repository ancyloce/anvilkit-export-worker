// Internal pipeline tests for the update_status_ready stage (span
// correctness + conflict branches) and the whole-bundle size guard —
// unit-level, no Redis/MinIO required.
package export

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/ancyloce/anvilkit-export-worker/contracts/deploymentservice"
	"github.com/ancyloce/anvilkit-export-worker/contracts/events"
	"github.com/ancyloce/anvilkit-export-worker/internal/errclass"
	"github.com/ancyloce/anvilkit-export-worker/internal/harvest"
	"github.com/ancyloce/anvilkit-export-worker/internal/render"
	"github.com/ancyloce/anvilkit-export-worker/internal/worker"
)

// stubDeployments returns canned Transition results.
type stubDeployments struct {
	transitionErr error
	transitions   int
}

func (s *stubDeployments) Transition(context.Context, string,
	deploymentservice.DeploymentStatus, deploymentservice.DeploymentStatus,
	string, string, events.FailedStage) error {
	s.transitions++
	return s.transitionErr
}

func (s *stubDeployments) SubmitArtifact(context.Context, string, deploymentservice.ArtifactPointer) error {
	return nil
}

// recordSpans installs an in-memory tracer provider for the test and
// restores the previous global provider afterwards.
func recordSpans(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		_ = tp.Shutdown(context.Background())
	})
	return sr
}

func endedSpan(t *testing.T, sr *tracetest.SpanRecorder, name string) sdktrace.ReadOnlySpan {
	t.Helper()
	for _, s := range sr.Ended() {
		if s.Name() == name {
			return s
		}
	}
	t.Fatalf("span %q was not ended", name)
	return nil
}

func internalJob() *worker.Job {
	return &worker.Job{
		Record: &deploymentservice.DeploymentRecord{
			DeploymentID: "dep_span", TeamID: "team_01", SiteID: "site_s",
			PageID: "page_01", Slug: "home", Version: "v1",
			Status: deploymentservice.DeploymentStatusExporting, RenderMode: "fetch_route",
			TargetID: "target_platform_prod", Environment: "production",
		},
		TraceID: "0123456789abcdef0123456789abcdef",
		Log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// TestCasReadySpanRecordsError: the update_status_ready span must carry the
// actual CAS error — this test fails against code that ends the span with a
// hardcoded nil.
func TestCasReadySpanRecordsError(t *testing.T) {
	sr := recordSpans(t)
	boom := errors.New("deployment-service unavailable")
	p := &Pipeline{Deploy: &stubDeployments{transitionErr: boom}}

	emit, err := p.casReady(context.Background(), internalJob())
	if !errors.Is(err, boom) || emit {
		t.Fatalf("casReady = emit %v, err %v; want emit=false, the CAS error", emit, err)
	}
	span := endedSpan(t, sr, "update_status_ready")
	if span.Status().Code != codes.Error {
		t.Fatalf("update_status_ready span status = %v, want Error (span must not hide the CAS failure)", span.Status().Code)
	}
	if len(span.Events()) == 0 {
		t.Error("update_status_ready span recorded no error event")
	}
}

// TestCasReadySpanCleanOnSuccess: a successful CAS traces clean and the
// ready event is emitted.
func TestCasReadySpanCleanOnSuccess(t *testing.T) {
	sr := recordSpans(t)
	p := &Pipeline{Deploy: &stubDeployments{}}

	emit, err := p.casReady(context.Background(), internalJob())
	if err != nil || !emit {
		t.Fatalf("casReady = emit %v, err %v; want emit=true, nil", emit, err)
	}
	if span := endedSpan(t, sr, "update_status_ready"); span.Status().Code != codes.Unset {
		t.Fatalf("span status on success = %v, want Unset", span.Status().Code)
	}
}

// TestCasReadyConflictBranches covers the stop-safe 409 resolutions: the
// pipeline may recover, but the span still records the conflict.
func TestCasReadyConflictBranches(t *testing.T) {
	cases := []struct {
		name          string
		currentStatus deploymentservice.DeploymentStatus
		wantEmit      bool
		wantErrCode   events.ErrorCode // "" = no error
	}{
		{"already ARTIFACT_READY re-emits", deploymentservice.DeploymentStatusArtifactReady, true, ""},
		{"CANCELLED stops without event", deploymentservice.DeploymentStatusCancelled, false, ""},
		{"active status is a validation failure", deploymentservice.DeploymentStatusExportQueued, false, events.ErrorCodeValidationFailed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sr := recordSpans(t)
			conflict := &deploymentservice.StatusConflictError{
				ErrorCode: "STATUS_CONFLICT", CurrentStatus: tc.currentStatus,
			}
			p := &Pipeline{Deploy: &stubDeployments{transitionErr: conflict}}

			emit, err := p.casReady(context.Background(), internalJob())
			if emit != tc.wantEmit {
				t.Errorf("emit = %v, want %v", emit, tc.wantEmit)
			}
			if tc.wantErrCode == "" {
				if err != nil {
					t.Fatalf("err = %v, want nil", err)
				}
			} else {
				var ce *errclass.Error
				if !errors.As(err, &ce) || ce.Code != tc.wantErrCode || ce.Retryable() {
					t.Fatalf("err = %v, want non-retryable %s", err, tc.wantErrCode)
				}
			}
			if span := endedSpan(t, sr, "update_status_ready"); span.Status().Code != codes.Error {
				t.Errorf("span status = %v, want Error (the CAS did conflict)", span.Status().Code)
			}
		})
	}
}

// TestTotalArtifactSizeGuard: a harvested bundle over
// MAX_TOTAL_ARTIFACT_BYTES fails non-retryably before any upload — Store is
// nil, so reaching the upload stage would panic the test.
func TestTotalArtifactSizeGuard(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<html><body>" + strings.Repeat("x", 64) + "</body></html>"))
	}))
	defer origin.Close()

	p := &Pipeline{
		Render:                render.New(origin.URL, "test-token", 5*time.Second),
		Deploy:                &stubDeployments{},
		BasePrefix:            "sites",
		Allow:                 harvest.Allowlist{"/assets/*"},
		MaxTotalArtifactBytes: 16,
	}
	err := p.Export(context.Background(), internalJob())
	var ce *errclass.Error
	if !errors.As(err, &ce) || ce.Code != events.ErrorCodeValidationFailed ||
		ce.Stage != events.FailedStageUploadArtifacts || ce.Retryable() {
		t.Fatalf("err = %v, want non-retryable VALIDATION_FAILED at upload_artifacts", err)
	}
	if !strings.Contains(err.Error(), "MAX_TOTAL_ARTIFACT_BYTES") {
		t.Errorf("error must name the limit variable for operators: %v", err)
	}
	if strings.Contains(err.Error(), "test-token") {
		t.Errorf("error leaks the internal service token: %v", err)
	}
}
