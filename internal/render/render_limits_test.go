// Same-origin size-limit tests (M5 hardening): oversized render output is a
// broken render contract — non-retryable VALIDATION_FAILED, with error text
// that names the §14 limit and never leaks the bearer token or body bytes.
package render_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ancyloce/anvilkit-export-worker/contracts/events"
	"github.com/ancyloce/anvilkit-export-worker/internal/errclass"
	"github.com/ancyloce/anvilkit-export-worker/internal/render"
)

func originServing(t *testing.T, contentType string, body []byte) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", contentType)
		_, _ = w.Write(body)
	}))
}

func wantOversize(t *testing.T, err error, stage events.FailedStage, limitVar string) {
	t.Helper()
	var ce *errclass.Error
	if !errors.As(err, &ce) || ce.Code != events.ErrorCodeValidationFailed || ce.Stage != stage {
		t.Fatalf("err = %v, want VALIDATION_FAILED at %s", err, stage)
	}
	if ce.Retryable() {
		t.Error("oversized render output must be non-retryable")
	}
	msg := err.Error()
	if !strings.Contains(msg, limitVar) {
		t.Errorf("error must name %s for operators: %v", limitVar, msg)
	}
	if strings.Contains(msg, "secret-token") {
		t.Errorf("error leaks the bearer token: %v", msg)
	}
	if strings.Contains(msg, "AAAA") {
		t.Errorf("error leaks body content: %v", msg)
	}
}

func TestFetchPageOversizedHTML(t *testing.T) {
	page := []byte("<html>" + strings.Repeat("A", 64) + "</html>")
	srv := originServing(t, "text/html", page)
	defer srv.Close()

	c := render.New(srv.URL, "secret-token", 5*time.Second)
	c.MaxHTMLBytes = 32
	_, err := c.FetchPage(context.Background(), "home", testPin())
	if err == nil {
		t.Fatal("oversized HTML must fail")
	}
	wantOversize(t, err, events.FailedStageRenderHtml, "MAX_RENDER_HTML_BYTES")

	// Exactly at the limit passes — the bound is a ceiling, not a fuzz.
	c.MaxHTMLBytes = int64(len(page))
	if _, err := c.FetchPage(context.Background(), "home", testPin()); err != nil {
		t.Fatalf("page exactly at MAX_RENDER_HTML_BYTES must pass: %v", err)
	}
}

func TestFetchAssetOversized(t *testing.T) {
	body := []byte(strings.Repeat("A", 48))
	srv := originServing(t, "image/png", body)
	defer srv.Close()

	c := render.New(srv.URL, "secret-token", 5*time.Second)
	c.MaxAssetBytes = 16
	_, err := c.FetchAsset(context.Background(), "/assets/hero.png", testPin())
	if err == nil {
		t.Fatal("oversized asset must fail")
	}
	wantOversize(t, err, events.FailedStageHarvestDependencies, "MAX_RENDER_ASSET_BYTES")

	c.MaxAssetBytes = int64(len(body))
	if _, err := c.FetchAsset(context.Background(), "/assets/hero.png", testPin()); err != nil {
		t.Fatalf("asset exactly at MAX_RENDER_ASSET_BYTES must pass: %v", err)
	}
}

// TestDefaultLimitsApply: with no explicit limits the package defaults
// bound the read — ordinary pages are far inside them.
func TestDefaultLimitsApply(t *testing.T) {
	srv := originServing(t, "text/html", []byte("<html>ok</html>"))
	defer srv.Close()

	c := render.New(srv.URL, "secret-token", 5*time.Second)
	if _, err := c.FetchPage(context.Background(), "home", testPin()); err != nil {
		t.Fatalf("default limits must not reject an ordinary page: %v", err)
	}
}
