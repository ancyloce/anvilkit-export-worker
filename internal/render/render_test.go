package render_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"
	"time"

	"github.com/ancyloce/anvilkit-export-worker/contracts/events"
	"github.com/ancyloce/anvilkit-export-worker/internal/errclass"
	"github.com/ancyloce/anvilkit-export-worker/internal/render"
)

func testPin() render.Pin {
	return render.Pin{
		DeploymentID: "dep_01", PageID: "page_01", Version: "v12",
		TeamID: "team_01", SiteID: "site_01", Environment: "production",
		TraceID: "0123456789abcdef0123456789abcdef",
	}
}

// TestPinningHeaders asserts the full FR-007 header contract, including the
// forwarded W3C trace context (EW-RENDER-001 DoD).
func TestPinningHeaders(t *testing.T) {
	var got http.Header
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte("<html></html>"))
	}))
	defer srv.Close()

	c := render.New(srv.URL, "test-token", 5*time.Second)
	if _, err := c.FetchPage(context.Background(), "blog/hello", testPin()); err != nil {
		t.Fatalf("FetchPage: %v", err)
	}
	if gotPath != "/blog/hello" {
		t.Errorf("path = %q, want /blog/hello (nested slug preserved)", gotPath)
	}
	want := map[string]string{
		"Authorization":            "Bearer test-token",
		"X-Anvilkit-Render-Worker": "true",
		"X-Anvilkit-Deployment-Id": "dep_01",
		"X-Anvilkit-Page-Id":       "page_01",
		"X-Anvilkit-Version":       "v12",
		"X-Anvilkit-Team-Id":       "team_01",
		"X-Anvilkit-Site-Id":       "site_01",
		"X-Anvilkit-Environment":   "production",
	}
	for header, expected := range want {
		if got.Get(header) != expected {
			t.Errorf("header %s = %q, want %q", header, got.Get(header), expected)
		}
	}
	tp := got.Get("Traceparent")
	if !regexp.MustCompile(`^00-0123456789abcdef0123456789abcdef-[0-9a-f]{16}-01$`).MatchString(tp) {
		t.Errorf("traceparent = %q — trace context must be forwarded (FR-007)", tp)
	}
}

// TestPreviewPinningPath (EW-RENDER-004): preview uses the same
// version-pinned fetch with environment=preview in the pinning headers.
func TestPreviewPinningPath(t *testing.T) {
	var env string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		env = r.Header.Get("X-AnvilKit-Environment")
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<html></html>"))
	}))
	defer srv.Close()

	pin := testPin()
	pin.Environment = "preview"
	c := render.New(srv.URL, "test-token", 5*time.Second)
	if _, err := c.FetchPage(context.Background(), "home", pin); err != nil {
		t.Fatal(err)
	}
	if env != "preview" {
		t.Errorf("X-AnvilKit-Environment = %q, want preview", env)
	}
}

// TestResponseClassification is EW-RENDER-002: every branch of the
// PRD 0010 §8.3 error mapping.
func TestResponseClassification(t *testing.T) {
	cases := []struct {
		status    int
		wantCode  events.ErrorCode
		retryable bool
	}{
		{http.StatusUnauthorized, events.ErrorCodeRenderOrigin401, false},
		{http.StatusForbidden, events.ErrorCodeRenderOrigin403, false},
		{http.StatusNotFound, events.ErrorCodeRenderOrigin404, false},
		{http.StatusConflict, events.ErrorCodeVersionSlugMismatch, false},
		{http.StatusInternalServerError, events.ErrorCodeRenderOrigin5xx, true},
		{http.StatusBadGateway, events.ErrorCodeRenderOrigin5xx, true},
	}
	for _, tc := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "nope", tc.status)
		}))
		c := render.New(srv.URL, "test-token", 5*time.Second)
		_, err := c.FetchPage(context.Background(), "home", testPin())
		srv.Close()
		var ce *errclass.Error
		if !errors.As(err, &ce) {
			t.Fatalf("status %d: err = %v, want classified", tc.status, err)
		}
		if ce.Code != tc.wantCode || ce.Retryable() != tc.retryable {
			t.Errorf("status %d: code=%s retryable=%v, want %s/%v",
				tc.status, ce.Code, ce.Retryable(), tc.wantCode, tc.retryable)
		}
	}
}

// TestTimeoutBudget is EW-RENDER-003: a slow origin classifies as retryable
// RENDER_ORIGIN_TIMEOUT within the configured budget.
func TestTimeoutBudget(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
	}))
	defer srv.Close()

	c := render.New(srv.URL, "test-token", 150*time.Millisecond)
	start := time.Now()
	_, err := c.FetchPage(context.Background(), "home", testPin())
	elapsed := time.Since(start)
	var ce *errclass.Error
	if !errors.As(err, &ce) || ce.Code != events.ErrorCodeRenderOriginTimeout || !ce.Retryable() {
		t.Fatalf("err = %v, want retryable RENDER_ORIGIN_TIMEOUT", err)
	}
	if elapsed > time.Second {
		t.Errorf("timeout took %v — budget not enforced", elapsed)
	}
}

// TestNonHTMLRejected (FR-008): a 2xx response that is not text/html is a
// terminal render-contract violation.
func TestNonHTMLRejected(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"not":"html"}`))
	}))
	defer srv.Close()

	c := render.New(srv.URL, "test-token", 5*time.Second)
	_, err := c.FetchPage(context.Background(), "home", testPin())
	var ce *errclass.Error
	if !errors.As(err, &ce) || ce.Retryable() {
		t.Fatalf("non-HTML must fail non-retryably, got %v", err)
	}
}

func TestFetchAsset(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_next/static/chunks/a.js" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/javascript")
		_, _ = w.Write([]byte("console.log(1)"))
	}))
	defer srv.Close()

	c := render.New(srv.URL, "test-token", 5*time.Second)
	asset, err := c.FetchAsset(context.Background(), "/_next/static/chunks/a.js", testPin())
	if err != nil {
		t.Fatal(err)
	}
	if asset.MimeType != "application/javascript" || len(asset.Body) == 0 {
		t.Errorf("asset = %+v", asset)
	}

	_, err = c.FetchAsset(context.Background(), "/missing.js", testPin())
	var ce *errclass.Error
	if !errors.As(err, &ce) || ce.Code != events.ErrorCodeRenderOrigin404 {
		t.Fatalf("missing referenced asset must classify RENDER_ORIGIN_404, got %v", err)
	}
}
