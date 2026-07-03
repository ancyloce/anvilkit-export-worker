// Pipeline-level T-render-worker-happy-path (AC-003 evidence at the exporter
// seam) against a real MinIO (skipped without S3_TEST_ENDPOINT) and an
// in-process render origin, plus the pipeline-level guard tests
// (T-asset-unresolved-ref, T-next-image-guard — AC-005) and artifact
// idempotency re-run (AC-006/AC-008 groundwork).
package export_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/ancyloce/anvilkit-export-worker/contracts/artifact"
	"github.com/ancyloce/anvilkit-export-worker/contracts/deploymentservice"
	"github.com/ancyloce/anvilkit-export-worker/contracts/events"
	"github.com/ancyloce/anvilkit-export-worker/internal/emit"
	"github.com/ancyloce/anvilkit-export-worker/internal/errclass"
	"github.com/ancyloce/anvilkit-export-worker/internal/export"
	"github.com/ancyloce/anvilkit-export-worker/internal/harvest"
	"github.com/ancyloce/anvilkit-export-worker/internal/jsonschema"
	"github.com/ancyloce/anvilkit-export-worker/internal/queue"
	"github.com/ancyloce/anvilkit-export-worker/internal/render"
	"github.com/ancyloce/anvilkit-export-worker/internal/storage"
	"github.com/ancyloce/anvilkit-export-worker/internal/worker"
)

const testBucket = "anvilkit-artifacts-test"

// --- test doubles ------------------------------------------------------------

type fakeDeployments struct {
	mu          sync.Mutex
	transitions []deploymentservice.DeploymentStatus
	pointers    []deploymentservice.ArtifactPointer
}

func (f *fakeDeployments) Transition(_ context.Context, _ string,
	_, to deploymentservice.DeploymentStatus, _, _ string, _ events.FailedStage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.transitions = append(f.transitions, to)
	return nil
}

func (f *fakeDeployments) SubmitArtifact(_ context.Context, _ string, p deploymentservice.ArtifactPointer) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pointers = append(f.pointers, p)
	return nil
}

type fakeAppender struct {
	mu       sync.Mutex
	payloads map[string][][]byte
}

func (f *fakeAppender) AppendOutcome(_ context.Context, stream string, payload []byte) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.payloads == nil {
		f.payloads = map[string][][]byte{}
	}
	f.payloads[stream] = append(f.payloads[stream], payload)
	return "1-0", nil
}

// testOrigin serves a deterministic page with every harvest form.
func testOrigin(t *testing.T) *httptest.Server {
	t.Helper()
	pages := map[string]struct{ body, contentType string }{
		"/home": {`<html><head>
<link rel="stylesheet" href="/_next/static/css/main.css">
<script src="/_next/static/chunks/app.js"></script>
<meta property="og:image" content="/assets/og.png">
</head><body><img src="/assets/hero.jpg" srcset="/assets/hero-640.jpg 640w"></body></html>`, "text/html; charset=utf-8"},
		"/_next/static/css/main.css":  {`@font-face{src:url("/fonts/inter.woff2")}`, "text/css"},
		"/_next/static/chunks/app.js": {"console.log(1)", "application/javascript"},
		"/assets/og.png":              {"og-png", "image/png"},
		"/assets/hero.jpg":            {"hero", "image/jpeg"},
		"/assets/hero-640.jpg":        {"hero640", "image/jpeg"},
		"/fonts/inter.woff2":          {"woff2", "font/woff2"},
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		page, ok := pages[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", page.contentType)
		_, _ = w.Write([]byte(page.body))
	}))
}

func testStore(t *testing.T) *storage.S3Store {
	t.Helper()
	endpoint := os.Getenv("S3_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("S3_TEST_ENDPOINT not set; skipping MinIO-backed pipeline test")
	}
	host := strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	raw, err := minio.New(host, &minio.Options{
		Creds: credentials.NewStaticV4("minioadmin", "minioadmin", ""),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if exists, err := raw.BucketExists(ctx, testBucket); err != nil {
		t.Fatal(err)
	} else if !exists {
		if err := raw.MakeBucket(ctx, testBucket, minio.MakeBucketOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	for object := range raw.ListObjects(ctx, testBucket, minio.ListObjectsOptions{Prefix: "sites/site_e/", Recursive: true}) {
		if object.Err != nil {
			t.Fatal(object.Err)
		}
		_ = raw.RemoveObject(ctx, testBucket, object.Key, minio.RemoveObjectOptions{})
	}
	store, err := storage.NewS3(endpoint, "us-east-1", "minioadmin", "minioadmin", testBucket)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func testJob() *worker.Job {
	rec := &deploymentservice.DeploymentRecord{
		DeploymentID: "dep_e2e", TeamID: "team_01", SiteID: "site_e",
		PageID: "page_01", Slug: "home", Version: "v12",
		Status: deploymentservice.DeploymentStatusExporting, RenderMode: "fetch_route",
		TargetID: "target_platform_prod", Environment: "production",
	}
	return &worker.Job{
		Record:  rec,
		TraceID: "0123456789abcdef0123456789abcdef",
		Log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func newPipeline(origin *httptest.Server, store *storage.S3Store, deps *fakeDeployments, app *fakeAppender) *export.Pipeline {
	return &export.Pipeline{
		Render:            render.New(origin.URL, "test-token", 5*time.Second),
		Deploy:            deps,
		Store:             store,
		Emitter:           &emit.Emitter{Append: app},
		BasePrefix:        "sites",
		Allow:             harvest.Allowlist{"/_next/static/*", "/assets/*", "/fonts/*", "/component-styles.css"},
		UploadConcurrency: 8,
		UploadTimeout:     30 * time.Second,
	}
}

// --- tests -------------------------------------------------------------------

// TestPipelineHappyPath: render → harvest → upload → manifest → pointer →
// CAS ARTIFACT_READY → ready event, all verified against real storage.
func TestPipelineHappyPath(t *testing.T) {
	store := testStore(t)
	origin := testOrigin(t)
	defer origin.Close()
	deps := &fakeDeployments{}
	app := &fakeAppender{}
	p := newPipeline(origin, store, deps, app)
	ctx := context.Background()

	if err := p.Export(ctx, testJob()); err != nil {
		t.Fatalf("Export: %v", err)
	}

	// CAS ARTIFACT_READY happened exactly once.
	if len(deps.transitions) != 1 || deps.transitions[0] != deploymentservice.DeploymentStatusArtifactReady {
		t.Errorf("transitions = %v", deps.transitions)
	}

	// Pointer submission (FR-012) with routes[] present.
	if len(deps.pointers) != 1 {
		t.Fatalf("pointers = %d", len(deps.pointers))
	}
	pointer := deps.pointers[0]
	if pointer.ArtifactBasePath != "sites/site_e/deployments/dep_e2e" ||
		pointer.Entry != "/home/index.html" ||
		pointer.FilesCount != 7 || // html + 6 dependencies
		len(pointer.Routes) != 1 || pointer.Routes[0].Path != "/home" {
		t.Errorf("pointer = %+v", pointer)
	}
	if !strings.HasPrefix(pointer.ManifestDigest, "sha256-") {
		t.Errorf("manifestDigest = %q", pointer.ManifestDigest)
	}

	// Manifest is in storage, schema-valid, and self-consistent.
	manifestBytes, err := store.Fetch(ctx, pointer.ManifestStorageKey)
	if err != nil {
		t.Fatalf("fetch manifest: %v", err)
	}
	if violations := jsonschema.ValidateBytes(artifact.SchemaArtifactManifest, manifestBytes); len(violations) > 0 {
		t.Fatalf("stored manifest fails the frozen schema: %v", violations)
	}
	var m artifact.Manifest
	if err := json.Unmarshal(manifestBytes, &m); err != nil {
		t.Fatal(err)
	}
	if len(m.Files) != 7 || m.Entry != "/home/index.html" || len(m.Routes) != 1 {
		t.Errorf("manifest = entry %s, %d files, %d routes", m.Entry, len(m.Files), len(m.Routes))
	}
	for _, f := range m.Files {
		body, err := store.Fetch(ctx, f.StorageKey)
		if err != nil {
			t.Errorf("manifest file %s missing from storage: %v", f.StorageKey, err)
			continue
		}
		if "sha256-"+storage.SHA256Hex(body) != f.ContentHash {
			t.Errorf("stored %s does not match manifest contentHash", f.StorageKey)
		}
		if f.Path == "/home/index.html" && f.CacheControl != storage.CacheControlHTML {
			t.Errorf("html cacheControl = %q", f.CacheControl)
		}
		if strings.HasPrefix(f.Path, "/_next/static/") && f.CacheControl != storage.CacheControlHashed {
			t.Errorf("hashed asset cacheControl = %q for %s", f.CacheControl, f.Path)
		}
	}

	// Ready event: schema-valid, no routes[], CAS-then-emit (AC-023/AC-029).
	readyPayloads := app.payloads[queue.StreamArtifactReady]
	if len(readyPayloads) != 1 {
		t.Fatalf("ready events = %d", len(readyPayloads))
	}
	if violations := jsonschema.ValidateBytes(events.SchemaArtifactReady, readyPayloads[0]); len(violations) > 0 {
		t.Fatalf("ready event fails frozen schema: %v", violations)
	}
	var asMap map[string]any
	_ = json.Unmarshal(readyPayloads[0], &asMap)
	if _, hasRoutes := asMap["routes"]; hasRoutes {
		t.Fatal("ready event carries routes[] (forbidden by ADR-001)")
	}

	// Idempotent re-run (AC-006 groundwork): same deployment re-exports with
	// every object hash-skipped and one more duplicate-tolerant ready event.
	if err := p.Export(ctx, testJob()); err != nil {
		t.Fatalf("re-run Export: %v", err)
	}
	if len(app.payloads[queue.StreamArtifactReady]) != 2 {
		t.Errorf("re-run must emit a duplicate-tolerant ready event")
	}
}

// TestPipelineAssetRefGuard is T-asset-unresolved-ref at the pipeline level
// (AC-005).
func TestPipelineAssetRefGuard(t *testing.T) {
	store := testStore(t)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(`<html><img src="asset://img_01"></html>`))
	}))
	defer origin.Close()

	p := newPipeline(origin, store, &fakeDeployments{}, &fakeAppender{})
	err := p.Export(context.Background(), testJob())
	var ce *errclass.Error
	if !errors.As(err, &ce) || ce.Code != events.ErrorCodeUnresolvedAssetRef {
		t.Fatalf("err = %v, want UNRESOLVED_ASSET_REF", err)
	}
}

// TestPipelineNextImageGuard is T-next-image-guard at the pipeline level
// (AC-005).
func TestPipelineNextImageGuard(t *testing.T) {
	store := testStore(t)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(`<html><img src="/_next/image?url=%2Fhero.jpg&w=640"></html>`))
	}))
	defer origin.Close()

	p := newPipeline(origin, store, &fakeDeployments{}, &fakeAppender{})
	err := p.Export(context.Background(), testJob())
	var ce *errclass.Error
	if !errors.As(err, &ce) || ce.Code != events.ErrorCodeUnsupportedDynamicImageOptimizer {
		t.Fatalf("err = %v, want UNSUPPORTED_DYNAMIC_IMAGE_OPTIMIZER", err)
	}
}
