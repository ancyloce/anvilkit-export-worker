// M4 hardening suites (EW-TEST-005/006; skipped without REDIS_TEST_URL +
// S3_TEST_ENDPOINT): failure injection (render-origin down, deployment-
// service down, storage down → bounded retries → DLQ + EXPORT_FAILED +
// failed event), T-redelivery-idempotency (AC-008), the redelivery storm
// with concurrent workers (zero duplicate active artifacts — M4 exit), and
// the full §15.3 span-vocabulary assertion (AC-015).
package worker_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	goredis "github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/ancyloce/anvilkit-export-worker/contracts/deploymentservice"
	"github.com/ancyloce/anvilkit-export-worker/internal/deployment"
	"github.com/ancyloce/anvilkit-export-worker/internal/emit"
	"github.com/ancyloce/anvilkit-export-worker/internal/export"
	"github.com/ancyloce/anvilkit-export-worker/internal/harvest"
	"github.com/ancyloce/anvilkit-export-worker/internal/lock"
	"github.com/ancyloce/anvilkit-export-worker/internal/queue"
	"github.com/ancyloce/anvilkit-export-worker/internal/render"
	"github.com/ancyloce/anvilkit-export-worker/internal/storage"
	"github.com/ancyloce/anvilkit-export-worker/internal/testsupport"
	"github.com/ancyloce/anvilkit-export-worker/internal/worker"
)

const hardeningBucket = "anvilkit-artifacts-test"

// hardeningOrigin is a thread-safe render origin with a switchable failure
// mode and per-path request counters.
type hardeningOrigin struct {
	mu       sync.Mutex
	fail     bool
	requests map[string]int
}

func (o *hardeningOrigin) setFailing(failing bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.fail = failing
}

func (o *hardeningOrigin) renders(slug string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.requests["/"+slug]
}

func (o *hardeningOrigin) handler() http.Handler {
	pages := map[string]string{
		"/home": `<html><head><link rel="stylesheet" href="/assets/site.css"></head>` +
			`<body><img src="/assets/hero.jpg"></body></html>`,
		"/assets/site.css":   `.x{background:url("/fonts/inter.woff2")}`,
		"/assets/hero.jpg":   "hero-bytes",
		"/fonts/inter.woff2": "woff2-bytes",
	}
	types := map[string]string{
		"/home": "text/html", "/assets/site.css": "text/css",
		"/assets/hero.jpg": "image/jpeg", "/fonts/inter.woff2": "font/woff2",
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		o.mu.Lock()
		if o.requests == nil {
			o.requests = map[string]int{}
		}
		o.requests[r.URL.Path]++
		failing := o.fail
		o.mu.Unlock()
		if failing {
			http.Error(w, "injected origin outage", http.StatusInternalServerError)
			return
		}
		body, ok := pages[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", types[r.URL.Path])
		_, _ = w.Write([]byte(body))
	})
}

// hardeningStack wires the REAL pipeline (render client, harvester, MinIO
// storage, emitter) behind the processor against real Redis.
type hardeningStack struct {
	rdb      goredis.UniversalClient
	driver   *queue.RedisDriver
	retries  queue.RetryStore
	store    *storage.S3Store
	origin   *hardeningOrigin
	svc      *testDeploymentService
	pipeline *export.Pipeline
	proc     *worker.Processor
}

func newHardeningStack(t *testing.T, deploymentID string, mutate func(*export.Pipeline, *worker.Deps)) *hardeningStack {
	t.Helper()
	if os.Getenv("S3_TEST_ENDPOINT") == "" {
		t.Skip("S3_TEST_ENDPOINT not set; skipping hardening integration test")
	}
	rdb := testsupport.Redis(t, 3)
	store := hardeningStore(t)

	origin := &hardeningOrigin{}
	originSrv := httptest.NewServer(origin.handler())
	t.Cleanup(originSrv.Close)

	svc := &testDeploymentService{rec: deploymentservice.DeploymentRecord{
		DeploymentID: deploymentID, TeamID: "team_01", SiteID: "site_h",
		PageID: "page_01", Slug: "home", Version: "v12",
		Status:     deploymentservice.DeploymentStatusExportQueued,
		RenderMode: "fetch_route", TargetID: "target_platform_prod", Environment: "production",
	}}
	svcSrv := httptest.NewServer(svc.handler())
	t.Cleanup(svcSrv.Close)

	driver := queue.NewRedisDriver(rdb, "hardening-worker")
	if err := driver.EnsureGroup(context.Background()); err != nil {
		t.Fatal(err)
	}
	emitter := &emit.Emitter{Append: driver}
	deploy := deployment.New(svcSrv.URL, "test-token", 5*time.Second)
	retries := queue.NewRedisRetryStore(rdb)
	pipeline := &export.Pipeline{
		Render:            render.New(originSrv.URL, "test-token", 5*time.Second),
		Deploy:            deploy,
		Store:             store,
		Emitter:           emitter,
		BasePrefix:        "sites",
		Allow:             harvest.Allowlist{"/assets/*", "/fonts/*"},
		UploadConcurrency: 8,
		UploadTimeout:     20 * time.Second,
	}
	deps := worker.Deps{
		Consumer: driver, DLQ: driver, Retries: retries,
		Locker:         worker.LocksFrom(lock.NewLocker(rdb, "hardening-worker", 90*time.Second)),
		Deploy:         deploy,
		Exporter:       pipeline,
		FailedEmit:     emitter,
		ReadyRedeliver: pipeline,
		Log:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		WorkerID:       "hardening-worker",
		Backoff:        queue.BackoffPolicy{Base: time.Millisecond, Max: 2 * time.Millisecond, Rand: func() float64 { return 0 }},
	}
	if mutate != nil {
		mutate(pipeline, &deps)
	}
	return &hardeningStack{
		rdb: rdb, driver: driver, retries: retries, store: store,
		origin: origin, svc: svc, pipeline: pipeline, proc: worker.New(deps),
	}
}

func hardeningStore(t *testing.T) *storage.S3Store {
	t.Helper()
	endpoint := os.Getenv("S3_TEST_ENDPOINT")
	host := strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	raw, err := minio.New(host, &minio.Options{
		Creds: credentials.NewStaticV4("minioadmin", "minioadmin", ""),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if exists, err := raw.BucketExists(ctx, hardeningBucket); err != nil {
		t.Fatalf("test minio unreachable: %v", err)
	} else if !exists {
		if err := raw.MakeBucket(ctx, hardeningBucket, minio.MakeBucketOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	for object := range raw.ListObjects(ctx, hardeningBucket, minio.ListObjectsOptions{Prefix: "sites/site_h/", Recursive: true}) {
		if object.Err != nil {
			t.Fatal(object.Err)
		}
		_ = raw.RemoveObject(ctx, hardeningBucket, object.Key, minio.RemoveObjectOptions{})
	}
	store, err := storage.NewS3(endpoint, "us-east-1", "minioadmin", "minioadmin", hardeningBucket)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func publishHardeningEvent(t *testing.T, driver *queue.RedisDriver, deploymentID string) {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"eventId": "evt_" + deploymentID, "eventType": "deployment.export.requested",
		"deploymentId": deploymentID, "teamId": "team_01", "siteId": "site_h",
		"pageId": "page_01", "slug": "home", "version": "v12",
		"renderMode": "fetch_route", "targetId": "target_platform_prod",
		"environment": "production", "idempotencyKey": deploymentID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Publish(context.Background(), queue.OutgoingMessage{Payload: payload}); err != nil {
		t.Fatal(err)
	}
}

// driveUntilDLQ pumps fetch → handle → dispatcher-tick until the outcome
// sequence ends in a DLQ handoff, returning every outcome seen.
func (h *hardeningStack) driveUntilDLQ(t *testing.T) []worker.Outcome {
	t.Helper()
	ctx := context.Background()
	dispatcher := &queue.Dispatcher{Store: h.retries, Pub: h.driver, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	var outcomes []worker.Outcome
	deadline := time.Now().Add(30 * time.Second)
	for len(outcomes) == 0 || outcomes[len(outcomes)-1] != worker.OutcomeDLQ {
		if time.Now().After(deadline) {
			t.Fatalf("timed out; outcomes=%v", outcomes)
		}
		msgs, err := h.driver.Fetch(ctx, 1, 150*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range msgs {
			outcomes = append(outcomes, h.proc.Handle(ctx, m))
		}
		dispatcher.Tick(ctx, time.Now().Add(time.Second))
	}
	return outcomes
}

func (h *hardeningStack) failedEventPayloads(t *testing.T) []string {
	t.Helper()
	events, err := h.rdb.XRange(context.Background(), queue.StreamExportFailed, "-", "+").Result()
	if err != nil {
		t.Fatal(err)
	}
	var payloads []string
	for _, ev := range events {
		payloads = append(payloads, ev.Values["payload"].(string))
	}
	return payloads
}

// TestSpanVocabularyFullPipeline (AC-015): one happy-path job emits every
// §15.3 stage span.
func TestSpanVocabularyFullPipeline(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	h := newHardeningStack(t, "dep_spans", nil)
	publishHardeningEvent(t, h.driver, "dep_spans")

	msgs, err := h.driver.Fetch(context.Background(), 1, time.Second)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("fetch: %v %v", msgs, err)
	}
	if out := h.proc.Handle(context.Background(), msgs[0]); out != worker.OutcomeSuccess {
		t.Fatalf("outcome = %s", out)
	}
	if h.svc.status() != deploymentservice.DeploymentStatusArtifactReady {
		t.Fatalf("status = %s", h.svc.status())
	}

	got := map[string]bool{}
	for _, span := range exporter.GetSpans() {
		got[span.Name] = true
	}
	for _, name := range []string{
		"consume_job", "load_deployment", "acquire_lock", "update_status_exporting",
		"render_html", "harvest_dependencies", "upload_artifacts", "write_manifest",
		"submit_artifact", "update_status_ready", "emit_ready", "ack_message",
	} {
		if !got[name] {
			t.Errorf("missing §15.3 span %q; got %v", name, got)
		}
	}
}

// TestRedeliveryIdempotencyAfterReady is T-redelivery-idempotency (AC-008,
// FR-015 final): redelivery acks without re-rendering and re-emits the ready
// event from the stored manifest.
func TestRedeliveryIdempotencyAfterReady(t *testing.T) {
	h := newHardeningStack(t, "dep_redeliver", nil)
	ctx := context.Background()

	publishHardeningEvent(t, h.driver, "dep_redeliver")
	msgs, err := h.driver.Fetch(ctx, 1, time.Second)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("fetch: %v %v", msgs, err)
	}
	if out := h.proc.Handle(ctx, msgs[0]); out != worker.OutcomeSuccess {
		t.Fatalf("first outcome = %s", out)
	}
	rendersAfterFirst := h.origin.renders("home")

	// Redelivery of the same event (at-least-once).
	publishHardeningEvent(t, h.driver, "dep_redeliver")
	msgs, err = h.driver.Fetch(ctx, 1, time.Second)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("refetch: %v %v", msgs, err)
	}
	if out := h.proc.Handle(ctx, msgs[0]); out != worker.OutcomeAckedTerminal {
		t.Fatalf("redelivery outcome = %s, want acked_terminal", out)
	}
	if got := h.origin.renders("home"); got != rendersAfterFirst {
		t.Fatalf("re-render happened on redelivery: %d → %d (FR-015)", rendersAfterFirst, got)
	}
	if h.svc.status() != deploymentservice.DeploymentStatusArtifactReady {
		t.Fatalf("status = %s", h.svc.status())
	}

	readyEvents, err := h.rdb.XRange(ctx, queue.StreamArtifactReady, "-", "+").Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(readyEvents) != 2 {
		t.Fatalf("ready events = %d, want 2 (original + duplicate-tolerant re-emit)", len(readyEvents))
	}
	var digests []string
	for _, ev := range readyEvents {
		var payload struct {
			ManifestDigest string `json:"manifestDigest"`
		}
		_ = json.Unmarshal([]byte(ev.Values["payload"].(string)), &payload)
		digests = append(digests, payload.ManifestDigest)
	}
	if digests[0] != digests[1] || !strings.HasPrefix(digests[0], "sha256-") {
		t.Fatalf("re-emitted digest differs: %v", digests)
	}
	if n, _ := h.driver.PendingCount(ctx); n != 0 {
		t.Fatalf("pending = %d, want 0", n)
	}
}

// TestRenderOriginDownExhaustsToDLQ (EW-TEST-005): a down origin classifies
// retryable, burns four executions, and lands in the DLQ with a failed event.
func TestRenderOriginDownExhaustsToDLQ(t *testing.T) {
	h := newHardeningStack(t, "dep_origindown", nil)
	h.origin.setFailing(true)

	publishHardeningEvent(t, h.driver, "dep_origindown")
	outcomes := h.driveUntilDLQ(t)
	if len(outcomes) != 4 {
		t.Fatalf("executions = %d, want 4 (AC-033): %v", len(outcomes), outcomes)
	}
	if h.svc.status() != deploymentservice.DeploymentStatusExportFailed {
		t.Fatalf("status = %s, want EXPORT_FAILED", h.svc.status())
	}
	payloads := h.failedEventPayloads(t)
	if len(payloads) != 1 ||
		!strings.Contains(payloads[0], `"errorCode":"RENDER_ORIGIN_5XX"`) ||
		!strings.Contains(payloads[0], `"retryExhausted":true`) ||
		!strings.Contains(payloads[0], `"attempt":3`) {
		t.Fatalf("failed payloads = %v", payloads)
	}
	if n, _ := h.driver.PendingCount(context.Background()); n != 0 {
		t.Fatalf("pending = %d, want 0", n)
	}
}

// TestDeploymentServiceDownExhaustsToDLQ (EW-TEST-005).
func TestDeploymentServiceDownExhaustsToDLQ(t *testing.T) {
	h := newHardeningStack(t, "dep_svcdown", func(p *export.Pipeline, d *worker.Deps) {
		dead := deployment.New("http://127.0.0.1:1", "test-token", 300*time.Millisecond)
		p.Deploy = dead
		d.Deploy = dead
	})
	publishHardeningEvent(t, h.driver, "dep_svcdown")
	outcomes := h.driveUntilDLQ(t)
	if len(outcomes) != 4 {
		t.Fatalf("executions = %d, want 4: %v", len(outcomes), outcomes)
	}
	payloads := h.failedEventPayloads(t)
	if len(payloads) != 1 || !strings.Contains(payloads[0], `"errorCode":"DEPLOYMENT_SERVICE_TIMEOUT"`) {
		t.Fatalf("failed payloads = %v", payloads)
	}
}

// TestStorageDownExhaustsToDLQ (EW-TEST-005): a dead storage endpoint burns
// the whole-artifact upload budget (minio-go retries internally, so the
// budget — not a single-op error — is what fires: ARTIFACT_UPLOAD_TIMEOUT,
// retryable) and exhausts to the DLQ.
func TestStorageDownExhaustsToDLQ(t *testing.T) {
	h := newHardeningStack(t, "dep_storagedown", func(p *export.Pipeline, d *worker.Deps) {
		dead, err := storage.NewS3("http://127.0.0.1:1", "us-east-1", "x", "y", hardeningBucket)
		if err != nil {
			t.Fatal(err)
		}
		p.Store = dead
		p.UploadTimeout = 500 * time.Millisecond
	})
	publishHardeningEvent(t, h.driver, "dep_storagedown")
	outcomes := h.driveUntilDLQ(t)
	if len(outcomes) != 4 {
		t.Fatalf("executions = %d, want 4: %v", len(outcomes), outcomes)
	}
	if h.svc.status() != deploymentservice.DeploymentStatusExportFailed {
		t.Fatalf("status = %s, want EXPORT_FAILED", h.svc.status())
	}
	payloads := h.failedEventPayloads(t)
	if len(payloads) != 1 || !strings.Contains(payloads[0], `"errorCode":"ARTIFACT_UPLOAD_TIMEOUT"`) {
		t.Fatalf("failed payloads = %v", payloads)
	}
}

// TestRedeliveryStormZeroDuplicateArtifacts (EW-TEST-006, M4 exit): five
// duplicate deliveries, three concurrent workers with reclaim races — one
// deployment, one manifest, stable digest, nothing pending.
func TestRedeliveryStormZeroDuplicateArtifacts(t *testing.T) {
	h := newHardeningStack(t, "dep_storm", nil)

	for range 5 {
		publishHardeningEvent(t, h.driver, "dep_storm")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var handled atomic.Int64
	var wg sync.WaitGroup
	for i := range 3 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			name := fmt.Sprintf("storm-worker-%d", id)
			driver := queue.NewRedisDriver(h.rdb, name)
			emitter := &emit.Emitter{Append: driver}
			pipeline := &export.Pipeline{
				Render: h.pipeline.Render, Deploy: h.pipeline.Deploy, Store: h.store,
				Emitter: emitter, BasePrefix: "sites",
				Allow:             harvest.Allowlist{"/assets/*", "/fonts/*"},
				UploadConcurrency: 8, UploadTimeout: 20 * time.Second,
			}
			proc := worker.New(worker.Deps{
				Consumer: driver, DLQ: driver, Retries: queue.NewRedisRetryStore(h.rdb),
				Locker:         worker.LocksFrom(lock.NewLocker(h.rdb, name, 90*time.Second)),
				Deploy:         h.pipeline.Deploy.(*deployment.Client),
				Exporter:       pipeline,
				FailedEmit:     emitter,
				ReadyRedeliver: pipeline,
				Log:            slog.New(slog.NewTextHandler(io.Discard, nil)),
				WorkerID:       name,
			})
			for ctx.Err() == nil {
				msgs, err := driver.Fetch(ctx, 1, 100*time.Millisecond)
				if err != nil {
					return
				}
				for _, m := range msgs {
					proc.Handle(ctx, m)
					handled.Add(1)
				}
				// Reclaim lock-conflict leftovers from the other workers.
				reclaimed, _ := driver.Reclaim(ctx, 500*time.Millisecond, 4)
				for _, m := range reclaimed {
					proc.Handle(ctx, m)
					handled.Add(1)
				}
			}
		}(i)
	}

	deadline := time.Now().Add(25 * time.Second)
	for {
		if time.Now().After(deadline) {
			cancel()
			wg.Wait()
			t.Fatalf("storm did not settle; handled=%d", handled.Load())
		}
		pending, _ := h.driver.PendingCount(context.Background())
		if pending == 0 && handled.Load() >= 5 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	cancel()
	wg.Wait()

	if h.svc.status() != deploymentservice.DeploymentStatusArtifactReady {
		t.Fatalf("status = %s, want ARTIFACT_READY", h.svc.status())
	}

	// Zero duplicate active artifacts: exactly one manifest, stable digest,
	// every ready event carrying the same digest.
	manifestKey := "sites/site_h/deployments/dep_storm/artifact-manifest.json"
	manifestBytes, found, err := h.store.FetchIfExists(context.Background(), manifestKey)
	if err != nil || !found {
		t.Fatalf("manifest missing: %v", err)
	}
	wantDigest := "sha256-" + storage.SHA256Hex(manifestBytes)
	readyEvents, err := h.rdb.XRange(context.Background(), queue.StreamArtifactReady, "-", "+").Result()
	if err != nil || len(readyEvents) == 0 {
		t.Fatalf("ready events = %d err=%v", len(readyEvents), err)
	}
	for _, ev := range readyEvents {
		var payload struct {
			ManifestDigest string `json:"manifestDigest"`
			DeploymentID   string `json:"deploymentId"`
		}
		_ = json.Unmarshal([]byte(ev.Values["payload"].(string)), &payload)
		if payload.ManifestDigest != wantDigest || payload.DeploymentID != "dep_storm" {
			t.Fatalf("divergent ready event under storm: %+v want digest %s", payload, wantDigest)
		}
	}
	if n, _ := h.driver.PendingCount(context.Background()); n != 0 {
		t.Fatalf("pending after storm = %d", n)
	}
}
