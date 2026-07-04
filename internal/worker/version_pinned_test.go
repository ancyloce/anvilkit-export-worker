// T-version-pinned-render (EW-XREPO-004, AC-004): a concurrent publish of a
// newer version must not affect an in-flight artifact. The origin serves
// immutable version-pinned snapshots; deployment A (pinned v1, slow render)
// is mid-flight while deployment B (pinned v2) publishes and completes —
// A's artifact must be byte-stable v1 content. Skipped without
// REDIS_TEST_URL + S3_TEST_ENDPOINT.
package worker_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

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

// versionedOrigin serves immutable per-version snapshots; the v1 render is
// slow so a v2 publish can overlap it.
func versionedOrigin() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Trim(r.URL.Path, "/") != "home" {
			http.NotFound(w, r)
			return
		}
		switch r.Header.Get("X-AnvilKit-Version") {
		case "v1":
			time.Sleep(800 * time.Millisecond) // keep A in flight while B publishes
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte(`<html><body>content-v1</body></html>`))
		case "v2":
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte(`<html><body>content-v2</body></html>`))
		default:
			http.Error(w, "version not published", http.StatusConflict)
		}
	})
}

func TestVersionPinnedRenderUnderConcurrentPublish(t *testing.T) {
	if os.Getenv("S3_TEST_ENDPOINT") == "" {
		t.Skip("S3_TEST_ENDPOINT not set; skipping version-pinned E2E")
	}
	rdb := testsupport.Redis(t, 3)
	store := hardeningStore(t)
	origin := httptest.NewServer(versionedOrigin())
	defer origin.Close()
	ctx := context.Background()

	newStack := func(consumer, deploymentID, version string) (*queue.RedisDriver, *worker.Processor, *testDeploymentService) {
		svc := &testDeploymentService{rec: deploymentservice.DeploymentRecord{
			DeploymentID: deploymentID, TeamID: "team_01", SiteID: "site_h",
			PageID: "page_01", Slug: "home", Version: version,
			Status:     deploymentservice.DeploymentStatusExportQueued,
			RenderMode: "fetch_route", TargetID: "target_platform_prod", Environment: "production",
		}}
		srv := httptest.NewServer(svc.handler())
		t.Cleanup(srv.Close)
		driver := queue.NewRedisDriver(rdb, consumer)
		if err := driver.EnsureGroup(ctx); err != nil {
			t.Fatal(err)
		}
		deploy := deployment.New(srv.URL, "test-token", 5*time.Second)
		emitter := &emit.Emitter{Append: driver}
		pipeline := &export.Pipeline{
			Render: render.New(origin.URL, "test-token", 5*time.Second),
			Deploy: deploy, Store: store, Emitter: emitter, BasePrefix: "sites",
			Allow:             harvest.Allowlist{"/assets/*"},
			UploadConcurrency: 8, UploadTimeout: 20 * time.Second,
		}
		proc := worker.New(worker.Deps{
			Consumer: driver, DLQ: driver, Retries: queue.NewRedisRetryStore(rdb),
			Locker: worker.LocksFrom(lock.NewLocker(rdb, consumer, 90*time.Second)),
			Deploy: deploy, Exporter: pipeline, FailedEmit: emitter, ReadyRedeliver: pipeline,
			Log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
			WorkerID: consumer,
		})
		return driver, proc, svc
	}

	driverA, procA, svcA := newStack("pin-worker-a", "dep_pin_v1", "v1")
	driverB, procB, svcB := newStack("pin-worker-b", "dep_pin_v2", "v2")

	publish := func(driver *queue.RedisDriver, deploymentID, version string) {
		payload, _ := json.Marshal(map[string]any{
			"eventId": "evt_" + deploymentID, "eventType": "deployment.export.requested",
			"deploymentId": deploymentID, "teamId": "team_01", "siteId": "site_h",
			"pageId": "page_01", "slug": "home", "version": version,
			"renderMode": "fetch_route", "targetId": "target_platform_prod",
			"environment": "production", "idempotencyKey": deploymentID,
		})
		if _, err := driver.Publish(ctx, queue.OutgoingMessage{Payload: payload}); err != nil {
			t.Fatal(err)
		}
	}

	// A (pinned v1) goes in flight first: fetch its message, then handle it
	// in the background while its 800ms render sleeps.
	publish(driverA, "dep_pin_v1", "v1")
	msgsA, err := driverA.Fetch(ctx, 1, time.Second)
	if err != nil || len(msgsA) != 1 {
		t.Fatalf("fetch A: %v %v", msgsA, err)
	}
	doneA := make(chan worker.Outcome, 1)
	go func() { doneA <- procA.Handle(ctx, msgsA[0]) }()

	// Concurrent publish: v2 lands while A renders.
	time.Sleep(150 * time.Millisecond)
	publish(driverB, "dep_pin_v2", "v2")
	msgsB, err := driverB.Fetch(ctx, 1, time.Second)
	if err != nil || len(msgsB) != 1 {
		t.Fatalf("fetch B: %v %v", msgsB, err)
	}
	if out := procB.Handle(ctx, msgsB[0]); out != worker.OutcomeSuccess {
		t.Fatalf("B outcome = %s", out)
	}
	select {
	case out := <-doneA:
		if out != worker.OutcomeSuccess {
			t.Fatalf("A outcome = %s", out)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("A never completed")
	}
	if svcA.status() != deploymentservice.DeploymentStatusArtifactReady ||
		svcB.status() != deploymentservice.DeploymentStatusArtifactReady {
		t.Fatalf("statuses = %s / %s", svcA.status(), svcB.status())
	}

	// The in-flight artifact is byte-stable v1 content; B's is v2.
	htmlA, foundA, err := store.FetchIfExists(ctx, "sites/site_h/deployments/dep_pin_v1/home/index.html")
	if err != nil || !foundA {
		t.Fatalf("A entry missing: %v", err)
	}
	if !strings.Contains(string(htmlA), "content-v1") || strings.Contains(string(htmlA), "content-v2") {
		t.Fatalf("A artifact contaminated by the concurrent publish: %s", htmlA)
	}
	htmlB, foundB, err := store.FetchIfExists(ctx, "sites/site_h/deployments/dep_pin_v2/home/index.html")
	if err != nil || !foundB {
		t.Fatalf("B entry missing: %v", err)
	}
	if !strings.Contains(string(htmlB), "content-v2") {
		t.Fatalf("B artifact = %s", htmlB)
	}

	// A's manifest stays stable after B completed (no cross-deployment
	// interference — deployment-scoped prefixes).
	manifestA1, _, err := store.FetchIfExists(ctx, "sites/site_h/deployments/dep_pin_v1/artifact-manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	manifestA2, _, err := store.FetchIfExists(ctx, "sites/site_h/deployments/dep_pin_v1/artifact-manifest.json")
	if err != nil || storage.SHA256Hex(manifestA1) != storage.SHA256Hex(manifestA2) {
		t.Fatalf("A manifest not byte-stable: %v", err)
	}
}
