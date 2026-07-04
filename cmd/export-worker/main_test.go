// SIGTERM graceful-drain test (FR-017, EW-OBS-005, AC-014): the real binary,
// mid-job, receives SIGTERM — it must stop consuming, drain the in-flight
// job to completion, release its lock, ack, and exit cleanly with code 0.
// Skipped without REDIS_TEST_URL + S3_TEST_ENDPOINT.
package main_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	goredis "github.com/redis/go-redis/v9"

	"github.com/ancyloce/anvilkit-export-worker/contracts/deploymentservice"
)

const drainBucket = "anvilkit-artifacts-test"

// drainDeploymentService is a minimal thread-safe CAS deployment-service.
type drainDeploymentService struct {
	mu  sync.Mutex
	rec deploymentservice.DeploymentRecord
}

func (s *drainDeploymentService) status() deploymentservice.DeploymentStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rec.Status
}

func (s *drainDeploymentService) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/deployments/{id}", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		_ = json.NewEncoder(w).Encode(s.rec)
	})
	mux.HandleFunc("PATCH /internal/deployments/{id}/status", func(w http.ResponseWriter, r *http.Request) {
		var body deploymentservice.StatusUpdateRequest
		_ = json.NewDecoder(r.Body).Decode(&body)
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.rec.Status != body.From {
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(deploymentservice.StatusConflictError{
				ErrorCode: "STATUS_CONFLICT", CurrentStatus: s.rec.Status,
			})
			return
		}
		s.rec.Status = body.To
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /internal/deployments/{id}/artifact", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}

func TestSIGTERMDrainsInFlightJob(t *testing.T) {
	redisURL := os.Getenv("REDIS_TEST_URL")
	s3Endpoint := os.Getenv("S3_TEST_ENDPOINT")
	if redisURL == "" || s3Endpoint == "" {
		t.Skip("REDIS_TEST_URL / S3_TEST_ENDPOINT not set; skipping drain test")
	}
	ctx := context.Background()

	// Dedicated Redis DB for the subprocess.
	opts, err := goredis.ParseURL(redisURL)
	if err != nil {
		t.Fatal(err)
	}
	opts.DB = 5
	rdb := goredis.NewClient(opts)
	defer func() { _ = rdb.Close() }()
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	// Bucket bootstrap.
	host := strings.TrimPrefix(strings.TrimPrefix(s3Endpoint, "http://"), "https://")
	rawS3, err := minio.New(host, &minio.Options{Creds: credentials.NewStaticV4("minioadmin", "minioadmin", "")})
	if err != nil {
		t.Fatal(err)
	}
	if exists, err := rawS3.BucketExists(ctx, drainBucket); err != nil {
		t.Fatal(err)
	} else if !exists {
		if err := rawS3.MakeBucket(ctx, drainBucket, minio.MakeBucketOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	// Render origin whose page render takes 3s — the in-flight window.
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/home" {
			time.Sleep(3 * time.Second)
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte(`<html><body>slow page</body></html>`))
			return
		}
		http.NotFound(w, r)
	}))
	defer origin.Close()

	svc := &drainDeploymentService{rec: deploymentservice.DeploymentRecord{
		DeploymentID: "dep_drain", TeamID: "team_01", SiteID: "site_drain",
		PageID: "page_01", Slug: "home", Version: "v12",
		Status:     deploymentservice.DeploymentStatusExportQueued,
		RenderMode: "fetch_route", TargetID: "target_platform_prod", Environment: "production",
	}}
	svcSrv := httptest.NewServer(svc.handler())
	defer svcSrv.Close()

	// Build the real binary.
	bin := filepath.Join(t.TempDir(), "export-worker")
	build := exec.Command("go", "build", "-o", bin, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}

	redisSubURL := strings.TrimRight(redisURL, "/") + "/5"
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(),
		"ENVIRONMENT=local", // loopback test servers are local-dev targets
		"QUEUE_DRIVER=redis",
		"REDIS_URL="+redisSubURL,
		"RENDER_ORIGIN_URL="+origin.URL,
		"DEPLOYMENT_SERVICE_URL="+svcSrv.URL,
		"ASSET_SERVICE_URL="+svcSrv.URL,
		"INTERNAL_SERVICE_TOKEN=test-token",
		"ARTIFACT_STORAGE_DRIVER=s3",
		"S3_ENDPOINT="+s3Endpoint,
		"S3_ACCESS_KEY=minioadmin",
		"S3_SECRET_KEY=minioadmin",
		"ARTIFACT_BUCKET="+drainBucket,
		"WORKER_CONCURRENCY=2",
		"LOG_LEVEL=info",
		fmt.Sprintf("HEALTH_PORT=%d", freePort(t)),
		fmt.Sprintf("METRICS_PORT=%d", freePort(t)),
	)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill() }()

	// Publish the job once the worker is consuming.
	payload, _ := json.Marshal(map[string]any{
		"eventId": "evt_drain", "eventType": "deployment.export.requested",
		"deploymentId": "dep_drain", "teamId": "team_01", "siteId": "site_drain",
		"pageId": "page_01", "slug": "home", "version": "v12",
		"renderMode": "fetch_route", "targetId": "target_platform_prod",
		"environment": "production", "idempotencyKey": "dep_drain",
	})
	time.Sleep(2 * time.Second) // boot + group creation
	if err := rdb.XAdd(ctx, &goredis.XAddArgs{
		Stream: "anvilkit:deployment.export.requested",
		Values: map[string]any{"payload": string(payload), "attempt": "0"},
	}).Err(); err != nil {
		t.Fatal(err)
	}

	// Wait until the job is mid-flight (EXPORTING, render sleeping).
	deadline := time.Now().Add(15 * time.Second)
	for svc.status() != deploymentservice.DeploymentStatusExporting {
		if time.Now().After(deadline) {
			t.Fatalf("job never reached EXPORTING; status=%s", svc.status())
		}
		time.Sleep(50 * time.Millisecond)
	}

	// SIGTERM mid-job: the worker must drain, not abandon.
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("worker exited non-zero after SIGTERM: %v (AC-014)", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("worker did not exit within 30s of SIGTERM")
	}

	// The in-flight job completed during the drain.
	if got := svc.status(); got != deploymentservice.DeploymentStatusArtifactReady {
		t.Fatalf("status after drain = %s, want ARTIFACT_READY (in-flight job must complete)", got)
	}
	pending, err := rdb.XPending(ctx, "anvilkit:deployment.export.requested", "export-worker").Result()
	if err != nil {
		t.Fatal(err)
	}
	if pending.Count != 0 {
		t.Fatalf("pending after drain = %d, want 0 (acked before exit)", pending.Count)
	}
	if exists, _ := rdb.Exists(ctx, "lock:deployment:dep_drain").Result(); exists != 0 {
		t.Fatal("lock not released during drain")
	}
}
