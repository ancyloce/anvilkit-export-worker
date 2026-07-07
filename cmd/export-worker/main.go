// Command export-worker is anvilkit-export-worker: the stateless, queue-driven
// Go worker that owns the AnvilKit export stage.
//
// Hardened runtime: fail-fast config with the demo guard, Redis Streams
// consumption behind the driver seam, the five-mechanism retry/DLQ model,
// per-deployment locking, the full export pipeline — version-pinned render
// fetch with same-origin size bounds, deterministic dependency harvesting,
// hashed idempotent uploads, internal-only manifest, pointer submission,
// CAS ARTIFACT_READY, CAS-then-emit outcome events, FR-015 redelivery
// re-emit — plus stream retention trimming (ADR-011) and the SIGTERM
// graceful drain (FR-017).
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/ancyloce/anvilkit-export-worker/internal/buildinfo"
	"github.com/ancyloce/anvilkit-export-worker/internal/config"
	"github.com/ancyloce/anvilkit-export-worker/internal/deployment"
	"github.com/ancyloce/anvilkit-export-worker/internal/emit"
	"github.com/ancyloce/anvilkit-export-worker/internal/export"
	"github.com/ancyloce/anvilkit-export-worker/internal/harvest"
	"github.com/ancyloce/anvilkit-export-worker/internal/lock"
	"github.com/ancyloce/anvilkit-export-worker/internal/obs"
	"github.com/ancyloce/anvilkit-export-worker/internal/queue"
	"github.com/ancyloce/anvilkit-export-worker/internal/render"
	"github.com/ancyloce/anvilkit-export-worker/internal/storage"
	"github.com/ancyloce/anvilkit-export-worker/internal/worker"
)

func main() {
	showVersion := flag.Bool("version", false, "print the service name and version, then exit")
	flag.Parse()
	if *showVersion {
		fmt.Printf("%s %s\n", buildinfo.Name, buildinfo.Version)
		return
	}
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "%s %s: startup failed: %v\n", buildinfo.Name, buildinfo.Version, err)
		os.Exit(1)
	}
}

func run() error {
	// Fail-fast configuration (FR-019): jobs never start on a misconfigured
	// worker; the error aggregates every problem.
	cfg, err := config.FromEnv()
	if err != nil {
		return err
	}

	hostname, _ := os.Hostname()
	workerID := fmt.Sprintf("%s-%s-%s", cfg.WorkerName, hostname, randomSuffix())
	logger := obs.NewLogger(os.Stdout, cfg.LogLevel, workerID,
		[]string{cfg.InternalServiceToken, cfg.S3AccessKey, cfg.S3SecretKey})

	tracingShutdown, err := obs.InitTracing(context.Background(), cfg.OTELEndpoint, cfg.WorkerName, buildinfo.Version)
	if err != nil {
		return err
	}
	defer func() {
		flushCtx, flushCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer flushCancel()
		_ = tracingShutdown(flushCtx)
	}()

	lifecycle := &obs.Lifecycle{}
	registry := prometheus.NewRegistry()
	metrics := obs.NewMetrics(registry)
	ops := obs.NewOpsServer(cfg.HealthPort, cfg.MetricsPort, lifecycle, registry)
	opsErrs := ops.Start()

	// Hard dependencies at boot (readiness stays false until both pass).
	redisOpts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return fmt.Errorf("REDIS_URL: %w", err)
	}
	rdb := redis.NewClient(redisOpts)
	defer func() { _ = rdb.Close() }()
	bootCtx, bootCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer bootCancel()
	if err := rdb.Ping(bootCtx).Err(); err != nil {
		return fmt.Errorf("redis unreachable at boot (fail-fast): %w", err)
	}
	store, err := storage.NewS3(cfg.S3Endpoint, cfg.S3Region, cfg.S3AccessKey, cfg.S3SecretKey, cfg.ArtifactBucket)
	if err != nil {
		return err
	}
	if err := store.EnsureBucket(bootCtx); err != nil {
		return fmt.Errorf("artifact storage (fail-fast): %w", err)
	}

	driver := queue.NewRedisDriver(rdb, workerID)
	if err := driver.EnsureGroup(bootCtx); err != nil {
		return err
	}
	retries := queue.NewRedisRetryStore(rdb)
	lockTTL := lock.TTLFor(cfg.RenderTimeout, cfg.UploadTimeout)
	locker := lock.NewLocker(rdb, workerID, lockTTL)
	deploy := deployment.New(cfg.DeploymentServiceURL, cfg.InternalServiceToken, 10*time.Second)
	emitter := &emit.Emitter{Append: driver}

	// The export pipeline (render → harvest → upload → manifest →
	// submit → CAS ready → emit ready).
	renderClient := render.New(cfg.RenderOriginURL, cfg.InternalServiceToken, cfg.RenderTimeout)
	renderClient.MaxHTMLBytes = cfg.MaxRenderHTMLBytes
	renderClient.MaxAssetBytes = cfg.MaxRenderAssetBytes
	pipeline := &export.Pipeline{
		Render:                renderClient,
		Deploy:                deploy,
		Store:                 store,
		Emitter:               emitter,
		BasePrefix:            cfg.ArtifactBasePrefix,
		Allow:                 harvest.Allowlist(cfg.DependencyAllowlist),
		External:              harvest.Allowlist(cfg.ExternalAssetAllowlist),
		UploadConcurrency:     8,
		UploadTimeout:         cfg.UploadTimeout,
		MaxTotalArtifactBytes: cfg.MaxTotalArtifactBytes,
		ExternalHTTP:          externalHTTPClient(cfg),
		Metrics:               metrics,
	}
	var exporter worker.Exporter = pipeline
	var readyRedeliver worker.ReadyRedeliverer = pipeline
	if cfg.WorkerDryRun {
		// Local-only scaffold mode: the processor short-circuits before any
		// status write; the exporter is never invoked.
		exporter = worker.Unimplemented{}
		readyRedeliver = nil // dry-run never emits
	}

	proc := worker.New(worker.Deps{
		Consumer:       driver,
		DLQ:            driver,
		Retries:        retries,
		Locker:         worker.LocksFrom(locker),
		Deploy:         deploy,
		Exporter:       exporter,
		FailedEmit:     emitter,
		ReadyRedeliver: readyRedeliver,
		Metrics:        metrics,
		Log:            logger,
		WorkerID:       workerID,
		DryRun:         cfg.WorkerDryRun,
		JobTimeout:     lockTTL,
	})

	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	// In-flight work survives SIGTERM up to its own deadline (drain).
	drainCtx := context.WithoutCancel(rootCtx)

	var wg sync.WaitGroup

	// Delayed-retry dispatcher (mechanism 4).
	dispatcher := &queue.Dispatcher{Store: retries, Pub: driver, Log: logger, Metrics: metrics}
	wg.Add(1)
	go func() {
		defer wg.Done()
		dispatcher.Run(rootCtx)
	}()

	// Pending recovery (mechanism 2): reclaim messages idle past the lock
	// TTL (plus slack, so an actively locked job is never stolen).
	wg.Add(1)
	go func() {
		defer wg.Done()
		minIdle := lockTTL + 30*time.Second
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-rootCtx.Done():
				return
			case <-ticker.C:
				msgs, err := driver.Reclaim(rootCtx, minIdle, 16)
				if err != nil {
					logger.Error("pending reclaim failed", "err", err)
					continue
				}
				for _, m := range msgs {
					logger.Info("reclaimed pending message (attempt unchanged)",
						"messageId", m.ID, "attempt", m.Attempt)
					proc.Handle(drainCtx, m)
				}
			}
		}
	}()

	// Stream retention trimming (ADR-011): enforce the configured floors on
	// the four worker-owned streams. The main stream never trims
	// delivered-but-unacked or undelivered entries (ack rule outranks
	// retention); trimming is idempotent across a multi-worker fleet.
	wg.Add(1)
	go func() {
		defer wg.Done()
		type target struct {
			stream    string
			retention time.Duration
			main      bool
		}
		targets := []target{
			{queue.StreamMain, cfg.StreamMainRetention, true},
			{queue.StreamDLQ, cfg.StreamDLQRetention, false},
			{queue.StreamArtifactReady, cfg.StreamReadyRetention, false},
			{queue.StreamExportFailed, cfg.StreamFailedRetention, false},
		}
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-rootCtx.Done():
				return
			case <-ticker.C:
				for _, tgt := range targets {
					if tgt.retention <= 0 {
						continue // trimming disabled for this stream
					}
					horizon := time.Now().Add(-tgt.retention)
					var trimmed int64
					var err error
					if tgt.main {
						trimmed, err = driver.TrimMain(rootCtx, horizon)
					} else {
						trimmed, err = driver.TrimStream(rootCtx, tgt.stream, horizon)
					}
					if err != nil {
						if rootCtx.Err() == nil {
							logger.Error("stream retention trim failed", "stream", tgt.stream, "err", err)
						}
						continue
					}
					if trimmed > 0 {
						metrics.StreamTrimmedTotal.WithLabelValues(tgt.stream).Add(float64(trimmed))
						logger.Info("stream retention trim", "stream", tgt.stream,
							"trimmed", trimmed, "retentionMs", tgt.retention.Milliseconds())
					}
				}
			}
		}
	}()

	// Queue-pending gauge.
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-rootCtx.Done():
				return
			case <-ticker.C:
				if n, err := driver.PendingCount(rootCtx); err == nil {
					metrics.QueuePending.Set(float64(n))
				}
			}
		}
	}()

	// Consumer pool (WORKER_CONCURRENCY fetch loops).
	for i := 0; i < cfg.WorkerConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-rootCtx.Done():
					return
				default:
				}
				msgs, err := driver.Fetch(rootCtx, 1, 2*time.Second)
				if err != nil {
					if rootCtx.Err() != nil {
						return
					}
					logger.Error("fetch failed", "err", err)
					time.Sleep(time.Second)
					continue
				}
				for _, m := range msgs {
					proc.Handle(drainCtx, m)
				}
			}
		}()
	}

	lifecycle.Set(obs.StateReady)
	logger.Info("worker ready",
		"service", buildinfo.Name, "version", buildinfo.Version,
		"environment", cfg.Environment, "dryRun", cfg.WorkerDryRun,
		"concurrency", cfg.WorkerConcurrency, "lockTtlMs", lockTTL.Milliseconds(),
		"renderOrigin", cfg.RenderOriginURL, "artifactBucket", cfg.ArtifactBucket,
		"healthPort", cfg.HealthPort, "metricsPort", cfg.MetricsPort)

	select {
	case err := <-opsErrs:
		stop()
		wg.Wait()
		return err
	case <-rootCtx.Done():
	}

	// Graceful drain (FR-017): stop consuming, finish in-flight jobs, flip
	// readiness, exit cleanly — proven by the real-binary SIGTERM test
	// (AC-014); deploys rely on it.
	lifecycle.Set(obs.StateDraining)
	logger.Info("SIGTERM received; draining")
	wg.Wait()
	lifecycle.Set(obs.StateStopped)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = ops.Shutdown(shutdownCtx)
	logger.Info("worker stopped cleanly")
	return nil
}

// externalHTTPClient exists only when external mirroring is enabled
// (EXTERNAL_ASSET_ALLOWLIST non-empty); it never carries internal
// credentials.
func externalHTTPClient(cfg *config.Config) *http.Client {
	if len(cfg.ExternalAssetAllowlist) == 0 {
		return nil
	}
	return &http.Client{Timeout: 30 * time.Second}
}

func randomSuffix() string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
