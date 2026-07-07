// Package export implements the Exporter seam with the full M3 pipeline
// (PRD 0010 §5.1 stages render_html → harvest_dependencies →
// upload_artifacts → write_manifest → submit_artifact →
// update_status_ready → emit_ready): fetch version-pinned HTML, harvest
// dependencies deterministically, upload hashed artifacts, write the
// internal-only manifest, submit the pointer (BD-004 interim semantics),
// CAS EXPORTING → ARTIFACT_READY, and emit deployment.artifact.ready
// (CAS-then-emit, FR-013).
package export

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/ancyloce/anvilkit-export-worker/contracts/artifact"
	"github.com/ancyloce/anvilkit-export-worker/contracts/deploymentservice"
	"github.com/ancyloce/anvilkit-export-worker/contracts/events"
	"github.com/ancyloce/anvilkit-export-worker/internal/deployment"
	"github.com/ancyloce/anvilkit-export-worker/internal/emit"
	"github.com/ancyloce/anvilkit-export-worker/internal/errclass"
	"github.com/ancyloce/anvilkit-export-worker/internal/harvest"
	"github.com/ancyloce/anvilkit-export-worker/internal/obs"
	"github.com/ancyloce/anvilkit-export-worker/internal/render"
	"github.com/ancyloce/anvilkit-export-worker/internal/storage"
	"github.com/ancyloce/anvilkit-export-worker/internal/worker"
)

// Deployments is the pipeline's view of the deployment-service wrapper
// (satisfied by *deployment.Client).
type Deployments interface {
	Transition(ctx context.Context, deploymentID string,
		from, to deploymentservice.DeploymentStatus, reason, traceID string,
		stage events.FailedStage) error
	SubmitArtifact(ctx context.Context, deploymentID string, pointer deploymentservice.ArtifactPointer) error
}

// Pipeline is the M3 exporter.
type Pipeline struct {
	Render  *render.Client
	Deploy  Deployments
	Store   *storage.S3Store
	Emitter *emit.Emitter

	// BasePrefix is ARTIFACT_BASE_PREFIX (default "sites").
	BasePrefix string
	// Allow is the same-origin DEPENDENCY_ALLOWLIST.
	Allow harvest.Allowlist
	// External is the EXTERNAL_ASSET_ALLOWLIST (deny-by-default).
	External harvest.Allowlist
	// UploadConcurrency is the per-deployment upload pool size (8–16).
	UploadConcurrency int
	// UploadTimeout is the whole-artifact upload budget
	// (ARTIFACT_UPLOAD_TIMEOUT on expiry).
	UploadTimeout time.Duration
	// MaxTotalArtifactBytes bounds the whole artifact bundle
	// (MAX_TOTAL_ARTIFACT_BYTES; 0 disables). Oversize is a broken
	// render/artifact contract — non-retryable VALIDATION_FAILED.
	MaxTotalArtifactBytes int64
	// ExternalHTTP fetches allowlisted external assets (no internal
	// credentials attached); nil disables external mirroring.
	ExternalHTTP *http.Client
	// Metrics is the stage-duration/artifact-size instrumentation (nil-safe).
	Metrics *obs.Metrics

	Now func() time.Time
}

// Export runs the pipeline for one locked, EXPORTING deployment.
func (p *Pipeline) Export(ctx context.Context, job *worker.Job) error {
	now := p.Now
	if now == nil {
		now = time.Now
	}
	rec := job.Record
	basePath := fmt.Sprintf("%s/%s/deployments/%s", p.BasePrefix, rec.SiteID, rec.DeploymentID)
	entry, route, err := harvest.MapSlug(rec.Slug)
	if err != nil {
		return err
	}
	pin := render.PinFromRecord(rec, job.TraceID)

	// render_html: version-pinned fetch (FR-007).
	renderCtx, renderSpan := obs.StartSpan(ctx, "render_html")
	renderStart := now()
	html, err := p.Render.FetchPage(renderCtx, rec.Slug, pin)
	obs.EndSpan(renderSpan, err)
	if p.Metrics != nil {
		p.Metrics.RenderDurationMs.Observe(float64(now().Sub(renderStart).Milliseconds()))
	}
	if err != nil {
		return err
	}
	job.Log.Info("render fetched", "stage", string(events.FailedStageRenderHtml), "htmlBytes", len(html))

	// harvest_dependencies: guards + deterministic dependency walk
	// (FR-008/FR-009/FR-010).
	harvester := &harvest.Harvester{
		Fetch: func(ctx context.Context, path string) (*render.Asset, error) {
			return p.Render.FetchAsset(ctx, path, pin)
		},
		FetchExternal: p.externalFetcher(),
		Allow:         p.Allow,
		External:      p.External,
		Log:           job.Log,
	}
	harvestCtx, harvestSpan := obs.StartSpan(ctx, "harvest_dependencies")
	harvestStart := now()
	deps, err := harvester.Run(harvestCtx, html)
	obs.EndSpan(harvestSpan, err)
	if p.Metrics != nil {
		p.Metrics.HarvestDurationMs.Observe(float64(now().Sub(harvestStart).Milliseconds()))
	}
	if err != nil {
		return err
	}
	files := append([]harvest.File{{Path: entry, Body: html, MimeType: "text/html"}}, deps...)
	job.Log.Info("dependencies harvested", "stage", string(events.FailedStageHarvestDependencies),
		"files", len(files))

	// upload_artifacts: hashed, idempotent, bounded-concurrency (FR-011).
	objects := make([]storage.Object, 0, len(files))
	manifestFiles := make([]storage.ManifestFileInput, 0, len(files))
	var totalBytes int64
	for _, f := range files {
		key, kerr := harvest.StorageKey(basePath, f.Path)
		if kerr != nil {
			return kerr
		}
		hash := storage.SHA256Hex(f.Body)
		cacheControl := storage.CacheControlFor(f.Path, f.MimeType)
		objects = append(objects, storage.Object{
			Key: key, Body: f.Body, ContentType: f.MimeType,
			CacheControl: cacheControl, SHA256Hex: hash,
		})
		manifestFiles = append(manifestFiles, storage.ManifestFileInput{
			Path: f.Path, StorageKey: key, SHA256Hex: hash,
			SizeBytes: int64(len(f.Body)), MimeType: f.MimeType, CacheControl: cacheControl,
		})
		totalBytes += int64(len(f.Body))
	}
	if p.MaxTotalArtifactBytes > 0 && totalBytes > p.MaxTotalArtifactBytes {
		// Whole-bundle output guard: terminal, checked before any upload.
		// The error carries byte counts only — never body content.
		return errclass.New(events.ErrorCodeValidationFailed, events.FailedStageUploadArtifacts,
			fmt.Errorf("artifact totals %d bytes across %d files, exceeding MAX_TOTAL_ARTIFACT_BYTES (%d-byte limit)",
				totalBytes, len(files), p.MaxTotalArtifactBytes))
	}
	uploadCtx := ctx
	var uploadCancel context.CancelFunc
	if p.UploadTimeout > 0 {
		uploadCtx, uploadCancel = context.WithTimeout(ctx, p.UploadTimeout)
		defer uploadCancel()
	}
	uploadSpanCtx, uploadSpan := obs.StartSpan(uploadCtx, "upload_artifacts")
	uploadStart := now()
	uploaded, skipped, err := p.Store.UploadAll(uploadSpanCtx, objects, p.UploadConcurrency)
	obs.EndSpan(uploadSpan, err)
	if p.Metrics != nil {
		p.Metrics.UploadDurationMs.Observe(float64(now().Sub(uploadStart).Milliseconds()))
		p.Metrics.ArtifactBytesTotal.Add(float64(totalBytes))
		p.Metrics.ArtifactFilesTotal.Add(float64(len(objects)))
	}
	if err != nil {
		if errors.Is(uploadCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil {
			// The whole-artifact budget expired (distinct from a single
			// slow storage op): ARTIFACT_UPLOAD_TIMEOUT, retryable.
			return errclass.New(events.ErrorCodeArtifactUploadTimeout, events.FailedStageUploadArtifacts, err)
		}
		return err
	}
	job.Log.Info("artifacts uploaded", "stage", string(events.FailedStageUploadArtifacts),
		"uploaded", uploaded, "skipped", skipped, "totalBytes", totalBytes)

	// write_manifest: build, self-validate, upload internal-only (FR-012).
	manifestCtx, manifestSpan := obs.StartSpan(ctx, "write_manifest")
	manifest, encoded, digest, err := storage.BuildManifest(rec, basePath, entry, route, manifestFiles, now())
	if err != nil {
		obs.EndSpan(manifestSpan, err)
		return err
	}
	manifestKey := basePath + "/" + storage.ManifestFilename
	if _, err := p.Store.EnsureUploaded(manifestCtx, storage.Object{
		Key: manifestKey, Body: encoded, ContentType: "application/json",
		CacheControl: storage.CacheControlManifest, SHA256Hex: storage.SHA256Hex(encoded),
	}); err != nil {
		ce := errclass.From(err, events.FailedStageWriteManifest)
		obs.EndSpan(manifestSpan, err)
		return errclass.New(ce.Code, events.FailedStageWriteManifest, ce.Cause)
	}
	obs.EndSpan(manifestSpan, nil)
	job.Log.Info("manifest written", "stage", string(events.FailedStageWriteManifest),
		"manifestStorageKey", manifestKey, "manifestDigest", digest)

	// submit_artifact: pointer to deployment-service (FR-012, BD-004).
	pointer := deploymentservice.ArtifactPointer{
		ManifestStorageKey: manifestKey,
		ArtifactBasePath:   basePath,
		ManifestDigest:     digest,
		Entry:              entry,
		FilesCount:         int64(len(manifest.Files)),
		TotalBytes:         totalBytes,
		Routes:             []deploymentservice.ArtifactRoute{{Path: route, Entry: entry}},
	}
	submitCtx, submitSpan := obs.StartSpan(ctx, "submit_artifact")
	err = p.Deploy.SubmitArtifact(submitCtx, rec.DeploymentID, pointer)
	obs.EndSpan(submitSpan, err)
	if err != nil {
		return err
	}
	job.Log.Info("artifact pointer submitted", "stage", string(events.FailedStageSubmitArtifact))

	// update_status_ready: CAS EXPORTING → ARTIFACT_READY (FR-006).
	emitReady, err := p.casReady(ctx, job)
	if err != nil {
		return err
	}
	if !emitReady {
		return nil
	}

	// emit_ready: CAS-then-emit (FR-013). An emission failure is alerted
	// but does not fail the completed deployment: it is ARTIFACT_READY, so
	// any redelivery or replay of the request event verifies the stored
	// manifest and re-emits the ready event without re-rendering (FR-015).
	ready := emit.BuildReady(emit.ReadyInput{
		Record:             rec,
		ArtifactBasePath:   basePath,
		ManifestStorageKey: manifestKey,
		ManifestDigest:     digest,
		Entry:              entry,
		FilesCount:         int64(len(manifest.Files)),
		TotalBytes:         totalBytes,
		TraceID:            job.TraceID,
		Now:                now(),
	})
	emitCtx, emitSpan := obs.StartSpan(ctx, "emit_ready")
	err = p.Emitter.EmitReady(emitCtx, ready)
	obs.EndSpan(emitSpan, err)
	if err != nil {
		job.Log.Error("deployment.artifact.ready emission failed — recovered by re-emit on the next redelivery/replay of the request event (FR-015)",
			"alert", true, "err", err, "stage", string(events.FailedStageEmitReady))
	} else {
		job.Log.Info("ready event emitted", "stage", string(events.FailedStageEmitReady),
			"eventId", ready.EventID, "filesCount", ready.FilesCount, "totalBytes", ready.TotalBytes)
	}
	return nil
}

// casReady runs the update_status_ready stage: CAS EXPORTING →
// ARTIFACT_READY (FR-006) with the stop-safe 409 branches. It reports
// whether the ready event should be emitted. The stage span records the
// actual transition error — a failed or conflicted CAS must never trace as
// a clean stage, even on branches the pipeline recovers from.
func (p *Pipeline) casReady(ctx context.Context, job *worker.Job) (emitReady bool, err error) {
	rec := job.Record
	readyCtx, readySpan := obs.StartSpan(ctx, "update_status_ready")
	err = p.Deploy.Transition(readyCtx, rec.DeploymentID,
		deploymentservice.DeploymentStatusExporting,
		deploymentservice.DeploymentStatusArtifactReady,
		"artifact_ready", job.TraceID, events.FailedStageUpdateStatusReady)
	obs.EndSpan(readySpan, err)
	conflict, ok := deployment.AsStatusConflict(err)
	if !ok {
		return err == nil, err
	}
	switch {
	case conflict.CurrentStatus == deploymentservice.DeploymentStatusArtifactReady:
		// A previous run of this deployment CASed READY and crashed before
		// emitting: fall through and (re-)emit — consumers are
		// duplicate-tolerant by deploymentId (ADR-005).
		job.Log.Warn("deployment already ARTIFACT_READY; re-emitting ready event",
			"stage", string(events.FailedStageUpdateStatusReady))
		return true, nil
	case deployment.NonActionable(conflict.CurrentStatus):
		// e.g. CANCELLED mid-flight: stop quietly, no ready event.
		job.Log.Warn("deployment became non-actionable mid-export; skipping ready event",
			"stage", string(events.FailedStageUpdateStatusReady),
			"currentStatus", string(conflict.CurrentStatus))
		return false, nil
	default:
		return false, errclass.New(events.ErrorCodeValidationFailed, events.FailedStageUpdateStatusReady,
			fmt.Errorf("unexpected CAS conflict: deployment moved to %s during export", conflict.CurrentStatus))
	}
}

// ReemitReady implements the FR-015 redelivery path: load the stored
// manifest for an ARTIFACT_READY deployment and re-emit
// deployment.artifact.ready from stored state — never re-rendering.
func (p *Pipeline) ReemitReady(ctx context.Context, rec *deploymentservice.DeploymentRecord, traceID string) (bool, error) {
	now := p.Now
	if now == nil {
		now = time.Now
	}
	basePath := fmt.Sprintf("%s/%s/deployments/%s", p.BasePrefix, rec.SiteID, rec.DeploymentID)
	manifestKey := basePath + "/" + storage.ManifestFilename
	data, found, err := p.Store.FetchIfExists(ctx, manifestKey)
	if err != nil || !found {
		return found, err
	}
	var m artifact.Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return true, errclass.New(events.ErrorCodeValidationFailed, events.FailedStageEmitReady,
			fmt.Errorf("stored manifest is unreadable: %w", err))
	}
	var totalBytes int64
	for _, f := range m.Files {
		totalBytes += f.SizeBytes
	}
	ready := emit.BuildReady(emit.ReadyInput{
		Record:             rec,
		ArtifactBasePath:   basePath,
		ManifestStorageKey: manifestKey,
		ManifestDigest:     "sha256-" + storage.SHA256Hex(data),
		Entry:              m.Entry,
		FilesCount:         int64(len(m.Files)),
		TotalBytes:         totalBytes,
		TraceID:            traceID,
		Now:                now(),
	})
	return true, p.Emitter.EmitReady(ctx, ready)
}

// externalFetcher builds the allowlisted-external fetch function (no
// internal credentials ever attached to external requests).
func (p *Pipeline) externalFetcher() func(ctx context.Context, rawURL string, limit int64) (*render.Asset, error) {
	if p.ExternalHTTP == nil {
		return nil
	}
	return func(ctx context.Context, rawURL string, limit int64) (*render.Asset, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		if err != nil {
			return nil, errclass.New(events.ErrorCodeValidationFailed, events.FailedStageHarvestDependencies, err)
		}
		resp, err := p.ExternalHTTP.Do(req)
		if err != nil {
			return nil, errclass.New(events.ErrorCodeValidationFailed, events.FailedStageHarvestDependencies,
				fmt.Errorf("allowlisted external asset fetch failed: %w", err))
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			return nil, errclass.New(events.ErrorCodeValidationFailed, events.FailedStageHarvestDependencies,
				fmt.Errorf("allowlisted external asset %s returned %d", rawURL, resp.StatusCode))
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
		if err != nil {
			return nil, errclass.New(events.ErrorCodeValidationFailed, events.FailedStageHarvestDependencies, err)
		}
		if int64(len(body)) > limit {
			return nil, errclass.New(events.ErrorCodeValidationFailed, events.FailedStageHarvestDependencies,
				fmt.Errorf("external asset %s exceeds the %d-byte mirror limit", rawURL, limit))
		}
		mimeType := resp.Header.Get("Content-Type")
		if mimeType == "" {
			mimeType = "application/octet-stream"
		}
		return &render.Asset{Body: body, MimeType: mimeType}, nil
	}
}
