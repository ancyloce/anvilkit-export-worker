// Package render owns the render-origin HTTP client (FR-007,
// EW-RENDER-001..004): version-pinned page fetch with the bearer token and
// all seven X-AnvilKit-* pinning headers, same-origin dependency fetch for
// the harvester, response classification per PRD 0010 §8.3, and the
// RENDER_TIMEOUT_MS budget. Render output is consumed over HTTP only — never
// via render code (hard boundary, AC-002/AC-010).
//
// Preview deployments (environment=preview) flow through the same
// version-pinned path — the worker always sends the pinning headers; preview
// E2E acceptance stays blocked by BD-009 (FR-024, AC-030).
package render

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/ancyloce/anvilkit-export-worker/contracts/deploymentservice"
	"github.com/ancyloce/anvilkit-export-worker/contracts/events"
	"github.com/ancyloce/anvilkit-export-worker/internal/errclass"
)

// Default same-origin size limits (§14 MAX_RENDER_HTML_BYTES /
// MAX_RENDER_ASSET_BYTES defaults). Render output beyond the configured
// bound is a broken render contract — non-retryable VALIDATION_FAILED,
// mirroring the external-mirror limit's classification.
const (
	DefaultMaxHTMLBytes  = 10 << 20 // 10 MiB per rendered page
	DefaultMaxAssetBytes = 25 << 20 // 25 MiB per same-origin dependency
)

// Client fetches version-pinned render output.
type Client struct {
	baseURL string
	token   string
	http    *http.Client

	// MaxHTMLBytes / MaxAssetBytes bound one fetched page / same-origin
	// asset (0 = the package defaults above). Responses are read through a
	// bounded reader, so an oversized origin response can never exhaust
	// worker memory; oversize errors carry byte counts only — never body
	// content or credentials.
	MaxHTMLBytes  int64
	MaxAssetBytes int64
}

// New builds the client. timeout is the RENDER_TIMEOUT_MS budget — expiry
// classifies as retryable RENDER_ORIGIN_TIMEOUT (EW-RENDER-003).
func New(baseURL, token string, timeout time.Duration) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   token,
		http:    &http.Client{Timeout: timeout},
	}
}

// Pin carries the version-pinning header values (FR-007): the authoritative
// deployment record drives them, never the event hints.
type Pin struct {
	DeploymentID string
	PageID       string
	Version      string
	TeamID       string
	SiteID       string
	Environment  string
	TraceID      string // forwarded as W3C traceparent when it is a 32-hex trace id
}

// PinFromRecord derives the pinning headers from the deployment record.
func PinFromRecord(rec *deploymentservice.DeploymentRecord, traceID string) Pin {
	return Pin{
		DeploymentID: rec.DeploymentID,
		PageID:       rec.PageID,
		Version:      rec.Version,
		TeamID:       rec.TeamID,
		SiteID:       rec.SiteID,
		Environment:  rec.Environment,
		TraceID:      traceID,
	}
}

// Asset is one fetched same-origin dependency.
type Asset struct {
	Path     string
	Body     []byte
	MimeType string
}

// FetchPage fetches GET {base}/{slug} and enforces the 2xx + text/html
// success contract. Classification (EW-RENDER-002): 401/403/404 →
// RENDER_ORIGIN_401/403/404, 409 → VERSION_SLUG_MISMATCH (all
// non-retryable); 5xx → RENDER_ORIGIN_5XX and timeout →
// RENDER_ORIGIN_TIMEOUT (retryable).
func (c *Client) FetchPage(ctx context.Context, slug string, pin Pin) ([]byte, error) {
	target := c.baseURL + "/" + escapeSlug(slug)
	body, contentType, err := c.get(ctx, target, "text/html", pin)
	if err != nil {
		return nil, err
	}
	mediaType, _, _ := mime.ParseMediaType(contentType)
	if mediaType != "text/html" {
		// Render output guard (FR-008): non-HTML render output is a broken
		// render contract — terminal, surfaced to the platform team.
		return nil, errclass.New(events.ErrorCodeValidationFailed, events.FailedStageRenderHtml,
			fmt.Errorf("render-origin returned non-HTML content-type %q for slug %q", contentType, slug))
	}
	return body, nil
}

// FetchAsset fetches one same-origin dependency path (leading /) for the
// harvester. A 404 on a referenced dependency is a broken artifact —
// non-retryable RENDER_ORIGIN_404.
func (c *Client) FetchAsset(ctx context.Context, path string, pin Pin) (*Asset, error) {
	if !strings.HasPrefix(path, "/") {
		return nil, errclass.New(events.ErrorCodeValidationFailed, events.FailedStageHarvestDependencies,
			fmt.Errorf("asset path must be same-origin absolute, got %q", path))
	}
	body, contentType, err := c.get(ctx, c.baseURL+path, "*/*", pin)
	if err != nil {
		return nil, err
	}
	mediaType, _, _ := mime.ParseMediaType(contentType)
	if mediaType == "" {
		mediaType = "application/octet-stream"
	}
	return &Asset{Path: path, Body: body, MimeType: mediaType}, nil
}

func (c *Client) get(ctx context.Context, target, accept string, pin Pin) ([]byte, string, error) {
	stage := events.FailedStageRenderHtml
	if accept != "text/html" {
		stage = events.FailedStageHarvestDependencies
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, "", errclass.New(events.ErrorCodeValidationFailed, stage, err)
	}
	req.Header.Set("Accept", accept)
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("X-AnvilKit-Render-Worker", "true")
	req.Header.Set("X-AnvilKit-Deployment-Id", pin.DeploymentID)
	req.Header.Set("X-AnvilKit-Page-Id", pin.PageID)
	req.Header.Set("X-AnvilKit-Version", pin.Version)
	req.Header.Set("X-AnvilKit-Team-Id", pin.TeamID)
	req.Header.Set("X-AnvilKit-Site-Id", pin.SiteID)
	req.Header.Set("X-AnvilKit-Environment", pin.Environment)
	// Trace context forwarded across the repo boundary (FR-007): the live
	// OTel span context wins; the synthetic traceparent derived from the
	// job's traceID is the fallback when no span is recording.
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	if req.Header.Get("traceparent") == "" {
		if tp := traceparent(pin.TraceID); tp != "" {
			req.Header.Set("traceparent", tp)
		}
	}

	resp, err := c.http.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || isTimeout(err) {
			return nil, "", errclass.New(events.ErrorCodeRenderOriginTimeout, stage, err)
		}
		// Connection errors behave like an unavailable origin: retryable.
		return nil, "", errclass.New(events.ErrorCodeRenderOrigin5xx, stage, err)
	}
	defer func() { _ = resp.Body.Close() }()
	limit, limitVar := c.limitFor(stage)
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || isTimeout(err) {
			return nil, "", errclass.New(events.ErrorCodeRenderOriginTimeout, stage, err)
		}
		return nil, "", errclass.New(events.ErrorCodeRenderOrigin5xx, stage, err)
	}

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		if int64(len(body)) > limit {
			// Same-origin output guard: oversized render output is a broken
			// render/artifact contract — terminal, never truncated.
			return nil, "", errclass.New(events.ErrorCodeValidationFailed, stage,
				fmt.Errorf("render-origin response for %s exceeds %s (%d-byte limit)", target, limitVar, limit))
		}
		return body, resp.Header.Get("Content-Type"), nil
	case resp.StatusCode == http.StatusUnauthorized:
		return nil, "", errclass.New(events.ErrorCodeRenderOrigin401, stage, statusErr(resp.StatusCode, target))
	case resp.StatusCode == http.StatusForbidden:
		return nil, "", errclass.New(events.ErrorCodeRenderOrigin403, stage, statusErr(resp.StatusCode, target))
	case resp.StatusCode == http.StatusNotFound:
		return nil, "", errclass.New(events.ErrorCodeRenderOrigin404, stage, statusErr(resp.StatusCode, target))
	case resp.StatusCode == http.StatusConflict:
		return nil, "", errclass.New(events.ErrorCodeVersionSlugMismatch, stage, statusErr(resp.StatusCode, target))
	case resp.StatusCode >= 500:
		return nil, "", errclass.New(events.ErrorCodeRenderOrigin5xx, stage, statusErr(resp.StatusCode, target))
	default:
		return nil, "", errclass.New(events.ErrorCodeValidationFailed, stage,
			fmt.Errorf("unexpected render-origin status %d for %s", resp.StatusCode, target))
	}
}

// limitFor returns the byte bound and the §14 variable naming it for the
// given pipeline stage (page fetch vs dependency fetch).
func (c *Client) limitFor(stage events.FailedStage) (int64, string) {
	if stage == events.FailedStageHarvestDependencies {
		if c.MaxAssetBytes > 0 {
			return c.MaxAssetBytes, "MAX_RENDER_ASSET_BYTES"
		}
		return DefaultMaxAssetBytes, "MAX_RENDER_ASSET_BYTES"
	}
	if c.MaxHTMLBytes > 0 {
		return c.MaxHTMLBytes, "MAX_RENDER_HTML_BYTES"
	}
	return DefaultMaxHTMLBytes, "MAX_RENDER_HTML_BYTES"
}

func statusErr(status int, target string) error {
	return fmt.Errorf("render-origin returned %d for %s", status, target)
}

func isTimeout(err error) bool {
	var t interface{ Timeout() bool }
	return errors.As(err, &t) && t.Timeout()
}

// escapeSlug path-escapes each slug segment while preserving nesting.
func escapeSlug(slug string) string {
	segments := strings.Split(strings.Trim(slug, "/"), "/")
	for i, s := range segments {
		segments[i] = url.PathEscape(s)
	}
	return strings.Join(segments, "/")
}

// traceparent renders a W3C trace context header for a 32-hex trace id.
func traceparent(traceID string) string {
	if len(traceID) != 32 {
		return ""
	}
	if _, err := hex.DecodeString(traceID); err != nil {
		return ""
	}
	var span [8]byte
	_, _ = rand.Read(span[:])
	return fmt.Sprintf("00-%s-%s-01", traceID, hex.EncodeToString(span[:]))
}
