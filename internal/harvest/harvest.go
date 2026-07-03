package harvest

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"mime"
	"net/url"
	"path"
	"sort"
	"strings"

	"github.com/ancyloce/anvilkit-export-worker/contracts/events"
	"github.com/ancyloce/anvilkit-export-worker/internal/errclass"
	"github.com/ancyloce/anvilkit-export-worker/internal/render"
)

// DefaultMaxExternalBytes bounds one allowlisted external asset
// (Recommended Approach — broader external mirroring is P1-002).
const DefaultMaxExternalBytes = 10 << 20

// Allowlist matches same-origin paths: entries are exact paths or prefixes
// ending in /* (§14 DEPENDENCY_ALLOWLIST semantics).
type Allowlist []string

// Allows reports whether p matches the allowlist.
func (a Allowlist) Allows(p string) bool {
	for _, entry := range a {
		if prefix, ok := strings.CutSuffix(entry, "/*"); ok {
			if strings.HasPrefix(p, prefix+"/") {
				return true
			}
			continue
		}
		if p == entry {
			return true
		}
	}
	return false
}

// AllowsURL matches external URLs: an entry containing "/" is a URL prefix;
// otherwise it is a hostname (EXTERNAL_ASSET_ALLOWLIST; deny-by-default).
func (a Allowlist) AllowsURL(u *url.URL) bool {
	for _, entry := range a {
		if strings.Contains(entry, "/") {
			if strings.HasPrefix(u.String(), entry) {
				return true
			}
			continue
		}
		if u.Hostname() == entry {
			return true
		}
	}
	return false
}

// File is one harvested artifact file.
type File struct {
	Path     string // normalized artifact-relative path (leading /)
	Body     []byte
	MimeType string
}

// Harvester walks the rendered page's dependency graph deterministically
// (FR-009): HTML refs, then same-origin CSS recursion, breadth-first in
// reference order, output sorted by path.
type Harvester struct {
	// Fetch retrieves one same-origin path from render-origin.
	Fetch func(ctx context.Context, p string) (*render.Asset, error)
	// FetchExternal retrieves one allowlisted external URL, enforcing limit
	// bytes. nil disables external mirroring entirely.
	FetchExternal func(ctx context.Context, rawURL string, limit int64) (*render.Asset, error)
	// Allow is the same-origin DEPENDENCY_ALLOWLIST.
	Allow Allowlist
	// External is the EXTERNAL_ASSET_ALLOWLIST (deny-by-default).
	External         Allowlist
	MaxExternalBytes int64
	Log              *slog.Logger
}

type ref struct {
	raw     string
	baseDir string // directory of the referencing CSS file ("" for HTML refs)
}

// Run applies the FR-008 output guards to the page and harvests every
// allowlisted dependency.
func (h *Harvester) Run(ctx context.Context, pageHTML []byte) ([]File, error) {
	if err := GuardHTML(pageHTML); err != nil {
		return nil, err
	}
	maxExternal := h.MaxExternalBytes
	if maxExternal <= 0 {
		maxExternal = DefaultMaxExternalBytes
	}

	queue := make([]ref, 0, 16)
	for _, r := range ExtractHTMLRefs(pageHTML) {
		queue = append(queue, ref{raw: r})
	}

	visited := map[string]bool{}
	var files []File

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		raw := strings.TrimSpace(current.raw)
		if raw == "" {
			continue
		}

		switch {
		case strings.HasPrefix(raw, "data:"), strings.HasPrefix(raw, "blob:"),
			strings.HasPrefix(raw, "mailto:"), strings.HasPrefix(raw, "javascript:"),
			strings.HasPrefix(raw, "#"), strings.HasPrefix(raw, "about:"):
			continue

		case strings.HasPrefix(raw, "asset://"):
			// Belt and braces — GuardHTML/GuardCSS catch these first.
			return nil, errclass.New(events.ErrorCodeUnresolvedAssetRef, events.FailedStageHarvestDependencies,
				fmt.Errorf("residual asset reference %q", raw))

		case strings.HasPrefix(raw, "http://"), strings.HasPrefix(raw, "https://"), strings.HasPrefix(raw, "//"):
			file, err := h.harvestExternal(ctx, raw, maxExternal)
			if err != nil {
				return nil, err
			}
			if file != nil && !visited[file.Path] {
				visited[file.Path] = true
				files = append(files, *file)
			}

		case strings.HasPrefix(raw, "/"):
			newFiles, newRefs, err := h.harvestSameOrigin(ctx, raw, visited)
			if err != nil {
				return nil, err
			}
			files = append(files, newFiles...)
			queue = append(queue, newRefs...)

		default:
			// Relative reference at the HTML level: MVP render-origin output
			// uses absolute same-origin paths; CSS-relative refs are resolved
			// by the CSS branch. Skipped and logged, never guessed.
			if current.baseDir != "" {
				queue = append(queue, ref{raw: path.Join(current.baseDir, raw)})
				continue
			}
			if h.Log != nil {
				h.Log.Warn("skipping relative HTML reference (not harvestable deterministically)", "ref", raw)
			}
		}
	}

	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return files, nil
}

// GuardHTML applies the FR-008 output guards to rendered HTML.
func GuardHTML(body []byte) error {
	if bytes.Contains(body, []byte("asset://")) {
		return errclass.New(events.ErrorCodeUnresolvedAssetRef, events.FailedStageHarvestDependencies,
			fmt.Errorf("rendered HTML contains residual asset:// references (resolution is owned upstream, PRD 0008 §13.7)"))
	}
	if bytes.Contains(body, []byte("/_next/image")) {
		return errclass.New(events.ErrorCodeUnsupportedDynamicImageOptimizer, events.FailedStageHarvestDependencies,
			fmt.Errorf("rendered HTML references the dynamic /_next/image optimizer (PRD 0008 §13.3)"))
	}
	return nil
}

// GuardCSS applies the asset:// residue guard to a downloaded stylesheet.
func GuardCSS(cssPath string, body []byte) error {
	if bytes.Contains(body, []byte("asset://")) {
		return errclass.New(events.ErrorCodeUnresolvedAssetRef, events.FailedStageHarvestDependencies,
			fmt.Errorf("downloaded stylesheet %s contains residual asset:// references", cssPath))
	}
	return nil
}

func (h *Harvester) harvestSameOrigin(ctx context.Context, raw string, visited map[string]bool) ([]File, []ref, error) {
	// Strip query/fragment: static mirrors serve the path only.
	trimmed := raw
	if i := strings.IndexAny(trimmed, "?#"); i >= 0 {
		trimmed = trimmed[:i]
	}
	if strings.HasPrefix(trimmed, "/_next/image") {
		return nil, nil, errclass.New(events.ErrorCodeUnsupportedDynamicImageOptimizer,
			events.FailedStageHarvestDependencies,
			fmt.Errorf("dependency %q uses the dynamic /_next/image optimizer", raw))
	}
	normalized, err := NormalizePath(trimmed)
	if err != nil {
		return nil, nil, err
	}
	if !h.Allow.Allows(normalized) {
		if h.Log != nil {
			h.Log.Warn("skipping non-allowlisted same-origin dependency", "path", normalized)
		}
		return nil, nil, nil
	}
	if visited[normalized] {
		return nil, nil, nil
	}
	visited[normalized] = true

	asset, err := h.Fetch(ctx, normalized)
	if err != nil {
		return nil, nil, err
	}
	file := File{Path: normalized, Body: asset.Body, MimeType: asset.MimeType}

	var newRefs []ref
	if isCSS(normalized, asset.MimeType) {
		if err := GuardCSS(normalized, asset.Body); err != nil {
			return nil, nil, err
		}
		baseDir := path.Dir(normalized)
		for _, cssRef := range ExtractCSSURLs(asset.Body) {
			newRefs = append(newRefs, ref{raw: cssRef, baseDir: baseDir})
		}
	}
	return []File{file}, newRefs, nil
}

func (h *Harvester) harvestExternal(ctx context.Context, raw string, limit int64) (*File, error) {
	withScheme := raw
	if strings.HasPrefix(raw, "//") {
		withScheme = "https:" + raw
	}
	u, err := url.Parse(withScheme)
	if err != nil || u.Host == "" {
		if h.Log != nil {
			h.Log.Warn("skipping unparseable external reference", "ref", raw)
		}
		return nil, nil
	}
	if !h.External.AllowsURL(u) || h.FetchExternal == nil {
		// Deny-by-default (FR-009): external URLs stay external — the
		// browser fetches them directly; nothing to mirror.
		if h.Log != nil {
			h.Log.Info("external reference not mirrored (not on EXTERNAL_ASSET_ALLOWLIST)", "url", u.String())
		}
		return nil, nil
	}
	asset, err := h.FetchExternal(ctx, u.String(), limit)
	if err != nil {
		return nil, err
	}
	normalized, err := NormalizePath(u.Path)
	if err != nil {
		return nil, err
	}
	return &File{Path: normalized, Body: asset.Body, MimeType: asset.MimeType}, nil
}

func isCSS(p, mimeType string) bool {
	mediaType, _, _ := mime.ParseMediaType(mimeType)
	return mediaType == "text/css" || strings.HasSuffix(p, ".css")
}
