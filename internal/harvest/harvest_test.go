package harvest_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/ancyloce/anvilkit-export-worker/contracts/events"
	"github.com/ancyloce/anvilkit-export-worker/internal/errclass"
	"github.com/ancyloce/anvilkit-export-worker/internal/harvest"
	"github.com/ancyloce/anvilkit-export-worker/internal/render"
)

// origin is an in-memory same-origin asset space.
type origin map[string]render.Asset

func (o origin) fetch(_ context.Context, p string) (*render.Asset, error) {
	asset, ok := o[p]
	if !ok {
		return nil, errclass.New(events.ErrorCodeRenderOrigin404, events.FailedStageHarvestDependencies,
			fmt.Errorf("no such asset %s", p))
	}
	return &asset, nil
}

func newHarvester(o origin) *harvest.Harvester {
	return &harvest.Harvester{
		Fetch: o.fetch,
		Allow: harvest.Allowlist{"/_next/static/*", "/assets/*", "/fonts/*", "/component-styles.css"},
		Log:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

const happyHTML = `<html><head>
<link href="/_next/static/css/main.css" rel="stylesheet">
<script src="/_next/static/chunks/app.js"></script>
</head><body>
<img src="/assets/hero.jpg" srcset="/assets/hero-640.jpg 640w">
<img src="/not-allowlisted/skip.png">
</body></html>`

// TestHarvestHappyPathWithCSSRecursion: HTML refs plus same-origin CSS
// url(...) recursion, including a CSS-relative reference (FR-009).
func TestHarvestHappyPathWithCSSRecursion(t *testing.T) {
	o := origin{
		"/_next/static/css/main.css": {MimeType: "text/css",
			Body: []byte(`@font-face{src:url("/fonts/inter.woff2")} .r{background:url(./bg.png)}`)},
		"/_next/static/css/bg.png":    {MimeType: "image/png", Body: []byte("png")},
		"/_next/static/chunks/app.js": {MimeType: "application/javascript", Body: []byte("js")},
		"/assets/hero.jpg":            {MimeType: "image/jpeg", Body: []byte("jpg")},
		"/assets/hero-640.jpg":        {MimeType: "image/jpeg", Body: []byte("jpg640")},
		"/fonts/inter.woff2":          {MimeType: "font/woff2", Body: []byte("woff")},
	}
	files, err := newHarvester(o).Run(context.Background(), []byte(happyHTML))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	var paths []string
	for _, f := range files {
		paths = append(paths, f.Path)
	}
	want := []string{
		"/_next/static/chunks/app.js",
		"/_next/static/css/bg.png",
		"/_next/static/css/main.css",
		"/assets/hero-640.jpg",
		"/assets/hero.jpg",
		"/fonts/inter.woff2",
	}
	if fmt.Sprint(paths) != fmt.Sprint(want) {
		t.Fatalf("harvested (sorted):\ngot  %v\nwant %v", paths, want)
	}
	// The non-allowlisted path was skipped, never fetched, never an error.
}

// TestNextImageGuard is T-next-image-guard (AC-005): /_next/image in the
// rendered HTML fails non-retryably with the exact code.
func TestNextImageGuard(t *testing.T) {
	html := []byte(`<img src="/_next/image?url=%2Fassets%2Fhero.jpg&w=640">`)
	_, err := newHarvester(origin{}).Run(context.Background(), html)
	var ce *errclass.Error
	if !errors.As(err, &ce) || ce.Code != events.ErrorCodeUnsupportedDynamicImageOptimizer {
		t.Fatalf("err = %v, want UNSUPPORTED_DYNAMIC_IMAGE_OPTIMIZER", err)
	}
	if ce.Retryable() {
		t.Error("must be non-retryable")
	}
}

// TestAssetRefGuardHTML is T-asset-unresolved-ref (AC-005), HTML side.
func TestAssetRefGuardHTML(t *testing.T) {
	html := []byte(`<img src="asset://img_01">`)
	_, err := newHarvester(origin{}).Run(context.Background(), html)
	var ce *errclass.Error
	if !errors.As(err, &ce) || ce.Code != events.ErrorCodeUnresolvedAssetRef {
		t.Fatalf("err = %v, want UNRESOLVED_ASSET_REF", err)
	}
}

// TestAssetRefGuardCSS is T-asset-unresolved-ref (AC-005), CSS side
// (FR-008: residue in downloaded same-origin CSS).
func TestAssetRefGuardCSS(t *testing.T) {
	o := origin{
		"/component-styles.css": {MimeType: "text/css", Body: []byte(`.x{background:url("asset://img_01")}`)},
	}
	html := []byte(`<link href="/component-styles.css">`)
	_, err := newHarvester(o).Run(context.Background(), html)
	var ce *errclass.Error
	if !errors.As(err, &ce) || ce.Code != events.ErrorCodeUnresolvedAssetRef {
		t.Fatalf("err = %v, want UNRESOLVED_ASSET_REF from CSS", err)
	}
}

// TestTraversalInReference: hostile harvested paths classify
// PATH_TRAVERSAL_DETECTED (FR-010).
func TestTraversalInReference(t *testing.T) {
	html := []byte(`<script src="/assets/%2e%2e/%2e%2e/etc/passwd"></script>`)
	_, err := newHarvester(origin{}).Run(context.Background(), html)
	var ce *errclass.Error
	if !errors.As(err, &ce) || ce.Code != events.ErrorCodePathTraversalDetected {
		t.Fatalf("err = %v, want PATH_TRAVERSAL_DETECTED", err)
	}
}

// TestExternalDenyByDefault (EW-ARTIFACT-004): externals are skipped unless
// allowlisted; allowlisted externals mirror with the size limit enforced.
func TestExternalDenyByDefault(t *testing.T) {
	html := []byte(`<img src="https://cdn.example.com/pic.png"><img src="//proto.example.com/p.png">`)
	files, err := newHarvester(origin{}).Run(context.Background(), html)
	if err != nil {
		t.Fatalf("non-allowlisted externals must be skipped, not fail: %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("files = %v, want none", files)
	}
}

func TestExternalAllowlistedMirrorsWithSizeLimit(t *testing.T) {
	h := newHarvester(origin{})
	h.External = harvest.Allowlist{"cdn.example.com"}
	h.MaxExternalBytes = 8
	fetched := map[string]bool{}
	h.FetchExternal = func(_ context.Context, rawURL string, limit int64) (*render.Asset, error) {
		fetched[rawURL] = true
		body := []byte("0123456789") // 10 bytes > limit 8
		if int64(len(body)) > limit {
			return nil, errclass.New(events.ErrorCodeValidationFailed, events.FailedStageHarvestDependencies,
				fmt.Errorf("external asset %s exceeds the %d byte limit", rawURL, limit))
		}
		return &render.Asset{Body: body, MimeType: "image/png"}, nil
	}

	html := []byte(`<img src="https://cdn.example.com/pics/big.png">`)
	_, err := h.Run(context.Background(), html)
	var ce *errclass.Error
	if !errors.As(err, &ce) {
		t.Fatalf("oversized allowlisted external must be rejected, got %v", err)
	}
	if !fetched["https://cdn.example.com/pics/big.png"] {
		t.Error("allowlisted external must be fetched")
	}

	// Under the limit it mirrors under its URL path.
	h.MaxExternalBytes = 64
	files, err := h.Run(context.Background(), html)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].Path != "/pics/big.png" {
		t.Fatalf("files = %+v, want /pics/big.png", files)
	}
}

// TestAllowlistSemantics pins the §14 pattern forms.
func TestAllowlistSemantics(t *testing.T) {
	a := harvest.Allowlist{"/_next/static/*", "/component-styles.css"}
	cases := map[string]bool{
		"/_next/static/chunks/a.js": true,
		"/_next/static":             false,
		"/_next/staticevil/x.js":    false,
		"/component-styles.css":     true,
		"/component-styles.css.map": false,
		"/other.css":                false,
	}
	for p, want := range cases {
		if a.Allows(p) != want {
			t.Errorf("Allows(%q) = %v, want %v", p, !want, want)
		}
	}
}
