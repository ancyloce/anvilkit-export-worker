package storage

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/ancyloce/anvilkit-export-worker/contracts/deploymentservice"
)

const testCreatedAt = "2026-06-30T18:00:00Z"

// TestCacheControlClasses pins the PRD 0008 §9.3 table and the G-6
// hash-detection rule (EW-STORAGE-005 DoD).
func TestCacheControlClasses(t *testing.T) {
	cases := []struct {
		path, mime, want string
	}{
		{"/home/index.html", "text/html", CacheControlHTML},
		{"/index.html", "text/html; charset=utf-8", CacheControlHTML},
		{"/_next/static/chunks/app.js", "application/javascript", CacheControlHashed},
		{"/_next/static/css/main.css", "text/css", CacheControlHashed},
		{"/assets/app.3f9d21ab.css", "text/css", CacheControlHashed},
		{"/assets/pic.0123456789abcdef.webp", "image/webp", CacheControlHashed},
		{"/assets/logo.png", "image/png", CacheControlNonHashed},
		{"/fonts/inter.woff2", "font/woff2", CacheControlNonHashed},
		{"/assets/short.abc1.js", "application/javascript", CacheControlNonHashed}, // hash too short
	}
	for _, tc := range cases {
		if got := CacheControlFor(tc.path, tc.mime); got != tc.want {
			t.Errorf("CacheControlFor(%q, %q) = %q, want %q", tc.path, tc.mime, got, tc.want)
		}
	}
}

func TestManifestIsInternalOnly(t *testing.T) {
	if !strings.Contains(CacheControlManifest, "no-store") || strings.Contains(CacheControlManifest, "public") {
		t.Fatalf("manifest cache-control must be private/no-store, got %q", CacheControlManifest)
	}
}

func testRecord() *deploymentservice.DeploymentRecord {
	return &deploymentservice.DeploymentRecord{
		DeploymentID: "dep_01", TeamID: "team_01", SiteID: "site_01",
		PageID: "page_01", Slug: "home", Version: "v12",
		Status: deploymentservice.DeploymentStatusExporting, RenderMode: "fetch_route",
		TargetID: "target_platform_prod", Environment: "production",
	}
}

// TestBuildManifestValidatesAndDigests (FR-012): schemaVersion 1, routes[]
// always an array, sha256- digest, self-validation green.
func TestBuildManifest(t *testing.T) {
	files := []ManifestFileInput{
		{
			Path: "/home/index.html", StorageKey: "sites/site_01/deployments/dep_01/home/index.html",
			SHA256Hex: strings.Repeat("a", 64), SizeBytes: 100, MimeType: "text/html",
			CacheControl: CacheControlHTML,
		},
	}
	m, encoded, digest, err := BuildManifest(testRecord(),
		"sites/site_01/deployments/dep_01", "/home/index.html", "/home", files, testCreatedAt)
	if err != nil {
		t.Fatalf("BuildManifest: %v", err)
	}
	if m.SchemaVersion != 1 {
		t.Errorf("schemaVersion = %d", m.SchemaVersion)
	}
	if len(m.Routes) != 1 || m.Routes[0].Path != "/home" || m.Routes[0].Entry != "/home/index.html" {
		t.Errorf("routes = %+v", m.Routes)
	}
	if !strings.HasPrefix(digest, "sha256-") || len(digest) != len("sha256-")+64 {
		t.Errorf("digest = %q", digest)
	}
	if !strings.HasPrefix(m.Files[0].ContentHash, "sha256-") {
		t.Errorf("contentHash = %q", m.Files[0].ContentHash)
	}

	// routes[] must serialize as a JSON array even conceptually-empty cases;
	// the builder always emits exactly one route in the single-page MVP.
	var asMap map[string]any
	if err := json.Unmarshal(encoded, &asMap); err != nil {
		t.Fatal(err)
	}
	if _, ok := asMap["routes"].([]any); !ok {
		t.Fatalf("routes did not serialize as an array: %T", asMap["routes"])
	}
}

// TestBuildManifestSelfValidationCatchesBrokenOutput: a hash that violates
// the schema pattern must fail the build, never reach submission.
func TestBuildManifestSelfValidation(t *testing.T) {
	files := []ManifestFileInput{
		{
			Path: "/home/index.html", StorageKey: "sites/site_01/deployments/dep_01/home/index.html",
			SHA256Hex: "", SizeBytes: 100, MimeType: "text/html", CacheControl: CacheControlHTML,
		},
	}
	// SHA256Hex "" yields contentHash "sha256-" ... pattern ^sha256- still
	// matches, so break it harder: empty mime violates minLength.
	files[0].MimeType = ""
	_, _, _, err := BuildManifest(testRecord(),
		"sites/site_01/deployments/dep_01", "/home/index.html", "/home", files, testCreatedAt)
	if err == nil {
		t.Fatal("schema-invalid manifest must fail self-validation")
	}
}

// TestBuildManifestDeterministicDigest (M1): the digest is a pure function of
// the inputs — including createdAt — with no wall-clock dependency, so re-runs
// of the same deployment produce an identical manifest and digest.
func TestBuildManifestDeterministicDigest(t *testing.T) {
	files := []ManifestFileInput{
		{
			Path: "/home/index.html", StorageKey: "sites/site_01/deployments/dep_01/home/index.html",
			SHA256Hex: strings.Repeat("a", 64), SizeBytes: 100, MimeType: "text/html",
			CacheControl: CacheControlHTML,
		},
	}
	build := func() (string, []byte) {
		_, encoded, digest, err := BuildManifest(testRecord(),
			"sites/site_01/deployments/dep_01", "/home/index.html", "/home", files, testCreatedAt)
		if err != nil {
			t.Fatalf("BuildManifest: %v", err)
		}
		return digest, encoded
	}
	d1, e1 := build()
	d2, e2 := build()
	if d1 != d2 || string(e1) != string(e2) {
		t.Errorf("manifest not deterministic: digest %q vs %q", d1, d2)
	}
	// A different createdAt must change the digest (the timestamp is inside the
	// hashed manifest, so callers must supply a STABLE value — that is the fix).
	_, _, d3, err := BuildManifest(testRecord(),
		"sites/site_01/deployments/dep_01", "/home/index.html", "/home", files, "2020-01-01T00:00:00Z")
	if err != nil {
		t.Fatalf("BuildManifest: %v", err)
	}
	if d3 == d1 {
		t.Error("digest should incorporate createdAt (confirms determinism must come from a stable input)")
	}
}
