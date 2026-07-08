package harvest

import (
	"errors"
	"testing"

	"github.com/ancyloce/anvilkit-export-worker/contracts/events"
	"github.com/ancyloce/anvilkit-export-worker/internal/errclass"
)

// TestMapSlug pins the EW-ARTIFACT-006 rule table.
func TestMapSlug(t *testing.T) {
	cases := []struct {
		slug      string
		wantEntry string
		wantRoute string
		wantErr   bool
	}{
		{"home", "/home/index.html", "/home", false},
		{"about", "/about/index.html", "/about", false},
		{"blog/hello", "/blog/hello/index.html", "/blog/hello", false},
		{"/", "/index.html", "/", false},
		{"index", "/index.html", "/", false},
		{"..", "", "", true},
		{"a/../b", "", "", true},
		{"a//b", "", "", true},
		{"%2e%2e/etc", "", "", true},
		{"C:evil", "", "", true},
	}
	for _, tc := range cases {
		entry, route, err := MapSlug(tc.slug)
		if tc.wantErr {
			var ce *errclass.Error
			if !errors.As(err, &ce) || ce.Code != events.ErrorCodeInvalidSlug {
				t.Errorf("MapSlug(%q): err = %v, want INVALID_SLUG", tc.slug, err)
			}
			continue
		}
		if err != nil || entry != tc.wantEntry || route != tc.wantRoute {
			t.Errorf("MapSlug(%q) = (%q, %q, %v), want (%q, %q)", tc.slug, entry, route, err, tc.wantEntry, tc.wantRoute)
		}
	}
}

// TestNormalizePathTraversalCorpus is the FR-010 security corpus
// (EW-ARTIFACT-005 DoD: full traversal corpus incl. double-encoding).
func TestNormalizePathTraversalCorpus(t *testing.T) {
	valid := map[string]string{
		"/_next/static/chunks/a.js": "/_next/static/chunks/a.js",
		"/assets/img.png":           "/assets/img.png",
		"/fonts/inter%20bold.woff2": "/fonts/inter bold.woff2",
		"/component-styles.css":     "/component-styles.css",
	}
	for raw, want := range valid {
		got, err := NormalizePath(raw)
		if err != nil || got != want {
			t.Errorf("NormalizePath(%q) = (%q, %v), want %q", raw, got, err, want)
		}
	}

	hostile := []string{
		"/../etc/passwd",
		"/a/../../b",
		"/a/./b",
		"/%2e%2e/etc/passwd",           // encoded traversal
		"/%252e%252e/etc/passwd",       // double-encoded traversal
		"/%2E%2E/upper",                // uppercase encoding
		"/a//b",                        // empty segment
		"/a/",                          // trailing empty segment
		"/a\\b",                        // backslash separator
		"/C:/windows",                  // drive prefix
		"C:/windows",                   // bare drive prefix
		"/a/\x00null",                  // control character
		"/a/%00null",                   // encoded control character
		"relative/path",                // not absolute
		"/%zz",                         // malformed encoding
		"/%25252e%25252e/deep-encoded", // triple-encoded traversal
	}
	for _, raw := range hostile {
		_, err := NormalizePath(raw)
		var ce *errclass.Error
		if !errors.As(err, &ce) || ce.Code != events.ErrorCodePathTraversalDetected {
			t.Errorf("NormalizePath(%q): err = %v, want PATH_TRAVERSAL_DETECTED", raw, err)
		}
		if ce != nil && ce.Retryable() {
			t.Errorf("NormalizePath(%q) must be non-retryable", raw)
		}
	}
}

// TestStorageKeyConfinement: every key stays under the deployment prefix.
// Traversal is a dot SEGMENT, not a `..` substring — segment names containing
// dots (Next.js catch-all chunk directories like `[...slug]`) are legitimate.
func TestStorageKeyConfinement(t *testing.T) {
	base := "sites/site_01/deployments/dep_01"
	valid := []string{
		"/home/index.html",
		"/_next/static/chunks/app/[...slug]/page.js",
		"/assets/lib..min.js",
	}
	for _, p := range valid {
		key, err := StorageKey(base, p)
		if err != nil || key != base+p {
			t.Errorf("StorageKey(%q) = (%q, %v), want %q", p, key, err, base+p)
		}
	}
	hostile := []string{
		"../escape",
		"/..",
		"/a/../..",
		"/a/../b",
		"/a/./b",
		"/a//b",
		"/a/",
		`/a\..\b`,
		"relative/path",
	}
	for _, p := range hostile {
		_, err := StorageKey(base, p)
		var ce *errclass.Error
		if !errors.As(err, &ce) || ce.Code != events.ErrorCodePathTraversalDetected {
			t.Errorf("StorageKey(%q): err = %v, want PATH_TRAVERSAL_DETECTED", p, err)
		}
		if ce != nil && ce.Retryable() {
			t.Errorf("StorageKey(%q) must be non-retryable", p)
		}
	}
}
