package buildinfo

import "testing"

// TestCanonicalName guards the ADR-015 naming decision: the service identity
// is anvilkit-export-worker on every surface, never the superseded
// render-worker / static-publisher names.
func TestCanonicalName(t *testing.T) {
	if Name != "anvilkit-export-worker" {
		t.Fatalf("Name = %q, want %q (ADR-015)", Name, "anvilkit-export-worker")
	}
	if Version == "" {
		t.Fatal("Version must never be empty (default \"dev\")")
	}
}
