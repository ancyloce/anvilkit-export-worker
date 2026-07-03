// Contract tests for the artifact-manifest bindings (EW-STORAGE-005 contract
// side; AC-023 companion).
package artifact_test

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/ancyloce/anvilkit-export-worker/contracts/artifact"
)

func TestManifestFixtureRoundTrip(t *testing.T) {
	raw, err := os.ReadFile("testdata/artifact-manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	var m artifact.Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	remarshaled, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var want, got map[string]any
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(remarshaled, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("fixture does not round-trip:\nwant %v\ngot  %v", want, got)
	}

	if m.SchemaVersion != 1 || m.Entry != "/home/index.html" || len(m.Files) != 2 {
		t.Errorf("decoded manifest = %+v", m)
	}
	if m.Routes == nil {
		t.Fatal("routes[] must always be an array (FR-012 invariant)")
	}
	for _, f := range m.Files {
		if !strings.HasPrefix(f.ContentHash, "sha256-") {
			t.Errorf("contentHash %q must be sha256- form (never an ETag)", f.ContentHash)
		}
		if !strings.HasPrefix(f.StorageKey, m.ArtifactBasePath+"/") {
			t.Errorf("storageKey %q must live under artifactBasePath %q", f.StorageKey, m.ArtifactBasePath)
		}
	}
}

func TestManifestSchemaEmbedded(t *testing.T) {
	var doc map[string]any
	if err := json.Unmarshal([]byte(artifact.SchemaArtifactManifest), &doc); err != nil {
		t.Fatalf("embedded schema invalid: %v", err)
	}
	if doc["title"] != "artifact-manifest" {
		t.Errorf("title = %v", doc["title"])
	}
}
