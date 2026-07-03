package storage

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/ancyloce/anvilkit-export-worker/contracts/artifact"
	"github.com/ancyloce/anvilkit-export-worker/contracts/deploymentservice"
	"github.com/ancyloce/anvilkit-export-worker/contracts/events"
	"github.com/ancyloce/anvilkit-export-worker/internal/errclass"
	"github.com/ancyloce/anvilkit-export-worker/internal/jsonschema"
)

// Cache-control classes (PRD 0008 §9.3, FR-012).
const (
	CacheControlHTML      = "public, max-age=60, stale-while-revalidate=300"
	CacheControlHashed    = "public, max-age=31536000, immutable"
	CacheControlNonHashed = "public, max-age=3600"
	// CacheControlManifest: the manifest is INTERNAL-ONLY — never uploaded
	// to public CDN paths (AC-017).
	CacheControlManifest = "private, max-age=0, no-store"
)

// ManifestFilename is the well-known manifest object name under the
// deployment base path.
const ManifestFilename = "artifact-manifest.json"

// hashedNameRe implements the PRD 0008 §9.3 hash-detection rule (gap G-6):
// a filename containing a content-hash segment, e.g. app.3f9d21ab.css.
var hashedNameRe = regexp.MustCompile(`\.[0-9a-f]{8,}\.[A-Za-z0-9]+$`)

// IsHashedAsset reports whether path is content-hashed (immutable class):
// hash-pattern filenames or anything under /_next/static/.
func IsHashedAsset(path string) bool {
	return strings.HasPrefix(path, "/_next/static/") || hashedNameRe.MatchString(path)
}

// CacheControlFor assigns the per-class cache policy.
func CacheControlFor(path, mimeType string) string {
	if strings.HasPrefix(mimeType, "text/html") || strings.HasSuffix(path, ".html") {
		return CacheControlHTML
	}
	if IsHashedAsset(path) {
		return CacheControlHashed
	}
	return CacheControlNonHashed
}

// ManifestFileInput is one uploaded artifact file, as recorded in the
// manifest.
type ManifestFileInput struct {
	Path         string
	StorageKey   string
	SHA256Hex    string
	SizeBytes    int64
	MimeType     string
	CacheControl string
}

// BuildManifest assembles and self-validates artifact-manifest.json
// (FR-012; EW-STORAGE-005). routes[] is always an array — the single-page
// MVP emits exactly one route. Returns the manifest, its canonical JSON
// encoding, and its digest.
func BuildManifest(rec *deploymentservice.DeploymentRecord, basePath, entry, route string,
	files []ManifestFileInput, now time.Time) (*artifact.Manifest, []byte, string, error) {

	m := &artifact.Manifest{
		SchemaVersion:    1,
		DeploymentID:     rec.DeploymentID,
		SiteID:           rec.SiteID,
		PageID:           rec.PageID,
		Slug:             rec.Slug,
		Version:          rec.Version,
		Environment:      rec.Environment,
		RenderMode:       rec.RenderMode,
		ArtifactBasePath: basePath,
		Entry:            entry,
		Files:            make([]artifact.ManifestFile, 0, len(files)),
		Routes:           []artifact.ManifestRoute{{Path: route, Entry: entry}},
		CreatedAt:        now.UTC().Format(time.RFC3339),
	}
	for _, f := range files {
		m.Files = append(m.Files, artifact.ManifestFile{
			Path:         f.Path,
			StorageKey:   f.StorageKey,
			ContentHash:  "sha256-" + f.SHA256Hex,
			SizeBytes:    f.SizeBytes,
			MimeType:     f.MimeType,
			CacheControl: f.CacheControl,
		})
	}

	encoded, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, nil, "", errclass.New(events.ErrorCodeValidationFailed, events.FailedStageWriteManifest, err)
	}
	encoded = append(encoded, '\n')

	// Self-validation against the frozen contract schema (EW-STORAGE-005
	// DoD): a manifest the worker cannot validate must never be submitted.
	if violations := jsonschema.ValidateBytes(artifact.SchemaArtifactManifest, encoded); len(violations) > 0 {
		return nil, nil, "", errclass.New(events.ErrorCodeValidationFailed, events.FailedStageWriteManifest,
			fmt.Errorf("generated manifest failed schema self-validation: %s", strings.Join(violations, "; ")))
	}

	return m, encoded, "sha256-" + SHA256Hex(encoded), nil
}
