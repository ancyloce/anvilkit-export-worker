// Package harvest owns render output guards, deterministic dependency
// harvesting, allowlist enforcement, and path safety (FR-008, FR-009,
// FR-010; EW-ARTIFACT-001..006). The worker is a post-render verifier:
// asset:// resolution belongs upstream to render-origin/render-runtime
// (PRD 0008 §13.7).
package harvest

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/ancyloce/anvilkit-export-worker/contracts/events"
	"github.com/ancyloce/anvilkit-export-worker/internal/errclass"
)

var drivePrefixRe = regexp.MustCompile(`^[A-Za-z]:`)

// MapSlug maps a page slug to its artifact entry path and public route path
// (EW-ARTIFACT-006): home → /home/index.html · "/" or index → /index.html ·
// nested slugs preserved (blog/hello → /blog/hello/index.html). Invalid
// slugs classify INVALID_SLUG.
func MapSlug(slug string) (entryPath, routePath string, err error) {
	trimmed := strings.TrimSpace(slug)
	if trimmed == "/" || trimmed == "index" || trimmed == "" {
		return "/index.html", "/", nil
	}
	normalized, nerr := NormalizePath("/" + strings.Trim(trimmed, "/"))
	if nerr != nil {
		return "", "", errclass.New(events.ErrorCodeInvalidSlug, events.FailedStageHarvestDependencies,
			fmt.Errorf("slug %q fails path rules: %w", slug, nerr))
	}
	return normalized + "/index.html", normalized, nil
}

// NormalizePath implements the FR-010 rule: URL-decode THEN normalize.
// It decodes iteratively (double-encoded traversal is still traversal),
// then rejects `..` and `.` segments, empty segments, control characters,
// backslashes, and Windows drive prefixes. Violations classify
// PATH_TRAVERSAL_DETECTED. The result always has a leading slash and no
// trailing slash.
func NormalizePath(raw string) (string, error) {
	violation := func(format string, args ...any) error {
		return errclass.New(events.ErrorCodePathTraversalDetected, events.FailedStageHarvestDependencies,
			fmt.Errorf(format, args...))
	}

	decoded := raw
	for range 4 {
		next, err := url.PathUnescape(decoded)
		if err != nil {
			return "", violation("malformed percent-encoding in path %q", raw)
		}
		if next == decoded {
			break
		}
		decoded = next
	}
	if strings.Contains(decoded, "%") {
		// Still encoded after four rounds: hostile input.
		return "", violation("unresolvable percent-encoding in path %q", raw)
	}

	for _, r := range decoded {
		if r < 0x20 || r == 0x7f {
			return "", violation("control character in path %q", raw)
		}
	}
	if strings.Contains(decoded, `\`) {
		return "", violation("backslash separator in path %q", raw)
	}
	if drivePrefixRe.MatchString(strings.TrimPrefix(decoded, "/")) || drivePrefixRe.MatchString(decoded) {
		return "", violation("drive prefix in path %q", raw)
	}
	if !strings.HasPrefix(decoded, "/") {
		return "", violation("path %q is not absolute", raw)
	}

	segments := strings.Split(strings.TrimPrefix(decoded, "/"), "/")
	for _, seg := range segments {
		switch seg {
		case "":
			return "", violation("empty segment in path %q", raw)
		case ".", "..":
			return "", violation("dot segment in path %q", raw)
		}
	}
	return "/" + strings.Join(segments, "/"), nil
}

// StorageKey joins the deployment base path with an artifact-relative path,
// asserting confinement (FR-010: every key stays under
// sites/{siteId}/deployments/{deploymentId}/).
func StorageKey(basePath, artifactPath string) (string, error) {
	if !strings.HasPrefix(artifactPath, "/") || strings.Contains(artifactPath, "..") {
		return "", errclass.New(events.ErrorCodePathTraversalDetected, events.FailedStageUploadArtifacts,
			fmt.Errorf("artifact path %q escapes the deployment prefix", artifactPath))
	}
	return basePath + artifactPath, nil
}
