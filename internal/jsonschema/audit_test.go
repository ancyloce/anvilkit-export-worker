package jsonschema_test

import (
	"strings"
	"testing"

	"github.com/ancyloce/anvilkit-export-worker/contracts/artifact"
	"github.com/ancyloce/anvilkit-export-worker/contracts/events"
	"github.com/ancyloce/anvilkit-export-worker/internal/jsonschema"
)

// TestAuditFlagsUnsupportedConstructs: every construct outside the validator
// subset is reported — these are exactly the shapes that would make
// validation pass vacuously.
func TestAuditFlagsUnsupportedConstructs(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		expect string
	}{
		{"oneOf", `{"type":"object","oneOf":[{"type":"string"}]}`, `unsupported keyword "oneOf"`},
		{"ref", `{"$ref":"#/$defs/thing"}`, `unsupported keyword "$ref"`},
		{"defs", `{"$defs":{"thing":{"type":"string"}}}`, `unsupported keyword "$defs"`},
		{"maxLength", `{"type":"string","maxLength":10}`, `unsupported keyword "maxLength"`},
		{"type null", `{"type":"null"}`, "unsupported type null"},
		{"type union", `{"type":["string","integer"]}`, "unsupported type"},
		{"additionalProperties schema", `{"type":"object","additionalProperties":{"type":"string"}}`, "additionalProperties must be boolean"},
		{"items tuple", `{"type":"array","items":[{"type":"string"}]}`, "items must be a single schema object"},
		{"nested in properties", `{"type":"object","properties":{"x":{"type":"string","contentEncoding":"base64"}}}`, `$.x: unsupported keyword "contentEncoding"`},
		{"nested in items", `{"type":"array","items":{"anyOf":[{"type":"string"}]}}`, `$.items: unsupported keyword "anyOf"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			findings := jsonschema.Audit(tc.schema)
			if len(findings) == 0 {
				t.Fatalf("Audit accepted a schema the validator cannot enforce: %s", tc.schema)
			}
			if !strings.Contains(strings.Join(findings, "; "), tc.expect) {
				t.Errorf("findings = %v, want one containing %q", findings, tc.expect)
			}
		})
	}
}

// TestAuditAcceptsSupportedSubset: a schema using the full supported
// vocabulary audits clean.
func TestAuditAcceptsSupportedSubset(t *testing.T) {
	schema := `{
		"$schema": "https://json-schema.org/draft/2020-12/schema",
		"$id": "https://example.test/x.json",
		"title": "x", "description": "d",
		"type": "object",
		"additionalProperties": false,
		"required": ["a", "b"],
		"properties": {
			"a": {"type": "string", "minLength": 1, "pattern": "^[a-z]+$"},
			"b": {"type": "integer", "minimum": 0, "maximum": 10},
			"c": {"const": 1},
			"d": {"enum": ["x", "y"]},
			"e": {"type": "boolean"},
			"f": {"type": "array", "items": {"type": "number"}},
			"g": {"type": "string", "format": "date-time"}
		}
	}`
	if findings := jsonschema.Audit(schema); len(findings) != 0 {
		t.Fatalf("supported-subset schema flagged: %v", findings)
	}
}

// TestEmbeddedContractSchemasStayInsideValidatorSubset is the contract
// guard: if a frozen (or future) schema uses a keyword the worker validator
// ignores, this test fails — contract evolution can never silently bypass
// event/manifest validation (AC-023).
func TestEmbeddedContractSchemasStayInsideValidatorSubset(t *testing.T) {
	schemas := map[string]string{
		"deployment.export.requested": events.SchemaExportRequested,
		"deployment.artifact.ready":   events.SchemaArtifactReady,
		"deployment.export.failed":    events.SchemaExportFailed,
		"artifact-manifest":           artifact.SchemaArtifactManifest,
	}
	for name, source := range schemas {
		if findings := jsonschema.Audit(source); len(findings) != 0 {
			t.Errorf("%s uses constructs the worker validator does not enforce:\n  %s",
				name, strings.Join(findings, "\n  "))
		}
	}
}
