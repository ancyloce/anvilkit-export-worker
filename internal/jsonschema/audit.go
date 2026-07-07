package jsonschema

import (
	"fmt"
	"sort"
)

// The validator implements a fixed JSON Schema subset; anything outside it
// is silently ignored by Validate and would let a payload pass vacuously.
// Audit walks a schema and reports every construct outside the subset so a
// contract evolving past the validator breaks a test instead of silently
// bypassing validation. The same subset is implemented by the codegen
// validator in packages/contracts-codegen/generate.ts — keep the two in
// sync when extending either.

// supportedKeywords is the exact keyword vocabulary Validate implements,
// plus the annotation keywords it deliberately ignores.
var supportedKeywords = map[string]bool{
	// validation keywords (implemented)
	"type":                 true,
	"properties":           true,
	"required":             true,
	"additionalProperties": true, // boolean form only
	"enum":                 true,
	"const":                true,
	"pattern":              true,
	"minLength":            true,
	"minimum":              true,
	"maximum":              true,
	"items":                true, // single-schema form only
	// annotations (ignored, harmless)
	"$schema":     true,
	"$id":         true,
	"title":       true,
	"description": true,
	// format is annotation-only in draft 2020-12 unless the
	// format-assertion vocabulary is enabled (the frozen v1 schemas use it
	// on createdAt) — ignoring it is spec-conformant, not a bypass.
	"format": true,
}

// supportedTypes is the `type` vocabulary Validate implements.
var supportedTypes = map[string]bool{
	"object":  true,
	"array":   true,
	"string":  true,
	"integer": true,
	"number":  true,
	"boolean": true,
}

// Audit reports every construct in the raw schema source that Validate does
// not implement (empty = the schema is fully enforceable). Findings are
// sorted for deterministic output.
func Audit(schemaSource string) []string {
	schema, err := compile(schemaSource)
	if err != nil {
		return []string{err.Error()}
	}
	findings := audit(schema, "$")
	sort.Strings(findings)
	return findings
}

func audit(schema map[string]any, path string) []string {
	var findings []string
	for key, val := range schema {
		if !supportedKeywords[key] {
			findings = append(findings, fmt.Sprintf("%s: unsupported keyword %q", path, key))
			continue
		}
		switch key {
		case "type":
			if t, ok := val.(string); !ok || !supportedTypes[t] {
				findings = append(findings, fmt.Sprintf("%s: unsupported type %v", path, val))
			}
		case "additionalProperties":
			if _, ok := val.(bool); !ok {
				findings = append(findings, fmt.Sprintf(
					"%s: additionalProperties must be boolean (schema form is not enforced)", path))
			}
		case "properties":
			props, ok := val.(map[string]any)
			if !ok {
				findings = append(findings, fmt.Sprintf("%s: properties must be an object", path))
				continue
			}
			for name, sub := range props {
				subSchema, ok := sub.(map[string]any)
				if !ok {
					findings = append(findings, fmt.Sprintf("%s.%s: property schema must be an object", path, name))
					continue
				}
				findings = append(findings, audit(subSchema, path+"."+name)...)
			}
		case "items":
			sub, ok := val.(map[string]any)
			if !ok {
				findings = append(findings, fmt.Sprintf(
					"%s: items must be a single schema object (tuple form is not enforced)", path))
				continue
			}
			findings = append(findings, audit(sub, path+".items")...)
		}
	}
	return findings
}
