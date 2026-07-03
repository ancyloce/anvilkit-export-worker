// Package jsonschema is a minimal JSON Schema (draft 2020-12 subset)
// validator covering exactly the keywords used by the frozen contracts:
// type, properties, required, additionalProperties, enum, const, pattern,
// minLength, minimum, maximum, items. It mirrors the codegen validator in
// packages/contracts-codegen/generate.ts, so worker-side validation and
// contract-time validation agree.
package jsonschema

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sync"
)

var compiled sync.Map // schema source → map[string]any

// Validate checks a decoded JSON value against the raw schema source and
// returns human-readable violations (empty = valid).
func Validate(schemaSource string, value any) []string {
	schema, err := compile(schemaSource)
	if err != nil {
		return []string{err.Error()}
	}
	return validate(schema, value, "$")
}

// ValidateBytes decodes payload and validates it.
func ValidateBytes(schemaSource string, payload []byte) []string {
	var value any
	if err := json.Unmarshal(payload, &value); err != nil {
		return []string{fmt.Sprintf("$: not valid JSON: %v", err)}
	}
	return Validate(schemaSource, value)
}

func compile(source string) (map[string]any, error) {
	if cached, ok := compiled.Load(source); ok {
		return cached.(map[string]any), nil
	}
	var schema map[string]any
	if err := json.Unmarshal([]byte(source), &schema); err != nil {
		return nil, fmt.Errorf("schema source is invalid JSON: %w", err)
	}
	compiled.Store(source, schema)
	return schema, nil
}

func validate(schema map[string]any, value any, path string) []string {
	var violations []string

	if c, ok := schema["const"]; ok {
		if !jsonEqual(c, value) {
			violations = append(violations, fmt.Sprintf("%s: expected const %v", path, c))
		}
		return violations
	}
	if enum, ok := schema["enum"].([]any); ok {
		matched := false
		for _, e := range enum {
			if jsonEqual(e, value) {
				matched = true
				break
			}
		}
		if !matched {
			return append(violations, fmt.Sprintf("%s: value %v not in enum", path, value))
		}
	}

	switch schema["type"] {
	case "object":
		obj, ok := value.(map[string]any)
		if !ok {
			return append(violations, path+": expected object")
		}
		props, _ := schema["properties"].(map[string]any)
		if required, ok := schema["required"].([]any); ok {
			for _, r := range required {
				if _, present := obj[r.(string)]; !present {
					violations = append(violations, fmt.Sprintf("%s: missing required property %s", path, r))
				}
			}
		}
		for key, val := range obj {
			propSchema, known := props[key].(map[string]any)
			if known {
				violations = append(violations, validate(propSchema, val, path+"."+key)...)
			} else if ap, ok := schema["additionalProperties"].(bool); ok && !ap {
				violations = append(violations, fmt.Sprintf("%s: additional property %s not allowed", path, key))
			}
		}
	case "array":
		arr, ok := value.([]any)
		if !ok {
			return append(violations, path+": expected array")
		}
		if items, ok := schema["items"].(map[string]any); ok {
			for i, v := range arr {
				violations = append(violations, validate(items, v, fmt.Sprintf("%s[%d]", path, i))...)
			}
		}
	case "string":
		s, ok := value.(string)
		if !ok {
			return append(violations, path+": expected string")
		}
		if min, ok := schema["minLength"].(float64); ok && float64(len(s)) < min {
			violations = append(violations, fmt.Sprintf("%s: shorter than minLength %v", path, min))
		}
		if pattern, ok := schema["pattern"].(string); ok {
			if re, err := regexp.Compile(pattern); err == nil && !re.MatchString(s) {
				violations = append(violations, fmt.Sprintf("%s: does not match pattern %s", path, pattern))
			}
		}
	case "integer", "number":
		n, ok := value.(float64)
		if !ok {
			return append(violations, fmt.Sprintf("%s: expected %s", path, schema["type"]))
		}
		if schema["type"] == "integer" && n != float64(int64(n)) {
			return append(violations, path+": expected integer")
		}
		if min, ok := schema["minimum"].(float64); ok && n < min {
			violations = append(violations, fmt.Sprintf("%s: below minimum %v", path, min))
		}
		if max, ok := schema["maximum"].(float64); ok && n > max {
			violations = append(violations, fmt.Sprintf("%s: above maximum %v", path, max))
		}
	case "boolean":
		if _, ok := value.(bool); !ok {
			violations = append(violations, path+": expected boolean")
		}
	}
	return violations
}

func jsonEqual(a, b any) bool {
	ab, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	return string(ab) == string(bb)
}
