package queue

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/ancyloce/anvilkit-export-worker/contracts/events"
	"github.com/ancyloce/anvilkit-export-worker/internal/errclass"
	"github.com/ancyloce/anvilkit-export-worker/internal/jsonschema"
)

// ErrUnparseable marks input from which no deploymentId can be extracted —
// the DLQ-with-alert route (FR-003): the original is acked only after
// successful DLQ handoff, and no status update is ever attempted.
var ErrUnparseable = errors.New("unparseable event: no extractable deploymentId")

// ParseEvent validates payload against the frozen v1 inbound schema
// (embedded contract of record) and decodes it.
//
//   - JSON-invalid payloads, or payloads without a non-empty string
//     deploymentId, return an error wrapping ErrUnparseable.
//   - Schema-invalid payloads that do carry a deploymentId return a
//     classified non-retryable VALIDATION_FAILED (§13).
func ParseEvent(payload []byte) (*events.ExportRequested, error) {
	var generic any
	if err := json.Unmarshal(payload, &generic); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnparseable, err)
	}
	obj, ok := generic.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: payload is not a JSON object", ErrUnparseable)
	}
	dep, ok := obj["deploymentId"].(string)
	if !ok || strings.TrimSpace(dep) == "" {
		return nil, fmt.Errorf("%w: deploymentId missing or not a string", ErrUnparseable)
	}

	if violations := jsonschema.Validate(events.SchemaExportRequested, generic); len(violations) > 0 {
		return nil, errclass.New(events.ErrorCodeValidationFailed, events.FailedStageConsumeJob,
			fmt.Errorf("schema-invalid event payload: %s", strings.Join(violations, "; ")))
	}

	var ev events.ExportRequested
	if err := json.Unmarshal(payload, &ev); err != nil {
		return nil, errclass.New(events.ErrorCodeValidationFailed, events.FailedStageConsumeJob,
			fmt.Errorf("decode event: %w", err))
	}
	return &ev, nil
}
