// Log assertions over a REAL full-pipeline run (AC-015 log side, AC-022
// leak side): every entry is JSON, job-scoped entries carry the required
// §15.1 fields, completion entries carry status/durationMs/errorCode, and
// no token or storage-credential material appears anywhere in the output.
package worker_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ancyloce/anvilkit-export-worker/contracts/deploymentservice"
	"github.com/ancyloce/anvilkit-export-worker/internal/export"
	"github.com/ancyloce/anvilkit-export-worker/internal/obs"
	"github.com/ancyloce/anvilkit-export-worker/internal/worker"
)

// syncBuffer is a race-safe log sink (heartbeat goroutines log too).
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestLogFieldsAndTokenLeakOnRealPipeline(t *testing.T) {
	sink := &syncBuffer{}
	h := newHardeningStack(t, "dep_logs", func(p *export.Pipeline, d *worker.Deps) {
		// The production logger construction: secrets redacted (§11.1).
		d.Log = obs.NewLogger(sink, "debug", "log-test-worker",
			[]string{"test-token", "minioadmin"})
	})
	publishHardeningEvent(t, h.driver, "dep_logs")
	msgs, err := h.driver.Fetch(context.Background(), 1, time.Second)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("fetch: %v %v", msgs, err)
	}
	if out := h.proc.Handle(context.Background(), msgs[0]); out != worker.OutcomeSuccess {
		t.Fatalf("outcome = %s", out)
	}
	if h.svc.status() != deploymentservice.DeploymentStatusArtifactReady {
		t.Fatalf("status = %s", h.svc.status())
	}

	output := sink.String()

	// AC-022: no token or storage-credential material in any log output.
	for _, secret := range []string{"test-token", "minioadmin"} {
		if strings.Contains(output, secret) {
			t.Fatalf("secret %q leaked into logs:\n%s", secret, output)
		}
	}

	required := []string{
		"traceId", "eventId", "deploymentId", "teamId", "siteId", "pageId",
		"slug", "version", "environment", "renderMode", "workerId", "attempt", "stage",
	}
	var jobEntries, completionSeen int
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("non-JSON log line: %q", line)
		}
		if entry["deploymentId"] != "dep_logs" {
			continue // non-job-scoped infrastructure entry
		}
		jobEntries++
		for _, field := range required {
			if _, ok := entry[field]; !ok {
				t.Errorf("job entry missing required field %q (§15.1): %s", field, line)
			}
		}
		if entry["msg"] == "job succeeded" {
			completionSeen++
			for _, field := range []string{"status", "durationMs", "errorCode"} {
				if _, ok := entry[field]; !ok {
					t.Errorf("completion entry missing %q: %s", field, line)
				}
			}
		}
	}
	if jobEntries < 4 {
		t.Errorf("expected several job-scoped entries, got %d", jobEntries)
	}
	if completionSeen != 1 {
		t.Errorf("completion entries = %d, want 1", completionSeen)
	}
}
