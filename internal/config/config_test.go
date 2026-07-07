package config

import (
	"strings"
	"testing"
	"time"
)

// validEnv is a complete, valid configuration base for tests.
func validEnv() map[string]string {
	return map[string]string{
		"QUEUE_DRIVER":            "redis",
		"REDIS_URL":               "redis://redis:6379",
		"RENDER_ORIGIN_URL":       "http://render-origin:3000",
		"DEPLOYMENT_SERVICE_URL":  "http://deployment-service:8080",
		"INTERNAL_SERVICE_TOKEN":  "test-token",
		"ARTIFACT_STORAGE_DRIVER": "s3",
		"ARTIFACT_BUCKET":         "anvilkit-artifacts",
		"S3_ENDPOINT":             "http://minio:9000",
		"S3_ACCESS_KEY":           "minioadmin",
		"S3_SECRET_KEY":           "minioadmin",
		"ENVIRONMENT":             "production",
	}
}

func lookupFrom(env map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		v, ok := env[key]
		return v, ok
	}
}

func TestLoadValidDefaults(t *testing.T) {
	c, err := Load(lookupFrom(validEnv()))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.WorkerName != "anvilkit-export-worker" {
		t.Errorf("WorkerName default = %q (ADR-015)", c.WorkerName)
	}
	if c.WorkerConcurrency != 4 {
		t.Errorf("WorkerConcurrency default = %d, want 4", c.WorkerConcurrency)
	}
	if c.RenderTimeout.Milliseconds() != 15000 {
		t.Errorf("RenderTimeout default = %v, want 15s", c.RenderTimeout)
	}
	if got := strings.Join(c.DependencyAllowlist, ","); got != "/_next/static/*,/assets/*,/fonts/*,/component-styles.css" {
		t.Errorf("DependencyAllowlist default = %q", got)
	}
	if len(c.ExternalAssetAllowlist) != 0 {
		t.Error("ExternalAssetAllowlist must default to empty (deny-by-default)")
	}
	if c.HealthPort != 8081 || c.MetricsPort != 9091 {
		t.Errorf("ports = %d/%d, want 8081/9091", c.HealthPort, c.MetricsPort)
	}
}

// TestLoadFailFastAggregates: a worker with missing required config must
// refuse to boot and report every problem at once (FR-019, AC-011).
func TestLoadFailFastAggregates(t *testing.T) {
	_, err := Load(lookupFrom(map[string]string{}))
	if err == nil {
		t.Fatal("Load with empty env must fail")
	}
	msg := err.Error()
	if !strings.Contains(msg, "CONFIG_MISSING") {
		t.Errorf("error must carry the CONFIG_MISSING class: %v", msg)
	}
	for _, key := range []string{
		"QUEUE_DRIVER", "RENDER_ORIGIN_URL", "DEPLOYMENT_SERVICE_URL",
		"INTERNAL_SERVICE_TOKEN", "ARTIFACT_BUCKET",
		"S3_ENDPOINT", "S3_ACCESS_KEY", "S3_SECRET_KEY", "ENVIRONMENT",
	} {
		if !strings.Contains(msg, key) {
			t.Errorf("aggregated error must mention %s: %v", key, msg)
		}
	}
}

func TestRedisURLRequiredForRedisDriver(t *testing.T) {
	env := validEnv()
	delete(env, "REDIS_URL")
	if _, err := Load(lookupFrom(env)); err == nil || !strings.Contains(err.Error(), "REDIS_URL") {
		t.Fatalf("missing REDIS_URL with redis driver must fail, got %v", err)
	}
}

func TestKafkaDriverRejectedAtMVP(t *testing.T) {
	env := validEnv()
	env["QUEUE_DRIVER"] = "kafka"
	if _, err := Load(lookupFrom(env)); err == nil || !strings.Contains(err.Error(), "kafka") {
		t.Fatalf("kafka driver must be rejected at MVP (FR-021 seam), got %v", err)
	}
}

// TestDemoGuard is T-demo-guard (AC-011): apps/demo render targets are
// rejected at startup outside local development and allowed inside it.
func TestDemoGuard(t *testing.T) {
	cases := []struct {
		name        string
		environment string
		originURL   string
		wantReject  bool
	}{
		{"apps/demo rejected in production", "production", "http://studio:3000/apps/demo", true},
		{"apps/demo rejected in staging", "staging", "http://studio:3000/apps/demo", true},
		{"nested apps/demo rejected in production", "production", "http://studio:3000/x/apps/demo/page", true},
		{"apps/demo allowed in local", "local", "http://studio:3000/apps/demo", false},
		{"apps/demo allowed in development", "development", "http://localhost:3000/apps/demo", false},
		{"denylisted demo host rejected in production", "production", "http://demo:3000", true},
		{"apps-demo host rejected in production", "production", "http://apps-demo:3000", true},
		{"loopback rejected in production", "production", "http://localhost:3000", true},
		{"loopback allowed in local", "local", "http://localhost:3000", false},
		{"real origin allowed in production", "production", "http://render-origin:3000", false},
		{"relative URL rejected everywhere", "local", "render-origin:3000/apps/demo", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := validEnv()
			env["ENVIRONMENT"] = tc.environment
			env["RENDER_ORIGIN_URL"] = tc.originURL
			_, err := Load(lookupFrom(env))
			if tc.wantReject && err == nil {
				t.Fatalf("expected startup rejection for %q in %s", tc.originURL, tc.environment)
			}
			if !tc.wantReject && err != nil {
				t.Fatalf("expected acceptance for %q in %s, got %v", tc.originURL, tc.environment, err)
			}
		})
	}
}

// TestDryRunGuard: WORKER_DRY_RUN is a local-development scaffold mode only.
func TestDryRunGuard(t *testing.T) {
	env := validEnv()
	env["WORKER_DRY_RUN"] = "true"
	if _, err := Load(lookupFrom(env)); err == nil || !strings.Contains(err.Error(), "WORKER_DRY_RUN") {
		t.Fatalf("dry-run must be rejected in production, got %v", err)
	}
	env["ENVIRONMENT"] = "local"
	c, err := Load(lookupFrom(env))
	if err != nil {
		t.Fatalf("dry-run must be allowed in local: %v", err)
	}
	if !c.WorkerDryRun {
		t.Error("WorkerDryRun not set")
	}
}

// TestAssetServiceURLOptional: no worker code path calls asset-service yet,
// so the variable must not block boot — but a set value must still be a
// valid absolute URL (fail-fast on typos).
func TestAssetServiceURLOptional(t *testing.T) {
	c, err := Load(lookupFrom(validEnv())) // validEnv omits ASSET_SERVICE_URL
	if err != nil {
		t.Fatalf("boot without ASSET_SERVICE_URL must succeed: %v", err)
	}
	if c.AssetServiceURL != "" {
		t.Errorf("AssetServiceURL = %q, want empty", c.AssetServiceURL)
	}

	env := validEnv()
	env["ASSET_SERVICE_URL"] = "http://asset-service:8080"
	if c, err = Load(lookupFrom(env)); err != nil || c.AssetServiceURL != "http://asset-service:8080" {
		t.Fatalf("valid ASSET_SERVICE_URL rejected: %v", err)
	}

	env["ASSET_SERVICE_URL"] = "asset-service:8080/no-scheme"
	if _, err = Load(lookupFrom(env)); err == nil || !strings.Contains(err.Error(), "ASSET_SERVICE_URL") {
		t.Fatalf("relative ASSET_SERVICE_URL must fail fast, got %v", err)
	}
}

// TestSizeLimitDefaultsAndValidation covers the M5 same-origin output
// bounds: safe defaults, fail-fast on zero/negative/garbage.
func TestSizeLimitDefaultsAndValidation(t *testing.T) {
	c, err := Load(lookupFrom(validEnv()))
	if err != nil {
		t.Fatal(err)
	}
	if c.MaxRenderHTMLBytes != 10<<20 || c.MaxRenderAssetBytes != 25<<20 || c.MaxTotalArtifactBytes != 512<<20 {
		t.Errorf("size-limit defaults = %d/%d/%d, want 10MiB/25MiB/512MiB",
			c.MaxRenderHTMLBytes, c.MaxRenderAssetBytes, c.MaxTotalArtifactBytes)
	}
	for _, key := range []string{"MAX_RENDER_HTML_BYTES", "MAX_RENDER_ASSET_BYTES", "MAX_TOTAL_ARTIFACT_BYTES"} {
		for _, bad := range []string{"0", "-1", "ten"} {
			env := validEnv()
			env[key] = bad
			if _, err := Load(lookupFrom(env)); err == nil || !strings.Contains(err.Error(), key) {
				t.Errorf("%s=%q must fail fast, got %v", key, bad, err)
			}
		}
	}
	env := validEnv()
	env["MAX_RENDER_HTML_BYTES"] = "1048576"
	c, err = Load(lookupFrom(env))
	if err != nil || c.MaxRenderHTMLBytes != 1<<20 {
		t.Errorf("explicit MAX_RENDER_HTML_BYTES not applied: %d, %v", c.MaxRenderHTMLBytes, err)
	}
}

// TestStreamRetentionDefaultsAndValidation covers the ADR-011 retention
// config: floor defaults, 0 = disabled accepted, negatives/garbage rejected.
func TestStreamRetentionDefaultsAndValidation(t *testing.T) {
	c, err := Load(lookupFrom(validEnv()))
	if err != nil {
		t.Fatal(err)
	}
	if c.StreamMainRetention != 72*time.Hour {
		t.Errorf("StreamMainRetention default = %v, want 72h (ADR-011 production floor)", c.StreamMainRetention)
	}
	for name, got := range map[string]time.Duration{
		"dlq": c.StreamDLQRetention, "ready": c.StreamReadyRetention, "failed": c.StreamFailedRetention,
	} {
		if got != 7*24*time.Hour {
			t.Errorf("%s retention default = %v, want 168h", name, got)
		}
	}

	env := validEnv()
	env["STREAM_MAIN_RETENTION_MS"] = "0"
	if c, err = Load(lookupFrom(env)); err != nil || c.StreamMainRetention != 0 {
		t.Errorf("STREAM_MAIN_RETENTION_MS=0 (disabled) must load: %v, %v", c.StreamMainRetention, err)
	}
	for _, bad := range []string{"-1", "week"} {
		env := validEnv()
		env["STREAM_DLQ_RETENTION_MS"] = bad
		if _, err := Load(lookupFrom(env)); err == nil || !strings.Contains(err.Error(), "STREAM_DLQ_RETENTION_MS") {
			t.Errorf("STREAM_DLQ_RETENTION_MS=%q must fail fast, got %v", bad, err)
		}
	}
}

func TestLockTTLInputsHaveDefaults(t *testing.T) {
	c, err := Load(lookupFrom(validEnv()))
	if err != nil {
		t.Fatal(err)
	}
	if c.RenderTimeout <= 0 || c.UploadTimeout <= 0 {
		t.Fatalf("timeouts must default positive (lock TTL formula inputs): render=%v upload=%v",
			c.RenderTimeout, c.UploadTimeout)
	}
}
