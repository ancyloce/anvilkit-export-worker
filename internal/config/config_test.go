package config

import (
	"strings"
	"testing"
)

// validEnv is a complete, valid configuration base for tests.
func validEnv() map[string]string {
	return map[string]string{
		"QUEUE_DRIVER":            "redis",
		"REDIS_URL":               "redis://redis:6379",
		"RENDER_ORIGIN_URL":       "http://render-origin:3000",
		"DEPLOYMENT_SERVICE_URL":  "http://deployment-service:8080",
		"ASSET_SERVICE_URL":       "http://asset-service:8080",
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
		"ASSET_SERVICE_URL", "INTERNAL_SERVICE_TOKEN", "ARTIFACT_BUCKET",
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
