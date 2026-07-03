// Package config owns configuration loading for all PRD 0010 §14 environment
// variables with startup fail-fast validation (FR-019, EW-CONFIG-001): a
// misconfigured worker refuses to boot (CONFIG_MISSING class) and never
// starts jobs. It also owns the demo guard (EW-CONFIG-002, ADR-010) and the
// local-only dry-run guard, both driven by ENVIRONMENT strictness.
package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Environment values recognized for guard strictness (ADR-010): local and
// development are "local dev" (relaxed); staging and production are deployed
// (strict).
const (
	EnvLocal       = "local"
	EnvDevelopment = "development"
	EnvStaging     = "staging"
	EnvProduction  = "production"
)

// demoHostDenylist rejects render-origin hosts that indicate the demo app or
// a loopback target in deployed environments — defense in depth on top of
// the apps/demo path check (ADR-010).
var demoHostDenylist = map[string]bool{
	"demo":       true,
	"apps-demo":  true,
	"demo.local": true,
	"localhost":  true,
	"127.0.0.1":  true,
	"::1":        true,
}

// Config carries every §14 configuration item. Field comments note defaults
// and requiredness; secrets are marked and must never be logged (§11.1).
type Config struct {
	WorkerName        string        // WORKER_NAME, default anvilkit-export-worker (ADR-015)
	WorkerConcurrency int           // WORKER_CONCURRENCY, default 4 (MVP 4–8)
	QueueDriver       string        // QUEUE_DRIVER, required: redis (MVP) | kafka (GA)
	RedisURL          string        // REDIS_URL, required with redis driver
	KafkaBrokers      string        // KAFKA_BROKERS, GA only
	RenderOriginURL   string        // RENDER_ORIGIN_URL, required; demo guard enforced
	RenderTimeout     time.Duration // RENDER_TIMEOUT_MS, default 15000
	UploadTimeout     time.Duration // UPLOAD_TIMEOUT_MS, default 60000 (Recommended Approach: upload budget backing ARTIFACT_UPLOAD_TIMEOUT and the lock TTL formula)

	DeploymentServiceURL string // DEPLOYMENT_SERVICE_URL, required
	AssetServiceURL      string // ASSET_SERVICE_URL, required
	InternalServiceToken string // INTERNAL_SERVICE_TOKEN, required — SECRET, never logged

	ArtifactStorageDriver string // ARTIFACT_STORAGE_DRIVER, required: s3
	ArtifactBucket        string // ARTIFACT_BUCKET, required
	ArtifactBasePrefix    string // ARTIFACT_BASE_PREFIX, default sites
	S3Endpoint            string // S3_ENDPOINT, required
	S3Region              string // S3_REGION, default us-east-1
	S3AccessKey           string // S3_ACCESS_KEY, required — SECRET
	S3SecretKey           string // S3_SECRET_KEY, required — SECRET

	DependencyAllowlist    []string // DEPENDENCY_ALLOWLIST, default per §14
	ExternalAssetAllowlist []string // EXTERNAL_ASSET_ALLOWLIST, default empty (deny-by-default)

	HealthPort   int    // HEALTH_PORT, default 8081 (internal-only)
	MetricsPort  int    // METRICS_PORT, default 9091 (internal-only)
	LogLevel     string // LOG_LEVEL, default info
	OTELEndpoint string // OTEL_EXPORTER_OTLP_ENDPOINT, optional (staging/prod)

	Environment string // ENVIRONMENT, required: local|development|staging|production

	// WorkerDryRun (WORKER_DRY_RUN) is the Milestone 2 scaffold mode: the
	// worker consumes, validates, loads, reconciles, and locks, but performs
	// no status writes and no export (the pipeline lands in M3). Allowed only
	// in local-dev environments — same strictness switch as the demo guard.
	WorkerDryRun bool
}

// LocalDev reports whether guards run relaxed (ADR-010 strictness switch).
func (c *Config) LocalDev() bool {
	return c.Environment == EnvLocal || c.Environment == EnvDevelopment
}

// Load reads configuration from lookup (usually os.LookupEnv), applies §14
// defaults, and fail-fast validates. All problems are aggregated into one
// error so a misconfigured boot reports everything at once.
func Load(lookup func(string) (string, bool)) (*Config, error) {
	get := func(key, def string) string {
		if v, ok := lookup(key); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
		return def
	}

	var problems []string
	require := func(key string) string {
		v := get(key, "")
		if v == "" {
			problems = append(problems, fmt.Sprintf("%s is required", key))
		}
		return v
	}
	intVal := func(key string, def int) int {
		raw := get(key, "")
		if raw == "" {
			return def
		}
		n, err := strconv.Atoi(raw)
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s must be an integer, got %q", key, raw))
			return def
		}
		return n
	}
	msVal := func(key string, def time.Duration) time.Duration {
		raw := get(key, "")
		if raw == "" {
			return def
		}
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			problems = append(problems, fmt.Sprintf("%s must be a positive integer of milliseconds, got %q", key, raw))
			return def
		}
		return time.Duration(n) * time.Millisecond
	}
	boolVal := func(key string, def bool) bool {
		raw := get(key, "")
		if raw == "" {
			return def
		}
		b, err := strconv.ParseBool(raw)
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s must be a boolean, got %q", key, raw))
			return def
		}
		return b
	}
	listVal := func(key string, def []string) []string {
		raw := get(key, "")
		if raw == "" {
			return def
		}
		var out []string
		for _, part := range strings.Split(raw, ",") {
			if p := strings.TrimSpace(part); p != "" {
				out = append(out, p)
			}
		}
		return out
	}

	c := &Config{
		WorkerName:        get("WORKER_NAME", "anvilkit-export-worker"),
		WorkerConcurrency: intVal("WORKER_CONCURRENCY", 4),
		QueueDriver:       require("QUEUE_DRIVER"),
		RedisURL:          get("REDIS_URL", ""),
		KafkaBrokers:      get("KAFKA_BROKERS", ""),
		RenderOriginURL:   require("RENDER_ORIGIN_URL"),
		RenderTimeout:     msVal("RENDER_TIMEOUT_MS", 15*time.Second),
		UploadTimeout:     msVal("UPLOAD_TIMEOUT_MS", 60*time.Second),

		DeploymentServiceURL: require("DEPLOYMENT_SERVICE_URL"),
		AssetServiceURL:      require("ASSET_SERVICE_URL"),
		InternalServiceToken: require("INTERNAL_SERVICE_TOKEN"),

		ArtifactStorageDriver: require("ARTIFACT_STORAGE_DRIVER"),
		ArtifactBucket:        require("ARTIFACT_BUCKET"),
		ArtifactBasePrefix:    get("ARTIFACT_BASE_PREFIX", "sites"),
		S3Endpoint:            require("S3_ENDPOINT"),
		S3Region:              get("S3_REGION", "us-east-1"),
		S3AccessKey:           require("S3_ACCESS_KEY"),
		S3SecretKey:           require("S3_SECRET_KEY"),

		DependencyAllowlist:    listVal("DEPENDENCY_ALLOWLIST", []string{"/_next/static/*", "/assets/*", "/fonts/*", "/component-styles.css"}),
		ExternalAssetAllowlist: listVal("EXTERNAL_ASSET_ALLOWLIST", nil),

		HealthPort:   intVal("HEALTH_PORT", 8081),
		MetricsPort:  intVal("METRICS_PORT", 9091),
		LogLevel:     get("LOG_LEVEL", "info"),
		OTELEndpoint: get("OTEL_EXPORTER_OTLP_ENDPOINT", ""),

		Environment:  require("ENVIRONMENT"),
		WorkerDryRun: boolVal("WORKER_DRY_RUN", false),
	}

	switch c.Environment {
	case "", EnvLocal, EnvDevelopment, EnvStaging, EnvProduction:
	default:
		problems = append(problems, fmt.Sprintf(
			"ENVIRONMENT must be one of local|development|staging|production, got %q", c.Environment))
	}

	switch c.QueueDriver {
	case "":
	case "redis":
		if c.RedisURL == "" {
			problems = append(problems, "REDIS_URL is required when QUEUE_DRIVER=redis")
		}
	case "kafka":
		problems = append(problems, "QUEUE_DRIVER=kafka is the GA driver and is not implemented in the MVP (FR-021 seam)")
	default:
		problems = append(problems, fmt.Sprintf("QUEUE_DRIVER must be redis|kafka, got %q", c.QueueDriver))
	}

	if c.ArtifactStorageDriver != "" && c.ArtifactStorageDriver != "s3" {
		problems = append(problems, fmt.Sprintf("ARTIFACT_STORAGE_DRIVER must be s3, got %q", c.ArtifactStorageDriver))
	}
	if c.WorkerConcurrency < 1 || c.WorkerConcurrency > 64 {
		problems = append(problems, fmt.Sprintf("WORKER_CONCURRENCY must be in 1..64, got %d", c.WorkerConcurrency))
	}

	// Demo guard (FR-019, AC-011, ADR-010): RENDER_ORIGIN_URL must never point
	// at apps/demo outside local development — startup failure, not a warning.
	if c.RenderOriginURL != "" {
		if guardErr := demoGuard(c.RenderOriginURL, c.LocalDev()); guardErr != "" {
			problems = append(problems, guardErr)
		}
	}

	// Dry-run is a local-development scaffold mode only (same strictness).
	if c.WorkerDryRun && !c.LocalDev() {
		problems = append(problems, fmt.Sprintf(
			"WORKER_DRY_RUN is allowed only when ENVIRONMENT is local or development, got %q", c.Environment))
	}

	if len(problems) > 0 {
		return nil, fmt.Errorf("CONFIG_MISSING: invalid worker configuration:\n  - %s",
			strings.Join(problems, "\n  - "))
	}
	return c, nil
}

// demoGuard returns a non-empty problem description when target violates the
// demo guard. The apps/demo path check applies in every environment; the
// host denylist applies only in deployed (strict) environments.
func demoGuard(target string, localDev bool) string {
	u, err := url.Parse(target)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Sprintf("RENDER_ORIGIN_URL is not a valid absolute URL: %q", target)
	}
	segments := strings.Split(strings.Trim(u.Path, "/"), "/")
	for i := 0; i+1 < len(segments); i++ {
		if segments[i] == "apps" && segments[i+1] == "demo" {
			if localDev {
				return "" // permitted in local development only (AC-011)
			}
			return fmt.Sprintf("RENDER_ORIGIN_URL points at apps/demo (%q) — forbidden outside local development (FR-019, ADR-010)", target)
		}
	}
	if !localDev && demoHostDenylist[u.Hostname()] {
		return fmt.Sprintf("RENDER_ORIGIN_URL host %q is on the demo/loopback denylist — forbidden outside local development (ADR-010)", u.Hostname())
	}
	return ""
}

// FromEnv is the production entrypoint: Load over os.LookupEnv.
func FromEnv() (*Config, error) {
	return Load(os.LookupEnv)
}
