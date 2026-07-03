package obs

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Worker lifecycle states (PRD 0010 §6.3 WorkerLifecycle) — reflected via
// the readiness probe, never persisted.
const (
	StateStarting int32 = iota
	StateReady
	StateDraining
	StateStopped
)

// Lifecycle is the worker's probe-visible state.
type Lifecycle struct {
	state atomic.Int32
}

func (l *Lifecycle) Set(state int32) { l.state.Store(state) }

func (l *Lifecycle) Get() int32 { return l.state.Load() }

// Ready reports whether the readiness probe should pass.
func (l *Lifecycle) Ready() bool { return l.state.Load() == StateReady }

func stateName(s int32) string {
	switch s {
	case StateStarting:
		return "STARTING"
	case StateReady:
		return "READY"
	case StateDraining:
		return "DRAINING"
	default:
		return "STOPPED"
	}
}

// OpsServer hosts the internal-only operational endpoints (FR-018,
// EW-OBS-004): liveness + readiness on the health port (8081) and Prometheus
// exposition on the metrics port (9091). No public ports; no auth (probes are
// cluster-internal by network policy).
type OpsServer struct {
	health  *http.Server
	metrics *http.Server
}

// NewOpsServer builds both servers. Call Start to begin serving.
func NewOpsServer(healthPort, metricsPort int, lc *Lifecycle, g prometheus.Gatherer) *OpsServer {
	healthMux := http.NewServeMux()
	healthMux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	healthMux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if lc.Ready() {
			_, _ = w.Write([]byte(`{"status":"ok"}`))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintf(w, `{"status":%q}`, stateName(lc.Get()))
	})

	metricsMux := http.NewServeMux()
	metricsMux.Handle("GET /metrics", promhttp.HandlerFor(g, promhttp.HandlerOpts{}))

	return &OpsServer{
		health:  &http.Server{Addr: fmt.Sprintf(":%d", healthPort), Handler: healthMux, ReadHeaderTimeout: 5 * time.Second},
		metrics: &http.Server{Addr: fmt.Sprintf(":%d", metricsPort), Handler: metricsMux, ReadHeaderTimeout: 5 * time.Second},
	}
}

// Start serves both listeners; server errors (other than clean shutdown) are
// reported on the returned channel.
func (s *OpsServer) Start() <-chan error {
	errs := make(chan error, 2)
	go func() {
		if err := s.health.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errs <- fmt.Errorf("health server: %w", err)
		}
	}()
	go func() {
		if err := s.metrics.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errs <- fmt.Errorf("metrics server: %w", err)
		}
	}()
	return errs
}

// Shutdown drains both servers.
func (s *OpsServer) Shutdown(ctx context.Context) error {
	healthErr := s.health.Shutdown(ctx)
	metricsErr := s.metrics.Shutdown(ctx)
	if healthErr != nil {
		return healthErr
	}
	return metricsErr
}

// Handlers exposes the health mux for in-process tests.
func (s *OpsServer) Handlers() (health http.Handler, metrics http.Handler) {
	return s.health.Handler, s.metrics.Handler
}
