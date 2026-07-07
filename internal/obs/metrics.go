package obs

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Metrics is the full anvilkit_export_worker_* baseline (PRD 0010 §15.2
// renamed per ADR-015; EW-OBS-002 final), including the §15.4 alert feeds
// (auth failures by code, unparseable events) and the storage-growth
// visibility counters (AC-032).
type Metrics struct {
	JobsTotal          prometheus.Counter
	JobsSuccessTotal   prometheus.Counter
	JobsFailedTotal    prometheus.Counter
	JobDurationMs      prometheus.Histogram
	RenderDurationMs   prometheus.Histogram
	HarvestDurationMs  prometheus.Histogram
	UploadDurationMs   prometheus.Histogram
	ArtifactBytesTotal prometheus.Counter
	ArtifactFilesTotal prometheus.Counter
	RetryTotal         prometheus.Counter
	DLQTotal           prometheus.Counter
	UnparseableTotal   prometheus.Counter
	AuthFailuresTotal  *prometheus.CounterVec
	LockConflictTotal  prometheus.Counter
	QueuePending       prometheus.Gauge
	RetryDispatchLagMs prometheus.Gauge
	StreamTrimmedTotal *prometheus.CounterVec
}

// NewMetrics registers the M2 metric set plus the standard Go runtime
// collectors on reg and returns the handles.
func NewMetrics(reg *prometheus.Registry) *Metrics {
	m := &Metrics{
		JobsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "anvilkit_export_worker_jobs_total",
			Help: "Jobs consumed (every execution, including retries and reclaims).",
		}),
		JobsSuccessTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "anvilkit_export_worker_jobs_success_total",
			Help: "Jobs completed successfully.",
		}),
		JobsFailedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "anvilkit_export_worker_jobs_failed_total",
			Help: "Jobs terminally failed (non-retryable or retry-exhausted).",
		}),
		JobDurationMs: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "anvilkit_export_worker_job_duration_ms",
			Help:    "End-to-end job execution latency in milliseconds (P95 target <= 20000).",
			Buckets: []float64{50, 100, 250, 500, 1000, 2500, 5000, 10000, 20000, 40000, 90000},
		}),
		RenderDurationMs: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "anvilkit_export_worker_render_duration_ms",
			Help:    "Render fetch latency in milliseconds (P95 target <= 5000).",
			Buckets: []float64{25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 15000},
		}),
		HarvestDurationMs: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "anvilkit_export_worker_dependency_harvest_duration_ms",
			Help:    "Dependency harvest latency in milliseconds.",
			Buckets: []float64{10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000},
		}),
		UploadDurationMs: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "anvilkit_export_worker_upload_duration_ms",
			Help:    "Artifact upload latency in milliseconds (P95 target <= 10000).",
			Buckets: []float64{25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 20000, 60000},
		}),
		ArtifactBytesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "anvilkit_export_worker_artifact_bytes_total",
			Help: "Total artifact bytes uploaded (storage-growth visibility, AC-032).",
		}),
		ArtifactFilesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "anvilkit_export_worker_artifact_files_total",
			Help: "Total artifact files uploaded.",
		}),
		UnparseableTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "anvilkit_export_worker_unparseable_events_total",
			Help: "Unparseable events routed to the DLQ (§15.4 alert feed).",
		}),
		AuthFailuresTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "anvilkit_export_worker_auth_failures_total",
			Help: "Per-service 401/403 classifications (token rotation/scope alert feed, §11.1).",
		}, []string{"code"}),
		RetryTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "anvilkit_export_worker_retry_total",
			Help: "Business retries scheduled (attempt increments; never pending reclaims).",
		}),
		DLQTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "anvilkit_export_worker_dlq_total",
			Help: "Messages routed to the DLQ (exhaustion or unparseable input).",
		}),
		LockConflictTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "anvilkit_export_worker_lock_conflict_total",
			Help: "Per-deployment lock acquisition conflicts (contention indicator).",
		}),
		QueuePending: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "anvilkit_export_worker_queue_pending",
			Help: "Pending (delivered, unacked) messages in the consumer group.",
		}),
		RetryDispatchLagMs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "anvilkit_export_worker_retry_dispatch_lag_ms",
			Help: "Age of the oldest due, undispatched retry envelope in milliseconds.",
		}),
		StreamTrimmedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "anvilkit_export_worker_stream_trimmed_total",
			Help: "Stream entries removed by retention trimming (ADR-011), by stream.",
		}, []string{"stream"}),
	}
	reg.MustRegister(
		m.JobsTotal, m.JobsSuccessTotal, m.JobsFailedTotal, m.JobDurationMs,
		m.RenderDurationMs, m.HarvestDurationMs, m.UploadDurationMs,
		m.ArtifactBytesTotal, m.ArtifactFilesTotal, m.UnparseableTotal, m.AuthFailuresTotal,
		m.RetryTotal, m.DLQTotal, m.LockConflictTotal, m.QueuePending, m.RetryDispatchLagMs,
		m.StreamTrimmedTotal,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}
