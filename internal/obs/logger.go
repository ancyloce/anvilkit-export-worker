package obs

import (
	"context"
	"io"
	"log/slog"
	"strings"
)

// NewLogger builds the worker's structured JSON logger (FR-020, EW-OBS-001).
// Every record carries workerId; any string attribute, error, or message
// containing one of the secrets is redacted (EW-CONFIG-005, §11.1: the token
// appears in no logs, traces, or error messages).
func NewLogger(w io.Writer, level string, workerID string, secrets []string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	var filtered []string
	for _, s := range secrets {
		if s != "" {
			filtered = append(filtered, s)
		}
	}
	handler := slog.Handler(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: lvl}))
	handler = &redactingHandler{inner: handler, secrets: filtered}
	return slog.New(handler).With(slog.String("workerId", workerID))
}

// redactingHandler replaces secret material in message and string attribute
// values with [REDACTED] before the record is emitted.
type redactingHandler struct {
	inner   slog.Handler
	secrets []string
}

func (h *redactingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *redactingHandler) Handle(ctx context.Context, r slog.Record) error {
	clean := slog.NewRecord(r.Time, r.Level, h.redact(r.Message), r.PC)
	r.Attrs(func(a slog.Attr) bool {
		clean.AddAttrs(h.redactAttr(a))
		return true
	})
	return h.inner.Handle(ctx, clean)
}

func (h *redactingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	clean := make([]slog.Attr, 0, len(attrs))
	for _, a := range attrs {
		clean = append(clean, h.redactAttr(a))
	}
	return &redactingHandler{inner: h.inner.WithAttrs(clean), secrets: h.secrets}
}

func (h *redactingHandler) WithGroup(name string) slog.Handler {
	return &redactingHandler{inner: h.inner.WithGroup(name), secrets: h.secrets}
}

func (h *redactingHandler) redactAttr(a slog.Attr) slog.Attr {
	switch a.Value.Kind() {
	case slog.KindString:
		return slog.String(a.Key, h.redact(a.Value.String()))
	case slog.KindGroup:
		attrs := a.Value.Group()
		clean := make([]any, 0, len(attrs))
		for _, ga := range attrs {
			clean = append(clean, h.redactAttr(ga))
		}
		return slog.Group(a.Key, clean...)
	case slog.KindAny:
		// Errors commonly smuggle response bodies; stringify and redact.
		if err, ok := a.Value.Any().(error); ok {
			return slog.String(a.Key, h.redact(err.Error()))
		}
		return a
	default:
		return a
	}
}

func (h *redactingHandler) redact(s string) string {
	for _, secret := range h.secrets {
		s = strings.ReplaceAll(s, secret, "[REDACTED]")
	}
	return s
}

// JobFields is the required job-scoped log field set (PRD 0010 §15.1).
// status, durationMs, and errorCode are attached at the completion log site;
// everything else is constant for the job and belongs on the job logger.
type JobFields struct {
	TraceID      string
	EventID      string
	DeploymentID string
	TeamID       string
	SiteID       string
	PageID       string
	Slug         string
	Version      string
	Environment  string
	RenderMode   string
	Attempt      int
}

// JobLogger derives a logger carrying every constant job-scoped field.
func JobLogger(base *slog.Logger, f JobFields) *slog.Logger {
	return base.With(
		slog.String("traceId", f.TraceID),
		slog.String("eventId", f.EventID),
		slog.String("deploymentId", f.DeploymentID),
		slog.String("teamId", f.TeamID),
		slog.String("siteId", f.SiteID),
		slog.String("pageId", f.PageID),
		slog.String("slug", f.Slug),
		slog.String("version", f.Version),
		slog.String("environment", f.Environment),
		slog.String("renderMode", f.RenderMode),
		slog.Int("attempt", f.Attempt),
	)
}
