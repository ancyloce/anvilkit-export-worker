package worker

import (
	"context"
	"errors"
	"log/slog"

	"github.com/ancyloce/anvilkit-export-worker/contracts/deploymentservice"
	"github.com/ancyloce/anvilkit-export-worker/contracts/events"
	"github.com/ancyloce/anvilkit-export-worker/internal/queue"
)

// Job is the validated, locked, EXPORTING-transitioned unit of work handed
// to the export pipeline.
type Job struct {
	Event   *events.ExportRequested
	Record  *deploymentservice.DeploymentRecord
	Msg     queue.Message
	TraceID string
	Log     *slog.Logger
}

// Exporter is the pipeline seam the processor drives after the EXPORTING
// transition. The M3 implementation performs render → harvest → upload →
// manifest → pointer submission → CAS ARTIFACT_READY → emit ready
// (EW-RENDER-*, EW-ARTIFACT-*, EW-STORAGE-*, EW-EVENT-*). Returning nil
// means the deployment reached ARTIFACT_READY and the message may be acked.
type Exporter interface {
	Export(ctx context.Context, job *Job) error
}

// ErrPipelineUnimplemented marks the M2 scaffold state. main refuses to
// start a non-dry-run worker while the default exporter is Unimplemented, so
// this error is unreachable in a correctly configured M2 deployment.
var ErrPipelineUnimplemented = errors.New("export pipeline not implemented until Milestone 3 (PLAN-0001 §5)")

// Unimplemented is the M2 placeholder exporter.
type Unimplemented struct{}

func (Unimplemented) Export(context.Context, *Job) error {
	return ErrPipelineUnimplemented
}
