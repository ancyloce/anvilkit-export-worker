package worker

import (
	"context"

	"github.com/ancyloce/anvilkit-export-worker/contracts/deploymentservice"
	"github.com/ancyloce/anvilkit-export-worker/contracts/events"
	"github.com/ancyloce/anvilkit-export-worker/internal/lock"
)

// DeploymentAPI is the processor's view of the deployment-service wrapper
// (satisfied by *deployment.Client; fakes in tests).
type DeploymentAPI interface {
	Load(ctx context.Context, deploymentID string) (*deploymentservice.DeploymentRecord, error)
	Transition(ctx context.Context, deploymentID string,
		from, to deploymentservice.DeploymentStatus, reason, traceID string,
		stage events.FailedStage) error
}

// Lease is one held per-deployment lock.
type Lease interface {
	Release(ctx context.Context) error
	Lost() <-chan struct{}
}

// FailedEmitter emits deployment.export.failed after terminal failures
// (FR-013); satisfied by *emit.Emitter.
type FailedEmitter interface {
	EmitFailed(ctx context.Context, ev events.ExportFailed) error
}

// ReadyRedeliverer implements the FR-015 redelivery-after-ARTIFACT_READY
// path: verify the stored manifest exists and re-emit
// deployment.artifact.ready from stored state (found=false when no manifest
// exists). Satisfied by *export.Pipeline.
type ReadyRedeliverer interface {
	ReemitReady(ctx context.Context, rec *deploymentservice.DeploymentRecord, traceID string) (found bool, err error)
}

// Locks acquires per-deployment locks; Acquire returns lock.ErrConflict when
// another worker holds the deployment.
type Locks interface {
	Acquire(ctx context.Context, deploymentID string) (Lease, error)
}

// LocksFrom adapts the Redis locker to the Locks seam.
func LocksFrom(l *lock.Locker) Locks { return redisLocks{l} }

type redisLocks struct{ l *lock.Locker }

func (r redisLocks) Acquire(ctx context.Context, deploymentID string) (Lease, error) {
	lease, err := r.l.Acquire(ctx, deploymentID)
	if err != nil {
		return nil, err
	}
	return lease, nil
}
