package outbound

import (
	"context"

	"github.com/google/uuid"
)

// RevertJobSubmitter submits and cancels revert jobs on the workflow engine.
// Implemented by the DBOS adapter; keeps engine details out of the core.
type RevertJobSubmitter interface {
	Submit(ctx context.Context, jobID uuid.UUID) error
	Cancel(ctx context.Context, jobID uuid.UUID) error
}
