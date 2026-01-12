package core

import (
	"context"
	"time"

	"github.com/nemanja-m/gomr/internal/shared/proto"
)

type CoordinatorClient interface {
	RegisterWorker(ctx context.Context, addr string, cpuCores uint32, memoryBytes uint64) (time.Duration, error)
	SendHeartbeat(ctx context.Context) error
	PullTask(ctx context.Context) (*proto.TaskAssignment, error)
	CompleteTask(ctx context.Context, taskID string) error
	FailTask(ctx context.Context, taskID string, errMsg string) error
	Close() error
}

type WorkerService interface {
	Run(ctx context.Context) error
}

type TaskExecutor interface {
	Execute(ctx context.Context, task *proto.TaskAssignment) error
}
