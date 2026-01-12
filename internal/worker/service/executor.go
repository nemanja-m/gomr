package service

import (
	"context"

	"github.com/nemanja-m/gomr/internal/shared/proto"
	"github.com/nemanja-m/gomr/internal/worker/core"
)

type noopExecutor struct{}

func NewNoopExecutor() core.TaskExecutor {
	return &noopExecutor{}
}

func (e *noopExecutor) Execute(ctx context.Context, task *proto.TaskAssignment) error {
	// TODO: Implement actual task execution
	return nil
}
