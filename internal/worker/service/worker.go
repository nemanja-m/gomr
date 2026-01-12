package service

import (
	"context"
	"time"

	"github.com/nemanja-m/gomr/internal/shared/logging"
	"github.com/nemanja-m/gomr/internal/worker/core"
)

type workerService struct {
	client            core.CoordinatorClient
	executor          core.TaskExecutor
	heartbeatInterval time.Duration
	logger            logging.Logger
}

func NewWorkerService(
	client core.CoordinatorClient,
	executor core.TaskExecutor,
	heartbeatInterval time.Duration,
	logger logging.Logger,
) core.WorkerService {
	return &workerService{
		client:            client,
		executor:          executor,
		heartbeatInterval: heartbeatInterval,
		logger:            logger,
	}
}

func (w *workerService) Run(ctx context.Context) error {
	go w.runHeartbeatLoop(ctx)
	go w.runTaskLoop(ctx)
	return nil
}

func (w *workerService) runHeartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(w.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.client.SendHeartbeat(ctx); err != nil {
				w.logger.Error("Failed to send heartbeat", "error", err)
			} else {
				w.logger.Debug("Heartbeat sent successfully")
			}
		}
	}
}

func (w *workerService) runTaskLoop(ctx context.Context) {
	const (
		minBackoff = 100 * time.Millisecond
		maxBackoff = 5 * time.Second
	)
	backoff := minBackoff

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		task, err := w.client.PullTask(ctx)
		if err != nil {
			w.logger.Error("Failed to pull task", "error", err)
			time.Sleep(backoff)
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		if task == nil {
			time.Sleep(backoff)
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		backoff = minBackoff

		w.logger.Info("Received task",
			"task_id", task.TaskId,
			"job_id", task.JobId,
			"type", task.Type.String(),
			"attempt", task.Attempt,
		)

		err = w.executor.Execute(ctx, task)

		if err == nil {
			w.logger.Info("Task completed", "task_id", task.TaskId)
			if reportErr := w.client.CompleteTask(ctx, task.TaskId); reportErr != nil {
				w.logger.Error("Failed to report task completion", "error", reportErr)
			}
		} else {
			w.logger.Error("Task execution failed", "task_id", task.TaskId, "error", err)
			if reportErr := w.client.FailTask(ctx, task.TaskId, err.Error()); reportErr != nil {
				w.logger.Error("Failed to report task failure", "error", reportErr)
			}
		}
	}
}
