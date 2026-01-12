package service

import (
	"context"
	"time"

	"github.com/nemanja-m/gomr/internal/coordinator/core"
	"github.com/nemanja-m/gomr/internal/shared/logging"
)

type WorkerHealthChecker struct {
	checkInterval time.Duration
	staleTimeout  time.Duration
	workerService core.WorkerService
	jobService    core.JobService
	logger        logging.Logger
}

func NewWorkerHealthChecker(
	checkInterval time.Duration,
	staleTimeout time.Duration,
	workerService core.WorkerService,
	jobService core.JobService,
	logger logging.Logger,
) *WorkerHealthChecker {
	return &WorkerHealthChecker{
		checkInterval: checkInterval,
		staleTimeout:  staleTimeout,
		workerService: workerService,
		jobService:    jobService,
		logger:        logger,
	}
}

func (h *WorkerHealthChecker) Start(ctx context.Context) {
	ticker := time.NewTicker(h.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.removeStaleWorkers()
		}
	}
}

func (h *WorkerHealthChecker) removeStaleWorkers() {
	staleWorkers, err := h.workerService.GetStaleWorkers(h.staleTimeout)
	if err != nil {
		h.logger.Error("Failed to get stale workers", "error", err)
		return
	}
	for _, worker := range staleWorkers {
		h.logger.Info("Removing stale worker", "worker_id", worker.ID)

		if err := h.jobService.RequeueWorkerTasks(worker.ID); err != nil {
			h.logger.Error("Failed to requeue worker tasks", "worker_id", worker.ID, "error", err)
		}

		if err := h.workerService.RemoveWorker(worker.ID); err != nil {
			h.logger.Error("Failed to remove stale worker", "worker_id", worker.ID, "error", err)
		}
	}
}
