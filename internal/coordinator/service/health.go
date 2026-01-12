package service

import (
	"context"
	"time"

	"github.com/nemanja-m/gomr/internal/coordinator/core"
	"github.com/nemanja-m/gomr/internal/shared/logging"
)

type WorkerHealthChecker struct {
	workerService core.WorkerService
	checkInterval time.Duration
	staleTimeout  time.Duration
	logger        logging.Logger
}

func NewWorkerHealthChecker(
	workerService core.WorkerService,
	checkInterval time.Duration,
	staleTimeout time.Duration,
	logger logging.Logger,
) *WorkerHealthChecker {
	return &WorkerHealthChecker{
		workerService: workerService,
		checkInterval: checkInterval,
		staleTimeout:  staleTimeout,
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
		if err := h.workerService.RemoveWorker(worker.ID); err != nil {
			h.logger.Error("Failed to remove stale worker", "worker_id", worker.ID, "error", err)
		}
	}
}
