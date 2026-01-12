package service

import (
	"github.com/nemanja-m/gomr/internal/coordinator/core"
	"github.com/nemanja-m/gomr/internal/shared/logging"
)

type workerService struct {
	workerStore core.WorkerStore
	logger      logging.Logger
}

func NewWorkerService(workerStore core.WorkerStore, logger logging.Logger) core.WorkerService {
	return &workerService{
		workerStore: workerStore,
		logger:      logger,
	}
}

func (c *workerService) RegisterWorker(worker *core.Worker) error {
	return c.workerStore.AddWorker(worker)
}
