package service

import (
	"time"

	"github.com/google/uuid"

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

func (s *workerService) RegisterWorker(worker *core.Worker) error {
	s.logger.Debug("Registering worker", "worker_id", worker.ID, "address", worker.Address)
	worker.Status = core.WorkerStatusActive
	worker.LastHeartbeatAt = time.Now()
	return s.workerStore.AddWorker(worker)
}

func (s *workerService) RecordHeartbeat(workerID uuid.UUID) error {
	return s.workerStore.UpdateWorkerHeartbeat(workerID, time.Now())
}

func (s *workerService) RemoveWorker(workerID uuid.UUID) error {
	return s.workerStore.RemoveWorker(workerID)
}

func (s *workerService) GetStaleWorkers(timeout time.Duration) ([]*core.Worker, error) {
	threshold := time.Now().Add(-timeout)
	return s.workerStore.GetStaleWorkers(threshold)
}
