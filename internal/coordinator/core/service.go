package core

import (
	"time"

	"github.com/google/uuid"
)

// JobService defines the interface for job orchestration and management
type JobService interface {
	SubmitJob(job *Job) error
	GetJob(id uuid.UUID) (*Job, error)
	GetJobs(filter JobFilter) ([]*Job, int, error)
	GetTasks(jobID uuid.UUID) ([]*Task, error)
	NextTask() (*Task, error)
}

// WorkerService defines the interface for worker management
type WorkerService interface {
	RegisterWorker(worker *Worker) error
	RecordHeartbeat(workerID uuid.UUID) error
	RemoveWorker(workerID uuid.UUID) error
	GetStaleWorkers(timeout time.Duration) ([]*Worker, error)
}
