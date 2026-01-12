package storage

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/nemanja-m/gomr/internal/coordinator/core"
)

// InMemoryJobStore is an in-memory implementation of JobStore for testing and development purposes.
type InMemoryJobStore struct {
	mu    sync.RWMutex
	jobs  map[uuid.UUID]*core.Job
	tasks map[uuid.UUID][]*core.Task
}

func NewInMemoryJobStore() *InMemoryJobStore {
	return &InMemoryJobStore{
		jobs:  make(map[uuid.UUID]*core.Job),
		tasks: make(map[uuid.UUID][]*core.Task),
	}
}

func (s *InMemoryJobStore) SaveJob(job *core.Job, tasks ...*core.Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[job.ID] = job
	if len(tasks) > 0 {
		s.tasks[job.ID] = append(s.tasks[job.ID], tasks...)
	}
	return nil
}

func (s *InMemoryJobStore) UpdateJob(job *core.Job, tasks ...*core.Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[job.ID] = job
	if len(tasks) > 0 {
		s.tasks[job.ID] = append(s.tasks[job.ID], tasks...)
	}
	return nil
}

func (s *InMemoryJobStore) GetJobByID(id uuid.UUID) (*core.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, exists := s.jobs[id]
	if !exists {
		return nil, nil
	}
	return job, nil
}

func (s *InMemoryJobStore) GetJobs(filter core.JobFilter) ([]*core.Job, int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var filteredJobs []*core.Job
	for _, job := range s.jobs {
		if filter.Status != nil && job.Status != *filter.Status {
			continue
		}
		filteredJobs = append(filteredJobs, job)
	}

	total := len(filteredJobs)
	start := min(filter.Offset, total)
	end := min(start+filter.Limit, total)
	pagedJobs := filteredJobs[start:end]

	return pagedJobs, total, nil
}

func (s *InMemoryJobStore) UpdateTask(task *core.Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tasks := s.tasks[task.JobID]
	for i, t := range tasks {
		if t.ID == task.ID {
			tasks[i] = task
			break
		}
	}
	s.tasks[task.JobID] = tasks
	return nil
}

func (s *InMemoryJobStore) GetTaskByID(id uuid.UUID) (*core.Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, tasks := range s.tasks {
		for _, task := range tasks {
			if task.ID == id {
				return task, nil
			}
		}
	}
	return nil, nil
}

func (s *InMemoryJobStore) GetTasksByJobID(jobID uuid.UUID) ([]*core.Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tasks[jobID], nil
}

func (s *InMemoryJobStore) IsMapPhaseCompleted(jobID uuid.UUID) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	tasks := s.tasks[jobID]
	for _, task := range tasks {
		if task.Type == core.TaskTypeMap && task.Status != core.TaskStatusCompleted {
			return false, nil
		}
	}
	return true, nil
}

func (s *InMemoryJobStore) GetRunningTasksByWorkerID(workerID uuid.UUID) ([]*core.Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*core.Task
	for _, tasks := range s.tasks {
		for _, task := range tasks {
			if task.WorkerID != nil && *task.WorkerID == workerID && task.Status == core.TaskStatusRunning {
				result = append(result, task)
			}
		}
	}
	return result, nil
}

// InMemoryWorkerStore is an in-memory implementation of WorkerStore for testing and development purposes.
type InMemoryWorkerStore struct {
	mu      sync.RWMutex
	workers map[uuid.UUID]*core.Worker
}

func NewInMemoryWorkerStore() *InMemoryWorkerStore {
	return &InMemoryWorkerStore{
		workers: make(map[uuid.UUID]*core.Worker),
	}
}

func (s *InMemoryWorkerStore) AddWorker(worker *core.Worker) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.workers[worker.ID] = worker
	return nil
}

func (s *InMemoryWorkerStore) GetWorkerByID(id uuid.UUID) (*core.Worker, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	worker, exists := s.workers[id]
	if !exists {
		return nil, nil
	}
	return worker, nil
}

func (s *InMemoryWorkerStore) GetAllWorkers() ([]*core.Worker, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var allWorkers []*core.Worker
	for _, worker := range s.workers {
		allWorkers = append(allWorkers, worker)
	}
	return allWorkers, nil
}

func (s *InMemoryWorkerStore) UpdateWorkerHeartbeat(id uuid.UUID, timestamp time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	worker, exists := s.workers[id]
	if !exists {
		return fmt.Errorf("worker not found: %s", id)
	}
	worker.LastHeartbeatAt = timestamp
	return nil
}

func (s *InMemoryWorkerStore) RemoveWorker(id uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.workers, id)
	return nil
}

func (s *InMemoryWorkerStore) GetStaleWorkers(threshold time.Time) ([]*core.Worker, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var stale []*core.Worker
	for _, worker := range s.workers {
		if worker.LastHeartbeatAt.Before(threshold) {
			stale = append(stale, worker)
		}
	}
	return stale, nil
}
