package storage

import (
	"sync"

	"github.com/google/uuid"

	"github.com/nemanja-m/gomr/internal/coordinator/core"
)

type InMemoryJobStore struct {
	mu    sync.RWMutex
	jobs  map[string]*core.Job
	tasks map[string][]*core.Task // jobID -> tasks
}

func NewInMemoryJobStore() *InMemoryJobStore {
	return &InMemoryJobStore{
		jobs:  make(map[string]*core.Job),
		tasks: make(map[string][]*core.Task),
	}
}

func (s *InMemoryJobStore) SaveJob(job *core.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[job.ID.String()] = job
	return nil
}

func (s *InMemoryJobStore) UpdateJob(job *core.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[job.ID.String()] = job
	return nil
}

func (s *InMemoryJobStore) GetJobByID(id uuid.UUID) (*core.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, exists := s.jobs[id.String()]
	if !exists {
		return nil, nil
	}
	return job, nil
}

func (s *InMemoryJobStore) ListJobs() ([]*core.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	jobs := make([]*core.Job, 0, len(s.jobs))
	for _, job := range s.jobs {
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func (s *InMemoryJobStore) SaveTask(task *core.Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks[task.JobID.String()] = append(s.tasks[task.JobID.String()], task)
	return nil
}

func (s *InMemoryJobStore) SaveTasks(tasks []*core.Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(tasks) == 0 {
		return nil
	}
	jobID := tasks[0].JobID.String()
	s.tasks[jobID] = append(s.tasks[jobID], tasks...)
	return nil
}

func (s *InMemoryJobStore) UpdateTask(task *core.Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tasks := s.tasks[task.JobID.String()]
	for i, t := range tasks {
		if t.ID == task.ID {
			tasks[i] = task
			break
		}
	}
	s.tasks[task.JobID.String()] = tasks
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

func (s *InMemoryJobStore) ListTasksByJobID(jobID uuid.UUID) ([]*core.Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tasks[jobID.String()], nil
}
