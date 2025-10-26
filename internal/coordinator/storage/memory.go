package storage

import (
	"sync"

	"github.com/google/uuid"

	"github.com/nemanja-m/gomr/internal/coordinator/core"
)

type InMemoryJobStore struct {
	mu   sync.RWMutex
	jobs map[string]*core.Job
}

func NewInMemoryJobStore() *InMemoryJobStore {
	return &InMemoryJobStore{
		jobs: make(map[string]*core.Job),
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

type InMemoryTaskStore struct {
	mu    sync.RWMutex
	tasks map[string]*core.Task
}

func NewInMemoryTaskStore() *InMemoryTaskStore {
	return &InMemoryTaskStore{
		tasks: make(map[string]*core.Task),
	}
}
func (s *InMemoryTaskStore) SaveTask(task *core.Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks[task.ID.String()] = task
	return nil
}

func (s *InMemoryTaskStore) UpdateTask(task *core.Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks[task.ID.String()] = task
	return nil
}

func (s *InMemoryTaskStore) GetTaskByID(id uuid.UUID) (*core.Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	task, exists := s.tasks[id.String()]
	if !exists {
		return nil, nil
	}
	return task, nil
}

func (s *InMemoryTaskStore) ListTasksByJobID(jobID uuid.UUID) ([]*core.Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	tasks := make([]*core.Task, 0)
	for _, task := range s.tasks {
		if task.JobID == jobID {
			tasks = append(tasks, task)
		}
	}
	return tasks, nil
}
