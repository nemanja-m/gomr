package core

import "github.com/google/uuid"

// TODO: Maybe rename to JobController
type JobOrchestrator interface {
	SubmitJob(job *Job) error
	GetJob(id uuid.UUID) (*Job, error)
	ListJobs() ([]*Job, error)

	GetTasks(jobID uuid.UUID) ([]*Task, error)
}

type jobOrchestrator struct {
	jobStore JobStore
}

func NewJobOrchestrator(jobStore JobStore) JobOrchestrator {
	return &jobOrchestrator{
		jobStore: jobStore,
	}
}

func (o *jobOrchestrator) SubmitJob(job *Job) error {
	return o.jobStore.SaveJob(job)
}

func (o *jobOrchestrator) GetJob(id uuid.UUID) (*Job, error) {
	return o.jobStore.GetJobByID(id)
}

func (o *jobOrchestrator) ListJobs() ([]*Job, error) {
	return o.jobStore.ListJobs()
}

func (o *jobOrchestrator) GetTasks(jobID uuid.UUID) ([]*Task, error) {
	return o.jobStore.ListTasksByJobID(jobID)
}
