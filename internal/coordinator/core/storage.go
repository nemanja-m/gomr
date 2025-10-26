package core

import "github.com/google/uuid"

type JobStore interface {
	SaveJob(job *Job, tasks ...*Task) error
	UpdateJob(job *Job, tasks ...*Task) error
	GetJobByID(id uuid.UUID) (*Job, error)
	GetJobs(filter JobFilter) ([]*Job, int, error)

	UpdateTask(task *Task) error
	GetTaskByID(id uuid.UUID) (*Task, error)
	GetTasksByJobID(jobID uuid.UUID) ([]*Task, error)

	IsMapPhaseCompleted(jobID uuid.UUID) (bool, error)
}
