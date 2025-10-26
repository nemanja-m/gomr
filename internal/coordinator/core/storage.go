package core

import "github.com/google/uuid"

type JobStore interface {
	SaveJob(job *Job) error
	UpdateJob(job *Job) error
	GetJobByID(id uuid.UUID) (*Job, error)
	ListJobs() ([]*Job, error)
}

type TaskStore interface {
	SaveTask(task *Task) error
	UpdateTask(task *Task) error
	GetTaskByID(id uuid.UUID) (*Task, error)
	ListTasksByJobID(jobID uuid.UUID) ([]*Task, error)
}
