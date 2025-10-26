package core

import "github.com/google/uuid"

type JobStore interface {
	SaveJob(job *Job) error
	UpdateJob(job *Job) error
	GetJobByID(id uuid.UUID) (*Job, error)
	ListJobs() ([]*Job, error)

	SaveTask(task *Task) error
	SaveTasks(tasks []*Task) error
	UpdateTask(task *Task) error
	GetTaskByID(id uuid.UUID) (*Task, error)
	ListTasksByJobID(jobID uuid.UUID) ([]*Task, error)
}
