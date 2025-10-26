package core

import (
	"time"

	"github.com/google/uuid"
)

type JobStatus string

const (
	JobStatusPending   JobStatus = "PENDING"
	JobStatusPlanning  JobStatus = "PLANNING"
	JobStatusRunning   JobStatus = "RUNNING"
	JobStatusCompleted JobStatus = "COMPLETED"
	JobStatusFailed    JobStatus = "FAILED"
)

type Job struct {
	ID        uuid.UUID
	Name      string
	Status    JobStatus
	Progress  JobProgress
	Input     InputConfig
	Output    OutputConfig
	Executors ExecutorsConfig
	Config    JobConfig
	Metadata  map[string]string

	SubmittedAt time.Time
	StartedAt   *time.Time
	CompletedAt *time.Time

	Errors []JobError
}

type InputConfig struct {
	Type   string
	Format string
	Paths  []string
}

type OutputConfig struct {
	Type string
	Path string
}

type ExecutorsConfig struct {
	Map      ExecutorSpec
	Reduce   ExecutorSpec
	Combiner *ExecutorSpec
}

type ExecutorSpec struct {
	Type string
	URI  string
}

type JobConfig struct {
	NumReducers int

	MapTimeout    time.Duration
	ReduceTimeout time.Duration

	MaxMapAttempts    int
	MaxReduceAttempts int

	Runtime RuntimeConfig
}

type RuntimeConfig struct {
	DefaultType string
	PullPolicy  string
	Resources   ResourceConfig
}

type ResourceConfig struct {
	CPU    string
	Memory string
}

type JobProgress struct {
	Map    TaskProgress
	Reduce TaskProgress
}

type TaskProgress struct {
	Total     int
	Pending   int
	Running   int
	Completed int
	Failed    int
}

type JobError struct {
	TaskID    uuid.UUID
	Attempt   int
	Error     string
	Timestamp time.Time
}

type TaskType string

const (
	TaskTypeMap    TaskType = "MAP"
	TaskTypeReduce TaskType = "REDUCE"
)

type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "PENDING"
	TaskStatusRunning   TaskStatus = "RUNNING"
	TaskStatusCompleted TaskStatus = "COMPLETED"
	TaskStatusFailed    TaskStatus = "FAILED"
)

type Task struct {
	ID     uuid.UUID
	JobID  uuid.UUID
	Type   TaskType
	Status TaskStatus
	Input  InputConfig
	Output OutputConfig

	StartedAt *time.Time
	EndedAt   *time.Time

	Attempt int
	Error   *string
}

type JobFilter struct {
	Status *JobStatus
	Limit  int
	Offset int
}
