package rest

import (
	"time"
)

type SubmitJobRequest struct {
	Name      string            `json:"name"`
	Input     InputConfig       `json:"input"`
	Executors ExecutorsConfig   `json:"executors"`
	Config    JobConfig         `json:"config"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

type InputConfig struct {
	Type   string   `json:"type"`   // "s3", "gcs", "local", etc.
	Paths  []string `json:"paths"`  // Glob patterns or specific paths
	Format string   `json:"format"` // "text", "json", "csv", "parquet", etc.
}

type ExecutorsConfig struct {
	Map      ExecutorSpec  `json:"map"`
	Reduce   ExecutorSpec  `json:"reduce"`
	Combiner *ExecutorSpec `json:"combiner,omitempty"`
}

type ExecutorSpec struct {
	Type string `json:"type"` // "docker", "lambda", etc.
	URI  string `json:"uri"`  // Image name, function ARN, etc.
}

type JobConfig struct {
	NumReducers          int            `json:"numReducers"`
	MaxMapAttempts       *int           `json:"maxMapAttempts,omitempty"`
	MaxReduceAttempts    *int           `json:"maxReduceAttempts,omitempty"`
	MapTimeoutSeconds    *int           `json:"mapTimeoutSeconds,omitempty"`
	ReduceTimeoutSeconds *int           `json:"reduceTimeoutSeconds,omitempty"`
	Runtime              *RuntimeConfig `json:"runtime,omitempty"`
}

type RuntimeConfig struct {
	DefaultType string         `json:"defaultType"`
	PullPolicy  string         `json:"pullPolicy"` // "Always", "IfNotPresent", "Never"
	Resources   ResourceConfig `json:"resources"`
}

type ResourceConfig struct {
	CPU    string `json:"cpu"`
	Memory string `json:"memory"`
}

type SubmitJobResponse struct {
	JobID                string    `json:"job_id"`
	Status               string    `json:"status"`
	SubmittedAt          time.Time `json:"submitted_at"`
	EstimatedMapTasks    int       `json:"estimated_map_tasks"`
	EstimatedReduceTasks int       `json:"estimated_reduce_tasks"`
	Links                Links     `json:"links"`
}

type Links struct {
	Self string `json:"self"`
}

type GetJobResponse struct {
	JobID      string         `json:"job_id"`
	Name       string         `json:"name"`
	Status     string         `json:"status"`
	Progress   ProgressInfo   `json:"progress"`
	Timestamps TimestampsInfo `json:"timestamps"`
	Output     OutputInfo     `json:"output"`
	Errors     []ErrorInfo    `json:"errors"`
}

type ProgressInfo struct {
	Map    TaskProgress `json:"map"`
	Reduce TaskProgress `json:"reduce"`
}

type TaskProgress struct {
	Total     int `json:"total"`
	Pending   int `json:"pending"`
	Running   int `json:"running"`
	Completed int `json:"completed"`
	Failed    int `json:"failed"`
}

type TimestampsInfo struct {
	Submitted time.Time  `json:"submitted"`
	Started   *time.Time `json:"started"`
	Completed *time.Time `json:"completed"`
}

type OutputInfo struct {
	Location  string `json:"location"`
	Available bool   `json:"available"`
}

type ErrorInfo struct {
	TaskID    string    `json:"task_id"`
	Attempt   int       `json:"attempt"`
	Error     string    `json:"error"`
	Timestamp time.Time `json:"timestamp"`
}

type ListJobsResponse struct {
	Jobs       []JobSummary `json:"jobs"`
	Total      int          `json:"total"`
	Limit      int          `json:"limit"`
	Offset     int          `json:"offset"`
	NextOffset *int         `json:"next_offset,omitempty"`
}

type JobSummary struct {
	JobID       string     `json:"job_id"`
	Name        string     `json:"name"`
	Status      string     `json:"status"`
	SubmittedAt time.Time  `json:"submitted_at"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
}

type GetTasksResponse struct {
	Tasks []TaskInfo `json:"tasks"`
}

type TaskInfo struct {
	TaskID    string     `json:"task_id"`
	Type      string     `json:"type"` // "MAP" or "REDUCE"
	Status    string     `json:"status"`
	Attempts  int        `json:"attempts"`
	StartTime *time.Time `json:"start_time,omitempty"`
	EndTime   *time.Time `json:"end_time,omitempty"`
}

type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message,omitempty"`
	Code    int    `json:"code"`
}
