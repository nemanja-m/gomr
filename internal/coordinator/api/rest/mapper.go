package rest

import (
	"time"

	"github.com/google/uuid"

	"github.com/nemanja-m/gomr/internal/coordinator/core"
)

const (
	DefaultMapTimeoutSeconds    = 300
	DefaultReduceTimeoutSeconds = 600
	DefaultMaxMapAttempts       = 3
	DefaultMaxReduceAttempts    = 3
)

func (req *SubmitJobRequest) ToJob() *core.Job {
	return &core.Job{
		ID:     uuid.New(),
		Name:   req.Name,
		Status: core.JobStatusPending,

		Input: core.InputConfig{
			Type:   req.Input.Type,
			Format: req.Input.Format,
			Paths:  req.Input.Paths,
		},
		Executors: core.ExecutorsConfig{
			Map: core.ExecutorSpec{
				Type: req.Executors.Map.Type,
				URI:  req.Executors.Map.URI,
			},
			Reduce: core.ExecutorSpec{
				Type: req.Executors.Reduce.Type,
				URI:  req.Executors.Reduce.URI,
			},
			Combiner: func() *core.ExecutorSpec {
				if req.Executors.Combiner == nil {
					return nil
				}
				return &core.ExecutorSpec{
					Type: req.Executors.Combiner.Type,
					URI:  req.Executors.Combiner.URI,
				}
			}(),
		},
		Config: core.JobConfig{
			NumMappers:  req.Config.NumMappers,
			NumReducers: req.Config.NumReducers,
			MapTimeout: func() time.Duration {
				if req.Config.MapTimeoutSeconds != nil {
					return time.Duration(*req.Config.MapTimeoutSeconds) * time.Second
				}
				return DefaultMapTimeoutSeconds * time.Second
			}(),
			ReduceTimeout: func() time.Duration {
				if req.Config.ReduceTimeoutSeconds != nil {
					return time.Duration(*req.Config.ReduceTimeoutSeconds) * time.Second
				}
				return DefaultReduceTimeoutSeconds * time.Second
			}(),
			MaxMapAttempts: func() int {
				if req.Config.MaxMapAttempts != nil {
					return *req.Config.MaxMapAttempts
				}
				return DefaultMaxMapAttempts
			}(),
			MaxReduceAttempts: func() int {
				if req.Config.MaxReduceAttempts != nil {
					return *req.Config.MaxReduceAttempts
				}
				return DefaultMaxReduceAttempts
			}(),
		},
		Metadata: req.Metadata,

		SubmittedAt: time.Now(),
		Errors:      []core.JobError{},
	}
}

func ToGetJobResponse(job *core.Job) GetJobResponse {
	errors := make([]ErrorInfo, 0, len(job.Errors))
	for _, e := range job.Errors {
		errors = append(errors, ErrorInfo{
			TaskID:    e.TaskID.String(),
			Attempt:   e.Attempt,
			Error:     e.Error,
			Timestamp: e.Timestamp,
		})
	}

	return GetJobResponse{
		JobID:  job.ID.String(),
		Name:   job.Name,
		Status: string(job.Status),
		Progress: ProgressInfo{
			Map: TaskProgress{
				Total:     job.Progress.Map.Total,
				Pending:   job.Progress.Map.Pending,
				Running:   job.Progress.Map.Running,
				Completed: job.Progress.Map.Completed,
				Failed:    job.Progress.Map.Failed,
			},
			Reduce: TaskProgress{
				Total:     job.Progress.Reduce.Total,
				Pending:   job.Progress.Reduce.Pending,
				Running:   job.Progress.Reduce.Running,
				Completed: job.Progress.Reduce.Completed,
				Failed:    job.Progress.Reduce.Failed,
			},
		},
		Timestamps: TimestampsInfo{
			Submitted: job.SubmittedAt,
			Started:   job.StartedAt,
			Completed: job.CompletedAt,
		},
		Output: OutputInfo{
			Location:  "",
			Available: false,
		},
		Errors: errors,
	}
}

func ToJobSummary(job *core.Job) JobSummary {
	return JobSummary{
		JobID:       job.ID.String(),
		Name:        job.Name,
		Status:      string(job.Status),
		SubmittedAt: job.SubmittedAt,
		CompletedAt: job.CompletedAt,
	}
}

func ToTaskInfo(task *core.Task) TaskInfo {
	return TaskInfo{
		TaskID:    task.ID.String(),
		Type:      string(task.Type),
		Status:    string(task.Status),
		Attempts:  task.Attempt,
		StartTime: task.StartedAt,
		EndTime:   task.EndedAt,
	}
}
