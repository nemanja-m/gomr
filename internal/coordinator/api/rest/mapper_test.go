package rest

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/nemanja-m/gomr/internal/coordinator/core"
)

func TestSubmitJobRequestToJob(t *testing.T) {
	t.Run("basic conversion", func(t *testing.T) {
		req := SubmitJobRequest{
			Name: "test-job",
			Input: InputConfig{
				Type:   "s3",
				Paths:  []string{"s3://bucket/input/*.txt"},
				Format: "text",
			},
			Output: OutputConfig{
				Type: "s3",
				Path: "s3://bucket/output",
			},
			Executors: ExecutorsConfig{
				Map: ExecutorSpec{
					Type: "docker",
					URI:  "map:latest",
				},
				Reduce: ExecutorSpec{
					Type: "docker",
					URI:  "reduce:latest",
				},
			},
			Config: JobConfig{
				NumReducers: 5,
			},
			Metadata: map[string]string{
				"owner": "test-user",
			},
		}

		job := req.ToJob()

		if job.ID == uuid.Nil {
			t.Error("Expected job ID to be generated")
		}

		if job.Name != "test-job" {
			t.Errorf("Expected name test-job, got %s", job.Name)
		}

		if job.Status != core.JobStatusPending {
			t.Errorf("Expected status PENDING, got %s", job.Status)
		}

		if job.Input.Type != "s3" {
			t.Errorf("Expected input type s3, got %s", job.Input.Type)
		}

		if len(job.Input.Paths) != 1 || job.Input.Paths[0] != "s3://bucket/input/*.txt" {
			t.Errorf("Expected input paths to be preserved, got %v", job.Input.Paths)
		}

		if job.Output.Type != "s3" {
			t.Errorf("Expected output type s3, got %s", job.Output.Type)
		}

		if job.Output.Path != "s3://bucket/output" {
			t.Errorf("Expected output path s3://bucket/output, got %s", job.Output.Path)
		}

		if job.Executors.Map.Type != "docker" {
			t.Errorf("Expected map executor type docker, got %s", job.Executors.Map.Type)
		}

		if job.Config.NumReducers != 5 {
			t.Errorf("Expected 5 reducers, got %d", job.Config.NumReducers)
		}

		if job.Metadata["owner"] != "test-user" {
			t.Errorf("Expected metadata to be preserved")
		}

		if job.SubmittedAt.IsZero() {
			t.Error("Expected SubmittedAt to be set")
		}

		if job.Errors == nil {
			t.Error("Expected Errors to be initialized as empty slice")
		}

		if len(job.Errors) != 0 {
			t.Errorf("Expected Errors to be empty, got %d errors", len(job.Errors))
		}
	})

	t.Run("with combiner", func(t *testing.T) {
		combiner := ExecutorSpec{
			Type: "docker",
			URI:  "combiner:latest",
		}

		req := SubmitJobRequest{
			Name: "test-job",
			Input: InputConfig{
				Type:  "s3",
				Paths: []string{"s3://bucket/input/*.txt"},
			},
			Output: OutputConfig{
				Type: "s3",
				Path: "s3://bucket/output",
			},
			Executors: ExecutorsConfig{
				Map: ExecutorSpec{
					Type: "docker",
					URI:  "map:latest",
				},
				Reduce: ExecutorSpec{
					Type: "docker",
					URI:  "reduce:latest",
				},
				Combiner: &combiner,
			},
			Config: JobConfig{
				NumReducers: 5,
			},
		}

		job := req.ToJob()

		if job.Executors.Combiner == nil {
			t.Error("Expected combiner to be set")
		}

		if job.Executors.Combiner.Type != "docker" {
			t.Errorf("Expected combiner type docker, got %s", job.Executors.Combiner.Type)
		}
	})

	t.Run("without combiner", func(t *testing.T) {
		req := SubmitJobRequest{
			Name: "test-job",
			Input: InputConfig{
				Type:  "s3",
				Paths: []string{"s3://bucket/input/*.txt"},
			},
			Output: OutputConfig{
				Type: "s3",
				Path: "s3://bucket/output",
			},
			Executors: ExecutorsConfig{
				Map: ExecutorSpec{
					Type: "docker",
					URI:  "map:latest",
				},
				Reduce: ExecutorSpec{
					Type: "docker",
					URI:  "reduce:latest",
				},
			},
			Config: JobConfig{
				NumReducers: 5,
			},
		}

		job := req.ToJob()

		if job.Executors.Combiner != nil {
			t.Error("Expected combiner to be nil")
		}
	})

	t.Run("with custom timeouts", func(t *testing.T) {
		mapTimeout := 600
		reduceTimeout := 1200

		req := SubmitJobRequest{
			Name: "test-job",
			Input: InputConfig{
				Type:  "s3",
				Paths: []string{"s3://bucket/input/*.txt"},
			},
			Output: OutputConfig{
				Type: "s3",
				Path: "s3://bucket/output",
			},
			Executors: ExecutorsConfig{
				Map: ExecutorSpec{
					Type: "docker",
					URI:  "map:latest",
				},
				Reduce: ExecutorSpec{
					Type: "docker",
					URI:  "reduce:latest",
				},
			},
			Config: JobConfig{
				NumReducers:          5,
				MapTimeoutSeconds:    &mapTimeout,
				ReduceTimeoutSeconds: &reduceTimeout,
			},
		}

		job := req.ToJob()

		if job.Config.MapTimeout != 600*time.Second {
			t.Errorf("Expected map timeout 600s, got %v", job.Config.MapTimeout)
		}

		if job.Config.ReduceTimeout != 1200*time.Second {
			t.Errorf("Expected reduce timeout 1200s, got %v", job.Config.ReduceTimeout)
		}
	})

	t.Run("with default timeouts", func(t *testing.T) {
		req := SubmitJobRequest{
			Name: "test-job",
			Input: InputConfig{
				Type:  "s3",
				Paths: []string{"s3://bucket/input/*.txt"},
			},
			Output: OutputConfig{
				Type: "s3",
				Path: "s3://bucket/output",
			},
			Executors: ExecutorsConfig{
				Map: ExecutorSpec{
					Type: "docker",
					URI:  "map:latest",
				},
				Reduce: ExecutorSpec{
					Type: "docker",
					URI:  "reduce:latest",
				},
			},
			Config: JobConfig{
				NumReducers: 5,
			},
		}

		job := req.ToJob()

		if job.Config.MapTimeout != DefaultMapTimeoutSeconds*time.Second {
			t.Errorf("Expected default map timeout %ds, got %v", DefaultMapTimeoutSeconds, job.Config.MapTimeout)
		}

		if job.Config.ReduceTimeout != DefaultReduceTimeoutSeconds*time.Second {
			t.Errorf("Expected default reduce timeout %ds, got %v", DefaultReduceTimeoutSeconds, job.Config.ReduceTimeout)
		}
	})

	t.Run("with custom max attempts", func(t *testing.T) {
		maxMapAttempts := 5
		maxReduceAttempts := 7

		req := SubmitJobRequest{
			Name: "test-job",
			Input: InputConfig{
				Type:  "s3",
				Paths: []string{"s3://bucket/input/*.txt"},
			},
			Output: OutputConfig{
				Type: "s3",
				Path: "s3://bucket/output",
			},
			Executors: ExecutorsConfig{
				Map: ExecutorSpec{
					Type: "docker",
					URI:  "map:latest",
				},
				Reduce: ExecutorSpec{
					Type: "docker",
					URI:  "reduce:latest",
				},
			},
			Config: JobConfig{
				NumReducers:       5,
				MaxMapAttempts:    &maxMapAttempts,
				MaxReduceAttempts: &maxReduceAttempts,
			},
		}

		job := req.ToJob()

		if job.Config.MaxMapAttempts != 5 {
			t.Errorf("Expected max map attempts 5, got %d", job.Config.MaxMapAttempts)
		}

		if job.Config.MaxReduceAttempts != 7 {
			t.Errorf("Expected max reduce attempts 7, got %d", job.Config.MaxReduceAttempts)
		}
	})

	t.Run("with default max attempts", func(t *testing.T) {
		req := SubmitJobRequest{
			Name: "test-job",
			Input: InputConfig{
				Type:  "s3",
				Paths: []string{"s3://bucket/input/*.txt"},
			},
			Output: OutputConfig{
				Type: "s3",
				Path: "s3://bucket/output",
			},
			Executors: ExecutorsConfig{
				Map: ExecutorSpec{
					Type: "docker",
					URI:  "map:latest",
				},
				Reduce: ExecutorSpec{
					Type: "docker",
					URI:  "reduce:latest",
				},
			},
			Config: JobConfig{
				NumReducers: 5,
			},
		}

		job := req.ToJob()

		if job.Config.MaxMapAttempts != DefaultMaxMapAttempts {
			t.Errorf("Expected default max map attempts %d, got %d", DefaultMaxMapAttempts, job.Config.MaxMapAttempts)
		}

		if job.Config.MaxReduceAttempts != DefaultMaxReduceAttempts {
			t.Errorf("Expected default max reduce attempts %d, got %d", DefaultMaxReduceAttempts, job.Config.MaxReduceAttempts)
		}
	})
}

func TestToGetJobResponse(t *testing.T) {
	t.Run("basic conversion", func(t *testing.T) {
		jobID := uuid.New()
		submittedAt := time.Now()

		job := &core.Job{
			ID:     jobID,
			Name:   "test-job",
			Status: core.JobStatusRunning,
			Output: core.OutputConfig{
				Type: "local",
				Path: "file://test",
			},
			Progress: core.JobProgress{
				Map: core.TaskProgress{
					Total:     10,
					Pending:   2,
					Running:   3,
					Completed: 4,
					Failed:    1,
				},
				Reduce: core.TaskProgress{
					Total:     5,
					Pending:   1,
					Running:   2,
					Completed: 2,
					Failed:    0,
				},
			},
			SubmittedAt: submittedAt,
			Errors:      []core.JobError{},
		}

		resp := ToGetJobResponse(job)

		if resp.JobID != jobID.String() {
			t.Errorf("Expected job ID %s, got %s", jobID.String(), resp.JobID)
		}

		if resp.Name != "test-job" {
			t.Errorf("Expected name test-job, got %s", resp.Name)
		}

		if resp.Status != "RUNNING" {
			t.Errorf("Expected status RUNNING, got %s", resp.Status)
		}

		if resp.Progress.Map.Total != 10 {
			t.Errorf("Expected map total 10, got %d", resp.Progress.Map.Total)
		}

		if resp.Progress.Reduce.Completed != 2 {
			t.Errorf("Expected reduce completed 2, got %d", resp.Progress.Reduce.Completed)
		}

		if resp.Timestamps.Submitted != submittedAt {
			t.Error("Expected submitted timestamp to be preserved")
		}

		if resp.Output.Location != "file://test" {
			t.Errorf("Expected output location file://test, got %s", resp.Output.Location)
		}

		if resp.Output.Available {
			t.Error("Expected output to not be available")
		}

		if len(resp.Errors) != 0 {
			t.Errorf("Expected 0 errors, got %d", len(resp.Errors))
		}
	})

	t.Run("with timestamps", func(t *testing.T) {
		submittedAt := time.Now().Add(-10 * time.Minute)
		startedAt := time.Now().Add(-5 * time.Minute)
		completedAt := time.Now()

		job := &core.Job{
			ID:          uuid.New(),
			Name:        "test-job",
			Status:      core.JobStatusCompleted,
			SubmittedAt: submittedAt,
			StartedAt:   &startedAt,
			CompletedAt: &completedAt,
			Errors:      []core.JobError{},
		}

		resp := ToGetJobResponse(job)

		if resp.Timestamps.Submitted != submittedAt {
			t.Error("Expected submitted timestamp to be preserved")
		}

		if resp.Timestamps.Started == nil || *resp.Timestamps.Started != startedAt {
			t.Error("Expected started timestamp to be preserved")
		}

		if resp.Timestamps.Completed == nil || *resp.Timestamps.Completed != completedAt {
			t.Error("Expected completed timestamp to be preserved")
		}
	})

	t.Run("with errors", func(t *testing.T) {
		taskID := uuid.New()
		errorTime := time.Now()

		job := &core.Job{
			ID:          uuid.New(),
			Name:        "test-job",
			Status:      core.JobStatusFailed,
			SubmittedAt: time.Now(),
			Errors: []core.JobError{
				{
					TaskID:    taskID,
					Attempt:   2,
					Error:     "task failed due to timeout",
					Timestamp: errorTime,
				},
			},
		}

		resp := ToGetJobResponse(job)

		if len(resp.Errors) != 1 {
			t.Fatalf("Expected 1 error, got %d", len(resp.Errors))
		}

		if resp.Errors[0].TaskID != taskID.String() {
			t.Errorf("Expected task ID %s, got %s", taskID.String(), resp.Errors[0].TaskID)
		}

		if resp.Errors[0].Attempt != 2 {
			t.Errorf("Expected attempt 2, got %d", resp.Errors[0].Attempt)
		}

		if resp.Errors[0].Error != "task failed due to timeout" {
			t.Errorf("Expected error message to be preserved, got %s", resp.Errors[0].Error)
		}

		if resp.Errors[0].Timestamp != errorTime {
			t.Error("Expected error timestamp to be preserved")
		}
	})

	t.Run("with multiple errors", func(t *testing.T) {
		job := &core.Job{
			ID:          uuid.New(),
			Name:        "test-job",
			Status:      core.JobStatusFailed,
			SubmittedAt: time.Now(),
			Errors: []core.JobError{
				{
					TaskID:    uuid.New(),
					Attempt:   1,
					Error:     "error 1",
					Timestamp: time.Now(),
				},
				{
					TaskID:    uuid.New(),
					Attempt:   2,
					Error:     "error 2",
					Timestamp: time.Now(),
				},
				{
					TaskID:    uuid.New(),
					Attempt:   1,
					Error:     "error 3",
					Timestamp: time.Now(),
				},
			},
		}

		resp := ToGetJobResponse(job)

		if len(resp.Errors) != 3 {
			t.Fatalf("Expected 3 errors, got %d", len(resp.Errors))
		}
	})
}

func TestToJobSummary(t *testing.T) {
	t.Run("without completed timestamp", func(t *testing.T) {
		jobID := uuid.New()
		submittedAt := time.Now()

		job := &core.Job{
			ID:          jobID,
			Name:        "test-job",
			Status:      core.JobStatusRunning,
			SubmittedAt: submittedAt,
		}

		summary := ToJobSummary(job)

		if summary.JobID != jobID.String() {
			t.Errorf("Expected job ID %s, got %s", jobID.String(), summary.JobID)
		}

		if summary.Name != "test-job" {
			t.Errorf("Expected name test-job, got %s", summary.Name)
		}

		if summary.Status != "RUNNING" {
			t.Errorf("Expected status RUNNING, got %s", summary.Status)
		}

		if summary.SubmittedAt != submittedAt {
			t.Error("Expected submitted timestamp to be preserved")
		}

		if summary.CompletedAt != nil {
			t.Error("Expected completed timestamp to be nil")
		}
	})

	t.Run("with completed timestamp", func(t *testing.T) {
		jobID := uuid.New()
		submittedAt := time.Now().Add(-10 * time.Minute)
		completedAt := time.Now()

		job := &core.Job{
			ID:          jobID,
			Name:        "completed-job",
			Status:      core.JobStatusCompleted,
			SubmittedAt: submittedAt,
			CompletedAt: &completedAt,
		}

		summary := ToJobSummary(job)

		if summary.JobID != jobID.String() {
			t.Errorf("Expected job ID %s, got %s", jobID.String(), summary.JobID)
		}

		if summary.Status != "COMPLETED" {
			t.Errorf("Expected status COMPLETED, got %s", summary.Status)
		}

		if summary.CompletedAt == nil {
			t.Fatal("Expected completed timestamp to be set")
		}

		if *summary.CompletedAt != completedAt {
			t.Error("Expected completed timestamp to be preserved")
		}
	})

	t.Run("different job statuses", func(t *testing.T) {
		statuses := []core.JobStatus{
			core.JobStatusPending,
			core.JobStatusPlanning,
			core.JobStatusRunning,
			core.JobStatusCompleted,
			core.JobStatusFailed,
		}

		for _, status := range statuses {
			job := &core.Job{
				ID:          uuid.New(),
				Name:        "test-job",
				Status:      status,
				SubmittedAt: time.Now(),
			}

			summary := ToJobSummary(job)

			if summary.Status != string(status) {
				t.Errorf("Expected status %s, got %s", status, summary.Status)
			}
		}
	})
}

func TestToTaskInfo(t *testing.T) {
	t.Run("map task without timestamps", func(t *testing.T) {
		taskID := uuid.New()

		task := &core.Task{
			ID:      taskID,
			JobID:   uuid.New(),
			Type:    core.TaskTypeMap,
			Status:  core.TaskStatusPending,
			Attempt: 1,
		}

		info := ToTaskInfo(task)

		if info.TaskID != taskID.String() {
			t.Errorf("Expected task ID %s, got %s", taskID.String(), info.TaskID)
		}

		if info.Type != "MAP" {
			t.Errorf("Expected type MAP, got %s", info.Type)
		}

		if info.Status != "PENDING" {
			t.Errorf("Expected status PENDING, got %s", info.Status)
		}

		if info.Attempts != 1 {
			t.Errorf("Expected attempts 1, got %d", info.Attempts)
		}

		if info.StartTime != nil {
			t.Error("Expected start time to be nil")
		}

		if info.EndTime != nil {
			t.Error("Expected end time to be nil")
		}
	})

	t.Run("reduce task with timestamps", func(t *testing.T) {
		taskID := uuid.New()
		startTime := time.Now().Add(-5 * time.Minute)
		endTime := time.Now()

		task := &core.Task{
			ID:        taskID,
			JobID:     uuid.New(),
			Type:      core.TaskTypeReduce,
			Status:    core.TaskStatusCompleted,
			Attempt:   2,
			StartedAt: &startTime,
			EndedAt:   &endTime,
		}

		info := ToTaskInfo(task)

		if info.Type != "REDUCE" {
			t.Errorf("Expected type REDUCE, got %s", info.Type)
		}

		if info.Status != "COMPLETED" {
			t.Errorf("Expected status COMPLETED, got %s", info.Status)
		}

		if info.Attempts != 2 {
			t.Errorf("Expected attempts 2, got %d", info.Attempts)
		}

		if info.StartTime == nil || *info.StartTime != startTime {
			t.Error("Expected start time to be preserved")
		}

		if info.EndTime == nil || *info.EndTime != endTime {
			t.Error("Expected end time to be preserved")
		}
	})

	t.Run("different task statuses", func(t *testing.T) {
		statuses := []core.TaskStatus{
			core.TaskStatusPending,
			core.TaskStatusRunning,
			core.TaskStatusCompleted,
			core.TaskStatusFailed,
		}

		for _, status := range statuses {
			task := &core.Task{
				ID:      uuid.New(),
				JobID:   uuid.New(),
				Type:    core.TaskTypeMap,
				Status:  status,
				Attempt: 1,
			}

			info := ToTaskInfo(task)

			if info.Status != string(status) {
				t.Errorf("Expected status %s, got %s", status, info.Status)
			}
		}
	})

	t.Run("multiple attempts", func(t *testing.T) {
		task := &core.Task{
			ID:      uuid.New(),
			JobID:   uuid.New(),
			Type:    core.TaskTypeMap,
			Status:  core.TaskStatusRunning,
			Attempt: 3,
		}

		info := ToTaskInfo(task)

		if info.Attempts != 3 {
			t.Errorf("Expected attempts 3, got %d", info.Attempts)
		}
	})
}
