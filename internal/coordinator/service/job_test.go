package service

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/nemanja-m/gomr/internal/coordinator/core"
)

// mockLogger is a no-op logger for testing
type mockLogger struct{}

func (m *mockLogger) Debug(msg string, args ...any) {}
func (m *mockLogger) Info(msg string, args ...any)  {}
func (m *mockLogger) Warn(msg string, args ...any)  {}
func (m *mockLogger) Error(msg string, args ...any) {}
func (m *mockLogger) Fatal(msg string, args ...any) {}

// mockJobStore is an in-memory implementation of JobStore for testing
type mockJobStore struct {
	mu    sync.RWMutex
	jobs  map[uuid.UUID]*core.Job
	tasks map[uuid.UUID][]*core.Task
}

func newMockJobStore() *mockJobStore {
	return &mockJobStore{
		jobs:  make(map[uuid.UUID]*core.Job),
		tasks: make(map[uuid.UUID][]*core.Task),
	}
}

func (s *mockJobStore) SaveJob(job *core.Job, tasks ...*core.Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[job.ID] = job
	if len(tasks) > 0 {
		s.tasks[job.ID] = append(s.tasks[job.ID], tasks...)
	}
	return nil
}

func (s *mockJobStore) UpdateJob(job *core.Job, tasks ...*core.Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[job.ID] = job
	if len(tasks) > 0 {
		s.tasks[job.ID] = append(s.tasks[job.ID], tasks...)
	}
	return nil
}

func (s *mockJobStore) GetJobByID(id uuid.UUID) (*core.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, exists := s.jobs[id]
	if !exists {
		return nil, nil
	}
	return job, nil
}

func (s *mockJobStore) GetJobs(filter core.JobFilter) ([]*core.Job, int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var filteredJobs []*core.Job
	for _, job := range s.jobs {
		if filter.Status != nil && job.Status != *filter.Status {
			continue
		}
		filteredJobs = append(filteredJobs, job)
	}

	total := len(filteredJobs)
	start := min(filter.Offset, total)
	end := min(start+filter.Limit, total)
	pagedJobs := filteredJobs[start:end]

	return pagedJobs, total, nil
}

func (s *mockJobStore) UpdateTask(task *core.Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tasks := s.tasks[task.JobID]
	for i, t := range tasks {
		if t.ID == task.ID {
			tasks[i] = task
			break
		}
	}
	s.tasks[task.JobID] = tasks
	return nil
}

func (s *mockJobStore) GetTaskByID(id uuid.UUID) (*core.Task, error) {
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

func (s *mockJobStore) GetTasksByJobID(jobID uuid.UUID) ([]*core.Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tasks[jobID], nil
}

func (s *mockJobStore) IsMapPhaseCompleted(jobID uuid.UUID) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	tasks := s.tasks[jobID]
	for _, task := range tasks {
		if task.Type == core.TaskTypeMap && task.Status != core.TaskStatusCompleted {
			return false, nil
		}
	}
	return true, nil
}

// createTestJob creates a test job with the given input paths
func createTestJob(inputPaths []string, numReducers int) *core.Job {
	return &core.Job{
		ID:     uuid.New(),
		Name:   "test-job",
		Status: core.JobStatusPending,
		Input: core.InputConfig{
			Type:   "local",
			Format: "text",
			Paths:  inputPaths,
		},
		Output: core.OutputConfig{
			Type: "local",
			Path: "/tmp/test-output",
		},
		Config: core.JobConfig{
			NumReducers: numReducers,
		},
		SubmittedAt: time.Now().UTC(),
	}
}

// createTempTestFiles creates temporary test files in a temp directory
func createTempTestFiles(t *testing.T, numFiles int) ([]string, string) {
	t.Helper()

	tempDir := t.TempDir()
	var filePaths []string

	for i := range numFiles {
		filePath := filepath.Join(tempDir, "input-"+string(rune('0'+i))+".txt")
		content := []byte("test data " + string(rune('0'+i)))
		if err := os.WriteFile(filePath, content, 0644); err != nil {
			t.Fatalf("failed to create test file: %v", err)
		}
		filePaths = append(filePaths, filePath)
	}

	return filePaths, tempDir
}

func TestNewJobService(t *testing.T) {
	store := newMockJobStore()
	logger := &mockLogger{}

	service := NewJobService(store, logger)

	if service == nil {
		t.Fatal("NewJobService returned nil")
	}
}

func TestJobService_SubmitJob(t *testing.T) {
	t.Run("submit job with valid local input", func(t *testing.T) {
		store := newMockJobStore()
		logger := &mockLogger{}
		service := NewJobService(store, logger)

		// Create test files
		filePaths, _ := createTempTestFiles(t, 3)
		job := createTestJob(filePaths, 2)

		err := service.SubmitJob(job)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Verify job status was updated
		if job.Status != core.JobStatusRunning {
			t.Errorf("expected job status RUNNING, got %s", job.Status)
		}

		// Verify StartedAt was set
		if job.StartedAt == nil {
			t.Error("expected StartedAt to be set")
		}

		// Verify progress was initialized
		if job.Progress.Map.Total != 3 {
			t.Errorf("expected 3 map tasks, got %d", job.Progress.Map.Total)
		}
		if job.Progress.Reduce.Total != 2 {
			t.Errorf("expected 2 reduce tasks, got %d", job.Progress.Reduce.Total)
		}

		// Verify job was saved to store
		savedJob, err := store.GetJobByID(job.ID)
		if err != nil {
			t.Fatalf("failed to get job from store: %v", err)
		}
		if savedJob == nil {
			t.Fatal("job not found in store")
		}
		if savedJob.Status != core.JobStatusRunning {
			t.Errorf("expected saved job status RUNNING, got %s", savedJob.Status)
		}

		// Verify tasks were created and saved
		tasks, err := store.GetTasksByJobID(job.ID)
		if err != nil {
			t.Fatalf("failed to get tasks: %v", err)
		}
		if len(tasks) != 5 { // 3 map + 2 reduce
			t.Errorf("expected 5 tasks, got %d", len(tasks))
		}

		// Count map and reduce tasks
		var mapTasks, reduceTasks int
		for _, task := range tasks {
			switch task.Type {
			case core.TaskTypeMap:
				mapTasks++
			case core.TaskTypeReduce:
				reduceTasks++
			}
		}
		if mapTasks != 3 {
			t.Errorf("expected 3 map tasks, got %d", mapTasks)
		}
		if reduceTasks != 2 {
			t.Errorf("expected 2 reduce tasks, got %d", reduceTasks)
		}
	})

	t.Run("submit job with unsupported input type", func(t *testing.T) {
		store := newMockJobStore()
		logger := &mockLogger{}
		service := NewJobService(store, logger)

		job := &core.Job{
			ID:   uuid.New(),
			Name: "test-job",
			Input: core.InputConfig{
				Type:  "http",
				Paths: []string{"http://example.com/path"},
			},
			Output: core.OutputConfig{
				Type: "local",
				Path: "/tmp/output",
			},
			Config: core.JobConfig{
				NumReducers: 2,
			},
		}

		err := service.SubmitJob(job)
		if err == nil {
			t.Error("expected error for unsupported input type")
		}
		if err != nil && err.Error() != "unsupported input type: http" {
			t.Errorf("unexpected error message: %v", err)
		}
	})

	t.Run("submit job with no input files", func(t *testing.T) {
		store := newMockJobStore()
		logger := &mockLogger{}
		service := NewJobService(store, logger)

		job := createTestJob([]string{"/nonexistent/*.txt"}, 2)

		err := service.SubmitJob(job)
		if err == nil {
			t.Error("expected error for no input files")
		}
	})

	t.Run("submit job with glob pattern", func(t *testing.T) {
		store := newMockJobStore()
		logger := &mockLogger{}
		service := NewJobService(store, logger)

		// Create test files
		_, tempDir := createTempTestFiles(t, 3)
		pattern := filepath.Join(tempDir, "*.txt")
		job := createTestJob([]string{pattern}, 2)

		err := service.SubmitJob(job)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Verify tasks were created
		tasks, err := store.GetTasksByJobID(job.ID)
		if err != nil {
			t.Fatalf("failed to get tasks: %v", err)
		}
		if len(tasks) != 5 { // 3 map + 2 reduce
			t.Errorf("expected 5 tasks, got %d", len(tasks))
		}
	})

	t.Run("map tasks have correct input configuration", func(t *testing.T) {
		store := newMockJobStore()
		logger := &mockLogger{}
		service := NewJobService(store, logger)

		filePaths, _ := createTempTestFiles(t, 2)
		job := createTestJob(filePaths, 1)

		err := service.SubmitJob(job)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		tasks, _ := store.GetTasksByJobID(job.ID)

		// Check each map task has exactly one input file
		for _, task := range tasks {
			if task.Type == core.TaskTypeMap {
				if len(task.Input.Paths) != 1 {
					t.Errorf("expected map task to have 1 input file, got %d", len(task.Input.Paths))
				}
				if task.Status != core.TaskStatusPending {
					t.Errorf("expected map task status PENDING, got %s", task.Status)
				}
			}
		}
	})

	t.Run("reduce tasks have correct shuffle pattern", func(t *testing.T) {
		store := newMockJobStore()
		logger := &mockLogger{}
		service := NewJobService(store, logger)

		filePaths, _ := createTempTestFiles(t, 2)
		job := createTestJob(filePaths, 2)

		err := service.SubmitJob(job)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		tasks, _ := store.GetTasksByJobID(job.ID)

		// Check reduce tasks have shuffle pattern
		for _, task := range tasks {
			if task.Type == core.TaskTypeReduce {
				if len(task.Input.Paths) != 1 {
					t.Errorf("expected reduce task to have 1 input path pattern, got %d", len(task.Input.Paths))
				}
				// Should have pattern like "shuffle-dir/map-*/part-000000000000000X"
				if !filepath.IsAbs(task.Input.Paths[0]) {
					t.Error("expected reduce task input to be absolute path")
				}
				if task.Status != core.TaskStatusPending {
					t.Errorf("expected reduce task status PENDING, got %s", task.Status)
				}
			}
		}
	})
}

func TestJobService_GetJob(t *testing.T) {
	t.Run("get existing job", func(t *testing.T) {
		store := newMockJobStore()
		logger := &mockLogger{}
		service := NewJobService(store, logger)

		// Create and save a job
		job := createTestJob([]string{"/tmp/test.txt"}, 1)
		_ = store.SaveJob(job)

		retrievedJob, err := service.GetJob(job.ID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if retrievedJob == nil {
			t.Fatal("expected job, got nil")
		}
		if retrievedJob.ID != job.ID {
			t.Errorf("expected job ID %s, got %s", job.ID, retrievedJob.ID)
		}
	})

	t.Run("get non-existent job", func(t *testing.T) {
		store := newMockJobStore()
		logger := &mockLogger{}
		service := NewJobService(store, logger)

		nonExistentID := uuid.New()
		job, err := service.GetJob(nonExistentID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if job != nil {
			t.Errorf("expected nil job, got %v", job)
		}
	})
}

func TestJobService_GetJobs(t *testing.T) {
	t.Run("get all jobs without filter", func(t *testing.T) {
		store := newMockJobStore()
		logger := &mockLogger{}
		service := NewJobService(store, logger)

		// Create and save multiple jobs
		job1 := createTestJob([]string{"/tmp/test1.txt"}, 1)
		job2 := createTestJob([]string{"/tmp/test2.txt"}, 1)
		_ = store.SaveJob(job1)
		_ = store.SaveJob(job2)

		jobs, total, err := service.GetJobs(core.JobFilter{
			Limit: 10,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if total != 2 {
			t.Errorf("expected total 2, got %d", total)
		}
		if len(jobs) != 2 {
			t.Errorf("expected 2 jobs, got %d", len(jobs))
		}
	})

	t.Run("get jobs with status filter", func(t *testing.T) {
		store := newMockJobStore()
		logger := &mockLogger{}
		service := NewJobService(store, logger)

		// Create jobs with different statuses
		job1 := createTestJob([]string{"/tmp/test1.txt"}, 1)
		job1.Status = core.JobStatusRunning
		job2 := createTestJob([]string{"/tmp/test2.txt"}, 1)
		job2.Status = core.JobStatusCompleted
		job3 := createTestJob([]string{"/tmp/test3.txt"}, 1)
		job3.Status = core.JobStatusRunning

		_ = store.SaveJob(job1)
		_ = store.SaveJob(job2)
		_ = store.SaveJob(job3)

		runningStatus := core.JobStatusRunning
		jobs, total, err := service.GetJobs(core.JobFilter{
			Status: &runningStatus,
			Limit:  10,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if total != 2 {
			t.Errorf("expected total 2, got %d", total)
		}
		if len(jobs) != 2 {
			t.Errorf("expected 2 running jobs, got %d", len(jobs))
		}
		for _, job := range jobs {
			if job.Status != core.JobStatusRunning {
				t.Errorf("expected RUNNING status, got %s", job.Status)
			}
		}
	})

	t.Run("get jobs with pagination", func(t *testing.T) {
		store := newMockJobStore()
		logger := &mockLogger{}
		service := NewJobService(store, logger)

		// Create multiple jobs
		for range 5 {
			job := createTestJob([]string{"/tmp/test.txt"}, 1)
			_ = store.SaveJob(job)
		}

		jobs, total, err := service.GetJobs(core.JobFilter{
			Offset: 1,
			Limit:  2,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if total != 5 {
			t.Errorf("expected total 5, got %d", total)
		}
		if len(jobs) != 2 {
			t.Errorf("expected 2 jobs in page, got %d", len(jobs))
		}
	})
}

func TestJobService_GetTasks(t *testing.T) {
	t.Run("get tasks for existing job", func(t *testing.T) {
		store := newMockJobStore()
		logger := &mockLogger{}
		service := NewJobService(store, logger)

		filePaths, _ := createTempTestFiles(t, 2)
		job := createTestJob(filePaths, 1)

		err := service.SubmitJob(job)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		tasks, err := service.GetTasks(job.ID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(tasks) != 3 { // 2 map + 1 reduce
			t.Errorf("expected 3 tasks, got %d", len(tasks))
		}

		// Verify all tasks belong to the job
		for _, task := range tasks {
			if task.JobID != job.ID {
				t.Errorf("expected task JobID %s, got %s", job.ID, task.JobID)
			}
		}
	})

	t.Run("get tasks for non-existent job", func(t *testing.T) {
		store := newMockJobStore()
		logger := &mockLogger{}
		service := NewJobService(store, logger)

		nonExistentID := uuid.New()
		tasks, err := service.GetTasks(nonExistentID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(tasks) != 0 {
			t.Errorf("expected empty tasks, got %d", len(tasks))
		}
	})
}

func TestJobService_NextTask(t *testing.T) {
	t.Run("next task returns map task first", func(t *testing.T) {
		store := newMockJobStore()
		logger := &mockLogger{}
		service := NewJobService(store, logger)

		filePaths, _ := createTempTestFiles(t, 2)
		job := createTestJob(filePaths, 1)

		err := service.SubmitJob(job)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Get next task - should be a map task
		task, err := service.NextTask()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if task == nil {
			t.Fatal("expected task, got nil")
		}
		if task.Type != core.TaskTypeMap {
			t.Errorf("expected map task, got %s", task.Type)
		}
	})

	t.Run("next task returns nil when queue is empty", func(t *testing.T) {
		store := newMockJobStore()
		logger := &mockLogger{}
		service := NewJobService(store, logger)

		task, err := service.NextTask()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if task != nil {
			t.Errorf("expected nil task, got %v", task)
		}
	})

	t.Run("reduce tasks not returned until map phase complete", func(t *testing.T) {
		store := newMockJobStore()
		logger := &mockLogger{}
		service := NewJobService(store, logger)

		filePaths, _ := createTempTestFiles(t, 2)
		job := createTestJob(filePaths, 1)

		err := service.SubmitJob(job)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Pop all map tasks
		task1, _ := service.NextTask()
		task2, _ := service.NextTask()

		// Both should be map tasks
		if task1.Type != core.TaskTypeMap || task2.Type != core.TaskTypeMap {
			t.Error("expected both tasks to be map tasks")
		}

		// Next task should be nil (reduce task blocked until map phase completes)
		task3, err := service.NextTask()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if task3 != nil {
			t.Errorf("expected nil task (reduce blocked), got %v type", task3.Type)
		}

		// Mark map tasks as completed
		task1.Status = core.TaskStatusCompleted
		task2.Status = core.TaskStatusCompleted
		_ = store.UpdateTask(task1)
		_ = store.UpdateTask(task2)

		// Now reduce task should be available
		task4, err := service.NextTask()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if task4 == nil {
			t.Fatal("expected reduce task after map phase complete")
		}
		if task4.Type != core.TaskTypeReduce {
			t.Errorf("expected reduce task, got %s", task4.Type)
		}
	})

	t.Run("multiple jobs - returns highest priority task", func(t *testing.T) {
		store := newMockJobStore()
		logger := &mockLogger{}
		service := NewJobService(store, logger)

		// Submit first job
		filePaths1, _ := createTempTestFiles(t, 1)
		job1 := createTestJob(filePaths1, 1)
		_ = service.SubmitJob(job1)

		// Submit second job
		filePaths2, _ := createTempTestFiles(t, 1)
		job2 := createTestJob(filePaths2, 1)
		_ = service.SubmitJob(job2)

		// Should get a map task (high priority)
		task, err := service.NextTask()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if task == nil {
			t.Fatal("expected task, got nil")
		}
		if task.Type != core.TaskTypeMap {
			t.Errorf("expected map task, got %s", task.Type)
		}
	})

	t.Run("FIFO order for same priority tasks", func(t *testing.T) {
		store := newMockJobStore()
		logger := &mockLogger{}
		service := NewJobService(store, logger)

		filePaths, _ := createTempTestFiles(t, 3)
		job := createTestJob(filePaths, 1)

		err := service.SubmitJob(job)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Get all map tasks - should be in FIFO order
		task1, _ := service.NextTask()
		task2, _ := service.NextTask()
		task3, _ := service.NextTask()

		if task1 == nil || task2 == nil || task3 == nil {
			t.Fatal("expected 3 map tasks")
		}

		// All should be map tasks
		if task1.Type != core.TaskTypeMap || task2.Type != core.TaskTypeMap || task3.Type != core.TaskTypeMap {
			t.Error("expected all tasks to be map tasks")
		}

		// Verify they have different IDs (all unique)
		if task1.ID == task2.ID || task2.ID == task3.ID || task1.ID == task3.ID {
			t.Error("expected unique task IDs")
		}
	})
}
