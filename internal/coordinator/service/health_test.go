package service

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/nemanja-m/gomr/internal/coordinator/core"
)

type mockWorkerServiceForHealth struct {
	mu            sync.Mutex
	staleWorkers  []*core.Worker
	removedIDs    []uuid.UUID
	staleErr      error
	removeErr     error
	getStaleCount int
}

func (m *mockWorkerServiceForHealth) RegisterWorker(worker *core.Worker) error {
	return nil
}

func (m *mockWorkerServiceForHealth) RecordHeartbeat(workerID uuid.UUID) error {
	return nil
}

func (m *mockWorkerServiceForHealth) RemoveWorker(workerID uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.removeErr != nil {
		return m.removeErr
	}
	m.removedIDs = append(m.removedIDs, workerID)
	return nil
}

func (m *mockWorkerServiceForHealth) GetStaleWorkers(timeout time.Duration) ([]*core.Worker, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getStaleCount++
	if m.staleErr != nil {
		return nil, m.staleErr
	}
	return m.staleWorkers, nil
}

func (m *mockWorkerServiceForHealth) getRemovedIDs() []uuid.UUID {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]uuid.UUID{}, m.removedIDs...)
}

func (m *mockWorkerServiceForHealth) getStaleCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.getStaleCount
}

type healthTestLogger struct {
	mu       sync.Mutex
	messages []string
}

func (l *healthTestLogger) Debug(msg string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.messages = append(l.messages, msg)
}

func (l *healthTestLogger) Info(msg string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.messages = append(l.messages, msg)
}

func (l *healthTestLogger) Warn(msg string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.messages = append(l.messages, msg)
}

func (l *healthTestLogger) Error(msg string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.messages = append(l.messages, msg)
}

func (l *healthTestLogger) Fatal(msg string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.messages = append(l.messages, msg)
}

func (l *healthTestLogger) getMessages() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string{}, l.messages...)
}

// mockJobServiceForHealth is a mock for JobService used in health checker tests
type mockJobServiceForHealth struct {
	mu          sync.Mutex
	requeuedIDs []uuid.UUID
	requeueErr  error
}

func (m *mockJobServiceForHealth) SubmitJob(job *core.Job) error {
	return nil
}

func (m *mockJobServiceForHealth) GetJob(id uuid.UUID) (*core.Job, error) {
	return nil, nil
}

func (m *mockJobServiceForHealth) GetJobs(filter core.JobFilter) ([]*core.Job, int, error) {
	return nil, 0, nil
}

func (m *mockJobServiceForHealth) GetTasks(jobID uuid.UUID) ([]*core.Task, error) {
	return nil, nil
}

func (m *mockJobServiceForHealth) AssignTask(workerID uuid.UUID) (*core.Task, error) {
	return nil, nil
}

func (m *mockJobServiceForHealth) CompleteTask(taskID uuid.UUID, workerID uuid.UUID) error {
	return nil
}

func (m *mockJobServiceForHealth) FailTask(taskID uuid.UUID, workerID uuid.UUID, errMsg string) error {
	return nil
}

func (m *mockJobServiceForHealth) RequeueWorkerTasks(workerID uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.requeueErr != nil {
		return m.requeueErr
	}
	m.requeuedIDs = append(m.requeuedIDs, workerID)
	return nil
}

func TestWorkerHealthChecker_RemovesStaleWorkers(t *testing.T) {
	worker1 := &core.Worker{ID: uuid.New(), Address: "worker1:5000"}
	worker2 := &core.Worker{ID: uuid.New(), Address: "worker2:5000"}

	mockWorkerService := &mockWorkerServiceForHealth{
		staleWorkers: []*core.Worker{worker1, worker2},
	}
	mockJobService := &mockJobServiceForHealth{}
	logger := &healthTestLogger{}

	checker := NewWorkerHealthChecker(10*time.Millisecond, 15*time.Second, mockWorkerService, mockJobService, logger)

	ctx, cancel := context.WithCancel(context.Background())
	go checker.Start(ctx)

	// Wait for at least one check cycle
	time.Sleep(50 * time.Millisecond)
	cancel()

	removedIDs := mockWorkerService.getRemovedIDs()
	if len(removedIDs) < 2 {
		t.Errorf("expected at least 2 workers removed, got %d", len(removedIDs))
	}

	// Verify both workers were removed
	foundWorker1 := false
	foundWorker2 := false
	for _, id := range removedIDs {
		if id == worker1.ID {
			foundWorker1 = true
		}
		if id == worker2.ID {
			foundWorker2 = true
		}
	}
	if !foundWorker1 || !foundWorker2 {
		t.Error("not all stale workers were removed")
	}
}

func TestWorkerHealthChecker_StopsOnContextCancel(t *testing.T) {
	mockWorkerService := &mockWorkerServiceForHealth{}
	mockJobService := &mockJobServiceForHealth{}
	logger := &healthTestLogger{}

	checker := NewWorkerHealthChecker(5*time.Millisecond, 15*time.Second, mockWorkerService, mockJobService, logger)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		checker.Start(ctx)
		close(done)
	}()

	// Let it run a few cycles
	time.Sleep(20 * time.Millisecond)
	cancel()

	// Should stop within reasonable time
	select {
	case <-done:
		// Success
	case <-time.After(100 * time.Millisecond):
		t.Error("health checker did not stop after context cancellation")
	}
}

func TestWorkerHealthChecker_RunsAtConfiguredInterval(t *testing.T) {
	mockWorkerService := &mockWorkerServiceForHealth{}
	mockJobService := &mockJobServiceForHealth{}
	logger := &healthTestLogger{}

	checkInterval := 20 * time.Millisecond
	checker := NewWorkerHealthChecker(checkInterval, 15*time.Second, mockWorkerService, mockJobService, logger)

	ctx, cancel := context.WithCancel(context.Background())
	go checker.Start(ctx)

	// Wait for approximately 3 intervals
	time.Sleep(70 * time.Millisecond)
	cancel()

	callCount := mockWorkerService.getStaleCallCount()
	// Should have been called 2-4 times (accounting for timing variability)
	if callCount < 2 || callCount > 5 {
		t.Errorf("expected 2-5 calls to GetStaleWorkers, got %d", callCount)
	}
}

func TestWorkerHealthChecker_NoStaleWorkers(t *testing.T) {
	mockWorkerService := &mockWorkerServiceForHealth{
		staleWorkers: []*core.Worker{}, // No stale workers
	}
	mockJobService := &mockJobServiceForHealth{}
	logger := &healthTestLogger{}

	checker := NewWorkerHealthChecker(10*time.Millisecond, 15*time.Second, mockWorkerService, mockJobService, logger)

	ctx, cancel := context.WithCancel(context.Background())
	go checker.Start(ctx)

	time.Sleep(30 * time.Millisecond)
	cancel()

	removedIDs := mockWorkerService.getRemovedIDs()
	if len(removedIDs) != 0 {
		t.Errorf("expected no workers removed, got %d", len(removedIDs))
	}
}

func TestWorkerHealthChecker_LogsOnRemoval(t *testing.T) {
	worker := &core.Worker{ID: uuid.New(), Address: "worker:5000"}

	mockWorkerService := &mockWorkerServiceForHealth{
		staleWorkers: []*core.Worker{worker},
	}
	mockJobService := &mockJobServiceForHealth{}
	logger := &healthTestLogger{}

	checker := NewWorkerHealthChecker(10*time.Millisecond, 15*time.Second, mockWorkerService, mockJobService, logger)

	ctx, cancel := context.WithCancel(context.Background())
	go checker.Start(ctx)

	time.Sleep(30 * time.Millisecond)
	cancel()

	messages := logger.getMessages()
	found := slices.Contains(messages, "Removing stale worker")
	if !found {
		t.Error("expected 'Removing stale worker' log message")
	}
}
