package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nemanja-m/gomr/internal/shared/proto"
)

type mockCoordinatorClient struct {
	mu sync.Mutex

	heartbeatCount int
	heartbeatErr   error

	tasks       []*proto.TaskAssignment
	taskIndex   int
	pullTaskErr error

	completedTasks []string
	completeErr    error

	failedTasks []string
	failErr     error
}

func (m *mockCoordinatorClient) RegisterWorker(ctx context.Context, addr string, cpuCores uint32, memoryBytes uint64) (time.Duration, error) {
	return 1 * time.Second, nil
}

func (m *mockCoordinatorClient) SendHeartbeat(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.heartbeatCount++
	return m.heartbeatErr
}

func (m *mockCoordinatorClient) PullTask(ctx context.Context) (*proto.TaskAssignment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pullTaskErr != nil {
		return nil, m.pullTaskErr
	}
	if m.taskIndex >= len(m.tasks) {
		return nil, nil
	}
	task := m.tasks[m.taskIndex]
	m.taskIndex++
	return task, nil
}

func (m *mockCoordinatorClient) CompleteTask(ctx context.Context, taskID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.completedTasks = append(m.completedTasks, taskID)
	return m.completeErr
}

func (m *mockCoordinatorClient) FailTask(ctx context.Context, taskID string, errMsg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failedTasks = append(m.failedTasks, taskID)
	return m.failErr
}

func (m *mockCoordinatorClient) Close() error {
	return nil
}

type mockExecutor struct {
	mu            sync.Mutex
	executedTasks []string
	execErr       error
}

func (m *mockExecutor) Execute(ctx context.Context, task *proto.TaskAssignment) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.executedTasks = append(m.executedTasks, task.TaskId)
	return m.execErr
}

type mockLogger struct{}

func (m *mockLogger) Debug(msg string, args ...any) {}
func (m *mockLogger) Info(msg string, args ...any)  {}
func (m *mockLogger) Warn(msg string, args ...any)  {}
func (m *mockLogger) Error(msg string, args ...any) {}
func (m *mockLogger) Fatal(msg string, args ...any) {}

func TestWorkerService_Run_StartsHeartbeatAndTaskLoops(t *testing.T) {
	client := &mockCoordinatorClient{}
	executor := &mockExecutor{}
	logger := &mockLogger{}

	svc := NewWorkerService(client, executor, 50*time.Millisecond, logger)

	ctx, cancel := context.WithCancel(context.Background())

	go svc.Run(ctx)

	// Wait for at least one heartbeat
	time.Sleep(100 * time.Millisecond)
	cancel()

	client.mu.Lock()
	defer client.mu.Unlock()

	if client.heartbeatCount == 0 {
		t.Error("Expected at least one heartbeat to be sent")
	}
}

func TestWorkerService_HeartbeatLoop_SendsHeartbeats(t *testing.T) {
	client := &mockCoordinatorClient{}
	executor := &mockExecutor{}
	logger := &mockLogger{}

	svc := NewWorkerService(client, executor, 20*time.Millisecond, logger)

	ctx, cancel := context.WithCancel(context.Background())

	go svc.Run(ctx)

	time.Sleep(70 * time.Millisecond)
	cancel()

	client.mu.Lock()
	defer client.mu.Unlock()

	// With 20ms interval and 70ms wait, expect at least 2-3 heartbeats
	if client.heartbeatCount < 2 {
		t.Errorf("Expected at least 2 heartbeats, got %d", client.heartbeatCount)
	}
}

func TestWorkerService_HeartbeatLoop_HandlesErrors(t *testing.T) {
	client := &mockCoordinatorClient{
		heartbeatErr: errors.New("connection failed"),
	}
	executor := &mockExecutor{}
	logger := &mockLogger{}

	svc := NewWorkerService(client, executor, 20*time.Millisecond, logger)

	ctx, cancel := context.WithCancel(context.Background())

	go svc.Run(ctx)

	time.Sleep(50 * time.Millisecond)
	cancel()

	client.mu.Lock()
	defer client.mu.Unlock()

	// Should still attempt heartbeats even with errors
	if client.heartbeatCount == 0 {
		t.Error("Expected heartbeat attempts even with errors")
	}
}

func TestWorkerService_TaskLoop_ExecutesTask(t *testing.T) {
	task := &proto.TaskAssignment{
		TaskId: "task-1",
		JobId:  "job-1",
		Type:   proto.TaskType_MAP,
	}

	client := &mockCoordinatorClient{
		tasks: []*proto.TaskAssignment{task},
	}
	executor := &mockExecutor{}
	logger := &mockLogger{}

	svc := NewWorkerService(client, executor, 1*time.Second, logger)

	ctx, cancel := context.WithCancel(context.Background())

	go svc.Run(ctx)

	time.Sleep(50 * time.Millisecond)
	cancel()

	executor.mu.Lock()
	defer executor.mu.Unlock()

	if len(executor.executedTasks) != 1 || executor.executedTasks[0] != "task-1" {
		t.Errorf("Expected task-1 to be executed, got %v", executor.executedTasks)
	}

	client.mu.Lock()
	defer client.mu.Unlock()

	if len(client.completedTasks) != 1 || client.completedTasks[0] != "task-1" {
		t.Errorf("Expected task-1 to be completed, got %v", client.completedTasks)
	}
}

func TestWorkerService_TaskLoop_ReportsFailure(t *testing.T) {
	task := &proto.TaskAssignment{
		TaskId: "task-1",
		JobId:  "job-1",
		Type:   proto.TaskType_MAP,
	}

	client := &mockCoordinatorClient{
		tasks: []*proto.TaskAssignment{task},
	}
	executor := &mockExecutor{
		execErr: errors.New("execution failed"),
	}
	logger := &mockLogger{}

	svc := NewWorkerService(client, executor, 1*time.Second, logger)

	ctx, cancel := context.WithCancel(context.Background())

	go svc.Run(ctx)

	time.Sleep(50 * time.Millisecond)
	cancel()

	client.mu.Lock()
	defer client.mu.Unlock()

	if len(client.failedTasks) != 1 || client.failedTasks[0] != "task-1" {
		t.Errorf("Expected task-1 to be failed, got %v", client.failedTasks)
	}

	if len(client.completedTasks) != 0 {
		t.Errorf("Expected no completed tasks, got %v", client.completedTasks)
	}
}

func TestWorkerService_TaskLoop_ExecutesMultipleTasks(t *testing.T) {
	tasks := []*proto.TaskAssignment{
		{TaskId: "task-1", JobId: "job-1", Type: proto.TaskType_MAP},
		{TaskId: "task-2", JobId: "job-1", Type: proto.TaskType_MAP},
		{TaskId: "task-3", JobId: "job-1", Type: proto.TaskType_REDUCE},
	}

	client := &mockCoordinatorClient{
		tasks: tasks,
	}
	executor := &mockExecutor{}
	logger := &mockLogger{}

	svc := NewWorkerService(client, executor, 1*time.Second, logger)

	ctx, cancel := context.WithCancel(context.Background())

	go svc.Run(ctx)

	time.Sleep(100 * time.Millisecond)
	cancel()

	executor.mu.Lock()
	defer executor.mu.Unlock()

	if len(executor.executedTasks) != 3 {
		t.Errorf("Expected 3 tasks executed, got %d", len(executor.executedTasks))
	}

	client.mu.Lock()
	defer client.mu.Unlock()

	if len(client.completedTasks) != 3 {
		t.Errorf("Expected 3 tasks completed, got %d", len(client.completedTasks))
	}
}

func TestWorkerService_TaskLoop_StopsOnContextCancel(t *testing.T) {
	client := &mockCoordinatorClient{}
	executor := &mockExecutor{}
	logger := &mockLogger{}

	svc := NewWorkerService(client, executor, 1*time.Second, logger)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		svc.Run(ctx)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Success - Run returned after context cancel
	case <-time.After(500 * time.Millisecond):
		t.Error("Run did not return after context cancel")
	}
}
