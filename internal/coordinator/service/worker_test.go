package service

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/nemanja-m/gomr/internal/coordinator/core"
)

// mockWorkerStore is an in-memory implementation of WorkerStore for testing
type mockWorkerStore struct {
	mu      sync.RWMutex
	workers map[uuid.UUID]*core.Worker
}

func newMockWorkerStore() *mockWorkerStore {
	return &mockWorkerStore{
		workers: make(map[uuid.UUID]*core.Worker),
	}
}

func (m *mockWorkerStore) AddWorker(worker *core.Worker) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if worker == nil {
		return fmt.Errorf("worker is nil")
	}
	m.workers[worker.ID] = worker
	return nil
}

func (m *mockWorkerStore) GetWorkerByID(id uuid.UUID) (*core.Worker, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	worker, exists := m.workers[id]
	if !exists {
		return nil, nil
	}
	return worker, nil
}

func (m *mockWorkerStore) GetAllWorkers() ([]*core.Worker, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	workers := make([]*core.Worker, 0, len(m.workers))
	for _, w := range m.workers {
		workers = append(workers, w)
	}
	return workers, nil
}

func (m *mockWorkerStore) UpdateWorkerHeartbeat(id uuid.UUID, timestamp time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	worker, exists := m.workers[id]
	if !exists {
		return fmt.Errorf("worker not found: %s", id)
	}
	worker.LastHeartbeatAt = timestamp
	return nil
}

func (m *mockWorkerStore) RemoveWorker(id uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.workers, id)
	return nil
}

func (m *mockWorkerStore) GetStaleWorkers(threshold time.Time) ([]*core.Worker, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var stale []*core.Worker
	for _, worker := range m.workers {
		if worker.LastHeartbeatAt.Before(threshold) {
			stale = append(stale, worker)
		}
	}
	return stale, nil
}

// mockLogger is a no-op logger for testing
type workerTestLogger struct {
	messages []string
	mu       sync.Mutex
}

func newWorkerTestLogger() *workerTestLogger {
	return &workerTestLogger{
		messages: []string{},
	}
}

func (m *workerTestLogger) Debug(msg string, args ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, msg)
}

func (m *workerTestLogger) Info(msg string, args ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, msg)
}

func (m *workerTestLogger) Warn(msg string, args ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, msg)
}

func (m *workerTestLogger) Error(msg string, args ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, msg)
}

func (m *workerTestLogger) Fatal(msg string, args ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, msg)
}

func TestWorkerService_RegisterWorker_Success(t *testing.T) {
	t.Run("register valid worker", func(t *testing.T) {
		store := newMockWorkerStore()
		logger := &workerTestLogger{}
		service := NewWorkerService(store, logger)

		worker := &core.Worker{
			ID:      uuid.New(),
			Address: "localhost:5000",
			Capabilities: core.WorkerCapabilities{
				AvailableCpuCores:    4,
				AvailableMemoryBytes: 8589934592, // 8GB
			},
		}

		err := service.RegisterWorker(worker)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Verify worker was stored
		storedWorker, err := store.GetWorkerByID(worker.ID)
		if err != nil {
			t.Fatalf("failed to get worker from store: %v", err)
		}
		if storedWorker == nil {
			t.Fatal("worker not found in store")
		}
		if storedWorker.ID != worker.ID {
			t.Errorf("expected worker ID %s, got %s", worker.ID, storedWorker.ID)
		}
		if storedWorker.Address != worker.Address {
			t.Errorf("expected address %s, got %s", worker.Address, storedWorker.Address)
		}
	})

	t.Run("register multiple workers", func(t *testing.T) {
		store := newMockWorkerStore()
		logger := &workerTestLogger{}
		service := NewWorkerService(store, logger)

		workers := make([]*core.Worker, 3)
		for i := 0; i < 3; i++ {
			workers[i] = &core.Worker{
				ID:      uuid.New(),
				Address: fmt.Sprintf("worker-%d:5000", i),
			}
		}

		for _, w := range workers {
			err := service.RegisterWorker(w)
			if err != nil {
				t.Fatalf("unexpected error registering worker %s: %v", w.ID, err)
			}
		}

		// Verify all workers were stored
		allWorkers, err := store.GetAllWorkers()
		if err != nil {
			t.Fatalf("failed to get all workers: %v", err)
		}
		if len(allWorkers) != 3 {
			t.Errorf("expected 3 workers, got %d", len(allWorkers))
		}
	})

	t.Run("register worker with capabilities", func(t *testing.T) {
		store := newMockWorkerStore()
		logger := &workerTestLogger{}
		service := NewWorkerService(store, logger)

		worker := &core.Worker{
			ID:      uuid.New(),
			Address: "worker.example.com:5000",
			Capabilities: core.WorkerCapabilities{
				AvailableCpuCores:    8,
				AvailableMemoryBytes: 17179869184, // 16GB
			},
		}

		err := service.RegisterWorker(worker)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		storedWorker, err := store.GetWorkerByID(worker.ID)
		if err != nil {
			t.Fatalf("failed to get worker: %v", err)
		}
		if storedWorker.Capabilities.AvailableCpuCores != 8 {
			t.Errorf("expected 8 CPU cores, got %f", storedWorker.Capabilities.AvailableCpuCores)
		}
		if storedWorker.Capabilities.AvailableMemoryBytes != 17179869184 {
			t.Errorf("expected 16GB memory, got %d bytes", storedWorker.Capabilities.AvailableMemoryBytes)
		}
	})
}

func TestWorkerService_RegisterWorker_Logging(t *testing.T) {
	t.Run("logs worker registration", func(t *testing.T) {
		store := newMockWorkerStore()
		logger := newWorkerTestLogger()
		service := NewWorkerService(store, logger)

		worker := &core.Worker{
			ID:      uuid.New(),
			Address: "localhost:5000",
		}

		_ = service.RegisterWorker(worker)

		if len(logger.messages) == 0 {
			t.Fatal("expected log message, but none was recorded")
		}

		if logger.messages[0] != "Registering worker" {
			t.Errorf("expected log message 'Registering worker', got %s", logger.messages[0])
		}
	})
}

func TestWorkerService_RegisterWorker_DuplicateWorker(t *testing.T) {
	t.Run("re-register existing worker overwrites previous entry", func(t *testing.T) {
		store := newMockWorkerStore()
		logger := &workerTestLogger{}
		service := NewWorkerService(store, logger)

		workerID := uuid.New()
		worker1 := &core.Worker{
			ID:      workerID,
			Address: "old-address:5000",
		}

		err := service.RegisterWorker(worker1)
		if err != nil {
			t.Fatalf("unexpected error registering first worker: %v", err)
		}

		// Register same worker ID with different address
		worker2 := &core.Worker{
			ID:      workerID,
			Address: "new-address:5000",
		}

		err = service.RegisterWorker(worker2)
		if err != nil {
			t.Fatalf("unexpected error registering second worker: %v", err)
		}

		// Verify the new worker data is stored
		storedWorker, err := store.GetWorkerByID(workerID)
		if err != nil {
			t.Fatalf("failed to get worker: %v", err)
		}
		if storedWorker.Address != "new-address:5000" {
			t.Errorf("expected address 'new-address:5000', got %s", storedWorker.Address)
		}
	})
}

func TestWorkerService_RegisterWorker_StoreError(t *testing.T) {
	t.Run("propagates store errors", func(t *testing.T) {
		// Create a store that returns errors
		failingStore := &failingWorkerStore{}
		logger := &workerTestLogger{}
		service := NewWorkerService(failingStore, logger)

		worker := &core.Worker{
			ID:      uuid.New(),
			Address: "localhost:5000",
		}

		err := service.RegisterWorker(worker)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err.Error() != "store error" {
			t.Errorf("expected error 'store error', got %s", err.Error())
		}
	})
}

// failingWorkerStore is a mock that always returns errors
type failingWorkerStore struct{}

func (f *failingWorkerStore) AddWorker(worker *core.Worker) error {
	return fmt.Errorf("store error")
}

func (f *failingWorkerStore) GetWorkerByID(id uuid.UUID) (*core.Worker, error) {
	return nil, fmt.Errorf("store error")
}

func (f *failingWorkerStore) GetAllWorkers() ([]*core.Worker, error) {
	return nil, fmt.Errorf("store error")
}

func (f *failingWorkerStore) UpdateWorkerHeartbeat(id uuid.UUID, timestamp time.Time) error {
	return fmt.Errorf("store error")
}

func (f *failingWorkerStore) RemoveWorker(id uuid.UUID) error {
	return fmt.Errorf("store error")
}

func (f *failingWorkerStore) GetStaleWorkers(threshold time.Time) ([]*core.Worker, error) {
	return nil, fmt.Errorf("store error")
}

func TestWorkerService_Concurrent(t *testing.T) {
	t.Run("concurrent worker registration", func(t *testing.T) {
		store := newMockWorkerStore()
		logger := &workerTestLogger{}
		service := NewWorkerService(store, logger)

		numWorkers := 10
		errors := make(chan error, numWorkers)
		var wg sync.WaitGroup

		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				worker := &core.Worker{
					ID:      uuid.New(),
					Address: fmt.Sprintf("worker-%d:5000", index),
				}
				errors <- service.RegisterWorker(worker)
			}(i)
		}

		wg.Wait()
		close(errors)

		// Check for any errors
		for err := range errors {
			if err != nil {
				t.Errorf("unexpected error during concurrent registration: %v", err)
			}
		}

		// Verify all workers were stored
		allWorkers, err := store.GetAllWorkers()
		if err != nil {
			t.Fatalf("failed to get all workers: %v", err)
		}
		if len(allWorkers) != numWorkers {
			t.Errorf("expected %d workers, got %d", numWorkers, len(allWorkers))
		}
	})
}

func TestWorkerService_EdgeCases(t *testing.T) {
	t.Run("register worker with empty address", func(t *testing.T) {
		store := newMockWorkerStore()
		logger := &workerTestLogger{}
		service := NewWorkerService(store, logger)

		worker := &core.Worker{
			ID:      uuid.New(),
			Address: "",
		}

		// Should still register (validation might be at API layer)
		err := service.RegisterWorker(worker)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		storedWorker, err := store.GetWorkerByID(worker.ID)
		if err != nil {
			t.Fatalf("failed to get worker: %v", err)
		}
		if storedWorker.Address != "" {
			t.Errorf("expected empty address, got %s", storedWorker.Address)
		}
	})

	t.Run("register worker with zero capabilities", func(t *testing.T) {
		store := newMockWorkerStore()
		logger := &workerTestLogger{}
		service := NewWorkerService(store, logger)

		worker := &core.Worker{
			ID:      uuid.New(),
			Address: "localhost:5000",
			Capabilities: core.WorkerCapabilities{
				AvailableCpuCores:    0,
				AvailableMemoryBytes: 0,
			},
		}

		err := service.RegisterWorker(worker)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		storedWorker, err := store.GetWorkerByID(worker.ID)
		if err != nil {
			t.Fatalf("failed to get worker: %v", err)
		}
		if storedWorker.Capabilities.AvailableCpuCores != 0 {
			t.Errorf("expected 0 CPU cores, got %f", storedWorker.Capabilities.AvailableCpuCores)
		}
	})
}
