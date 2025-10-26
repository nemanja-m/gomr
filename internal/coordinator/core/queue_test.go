package core

import (
	"sync"
	"testing"

	"github.com/google/uuid"
)

func createTestTask(id string) *Task {
	return &Task{
		ID:    uuid.MustParse(id),
		JobID: uuid.New(),
		Type:  TaskTypeMap,
	}
}

func TestNewTaskPriorityQueue(t *testing.T) {
	q := NewTaskPriorityQueue()
	if q == nil {
		t.Fatal("NewTaskPriorityQueue returned nil")
	}
	if q.Len() != 0 {
		t.Errorf("expected new queue to have length 0, got %d", q.Len())
	}
}

func TestTaskPriorityQueue_Push(t *testing.T) {
	tests := []struct {
		name     string
		task     *Task
		priority TaskPriority
		wantErr  bool
	}{
		{
			name:     "push valid task with high priority",
			task:     createTestTask("00000000-0000-0000-0000-000000000001"),
			priority: TaskPriorityHigh,
			wantErr:  false,
		},
		{
			name:     "push valid task with medium priority",
			task:     createTestTask("00000000-0000-0000-0000-000000000002"),
			priority: TaskPriorityMedium,
			wantErr:  false,
		},
		{
			name:     "push valid task with low priority",
			task:     createTestTask("00000000-0000-0000-0000-000000000003"),
			priority: TaskPriorityLow,
			wantErr:  false,
		},
		{
			name:     "push nil task returns error",
			task:     nil,
			priority: TaskPriorityHigh,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := NewTaskPriorityQueue()
			err := q.Push(tt.task, tt.priority)
			if (err != nil) != tt.wantErr {
				t.Errorf("Push() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && q.Len() != 1 {
				t.Errorf("expected queue length 1 after push, got %d", q.Len())
			}
		})
	}
}

func TestTaskPriorityQueue_Pop(t *testing.T) {
	t.Run("pop from empty queue returns error", func(t *testing.T) {
		q := NewTaskPriorityQueue()
		task, err := q.Pop()
		if err != ErrQueueEmpty {
			t.Errorf("expected ErrQueueEmpty, got %v", err)
		}
		if task != nil {
			t.Errorf("expected nil task, got %v", task)
		}
	})

	t.Run("pop single task", func(t *testing.T) {
		q := NewTaskPriorityQueue()
		expectedTask := createTestTask("00000000-0000-0000-0000-000000000001")
		_ = q.Push(expectedTask, TaskPriorityHigh)

		task, err := q.Pop()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if task != expectedTask {
			t.Errorf("expected task %v, got %v", expectedTask, task)
		}
		if q.Len() != 0 {
			t.Errorf("expected queue length 0 after pop, got %d", q.Len())
		}
	})

	t.Run("pop returns highest priority task first", func(t *testing.T) {
		q := NewTaskPriorityQueue()
		lowTask := createTestTask("00000000-0000-0000-0000-000000000001")
		highTask := createTestTask("00000000-0000-0000-0000-000000000002")
		mediumTask := createTestTask("00000000-0000-0000-0000-000000000003")

		_ = q.Push(lowTask, TaskPriorityLow)
		_ = q.Push(highTask, TaskPriorityHigh)
		_ = q.Push(mediumTask, TaskPriorityMedium)

		// Should pop high priority first
		task, err := q.Pop()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if task.ID != highTask.ID {
			t.Errorf("expected high priority task first, got task %v", task.ID)
		}

		// Then medium priority
		task, err = q.Pop()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if task.ID != mediumTask.ID {
			t.Errorf("expected medium priority task second, got task %v", task.ID)
		}

		// Finally low priority
		task, err = q.Pop()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if task.ID != lowTask.ID {
			t.Errorf("expected low priority task last, got task %v", task.ID)
		}

		if q.Len() != 0 {
			t.Errorf("expected empty queue, got length %d", q.Len())
		}
	})

	t.Run("FIFO order for same priority tasks", func(t *testing.T) {
		q := NewTaskPriorityQueue()
		task1 := createTestTask("00000000-0000-0000-0000-000000000001")
		task2 := createTestTask("00000000-0000-0000-0000-000000000002")
		task3 := createTestTask("00000000-0000-0000-0000-000000000003")

		_ = q.Push(task1, TaskPriorityMedium)
		_ = q.Push(task2, TaskPriorityMedium)
		_ = q.Push(task3, TaskPriorityMedium)

		// Should maintain FIFO order for same priority
		task, _ := q.Pop()
		if task.ID != task1.ID {
			t.Errorf("expected task1 first, got %v", task.ID)
		}

		task, _ = q.Pop()
		if task.ID != task2.ID {
			t.Errorf("expected task2 second, got %v", task.ID)
		}

		task, _ = q.Pop()
		if task.ID != task3.ID {
			t.Errorf("expected task3 third, got %v", task.ID)
		}
	})
}

func TestTaskPriorityQueue_Top(t *testing.T) {
	t.Run("top on empty queue returns error", func(t *testing.T) {
		q := NewTaskPriorityQueue()
		task, err := q.Top()
		if err != ErrQueueEmpty {
			t.Errorf("expected ErrQueueEmpty, got %v", err)
		}
		if task != nil {
			t.Errorf("expected nil task, got %v", task)
		}
	})

	t.Run("top returns highest priority without removing", func(t *testing.T) {
		q := NewTaskPriorityQueue()
		lowTask := createTestTask("00000000-0000-0000-0000-000000000001")
		highTask := createTestTask("00000000-0000-0000-0000-000000000002")

		_ = q.Push(lowTask, TaskPriorityLow)
		_ = q.Push(highTask, TaskPriorityHigh)

		// Top should return high priority task
		task, err := q.Top()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if task.ID != highTask.ID {
			t.Errorf("expected high priority task, got %v", task.ID)
		}

		// Queue length should remain unchanged
		if q.Len() != 2 {
			t.Errorf("expected queue length 2, got %d", q.Len())
		}

		// Second call to Top should return same task
		task2, err := q.Top()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if task2.ID != highTask.ID {
			t.Errorf("expected same high priority task, got %v", task2.ID)
		}
	})
}

func TestTaskPriorityQueue_Len(t *testing.T) {
	q := NewTaskPriorityQueue()

	if q.Len() != 0 {
		t.Errorf("expected initial length 0, got %d", q.Len())
	}

	_ = q.Push(createTestTask("00000000-0000-0000-0000-000000000001"), TaskPriorityHigh)
	if q.Len() != 1 {
		t.Errorf("expected length 1 after push, got %d", q.Len())
	}

	_ = q.Push(createTestTask("00000000-0000-0000-0000-000000000002"), TaskPriorityMedium)
	if q.Len() != 2 {
		t.Errorf("expected length 2 after second push, got %d", q.Len())
	}

	_, _ = q.Pop()
	if q.Len() != 1 {
		t.Errorf("expected length 1 after pop, got %d", q.Len())
	}

	_, _ = q.Pop()
	if q.Len() != 0 {
		t.Errorf("expected length 0 after second pop, got %d", q.Len())
	}
}

func TestTaskPriorityQueue_Concurrent(t *testing.T) {
	t.Run("concurrent push operations", func(t *testing.T) {
		q := NewTaskPriorityQueue()
		var wg sync.WaitGroup
		numGoroutines := 100
		numTasksPerGoroutine := 10

		wg.Add(numGoroutines)
		for i := range numGoroutines {
			go func(goroutineID int) {
				defer wg.Done()
				for j := 0; j < numTasksPerGoroutine; j++ {
					task := &Task{
						ID:    uuid.New(),
						JobID: uuid.New(),
						Type:  TaskTypeMap,
					}
					priority := TaskPriority(j % 3)
					_ = q.Push(task, priority)
				}
			}(i)
		}

		wg.Wait()

		expectedLen := numGoroutines * numTasksPerGoroutine
		if q.Len() != expectedLen {
			t.Errorf("expected queue length %d, got %d", expectedLen, q.Len())
		}
	})

	t.Run("concurrent pop operations", func(t *testing.T) {
		q := NewTaskPriorityQueue()
		numTasks := 100

		// Push tasks first
		for range numTasks {
			task := createTestTask(uuid.New().String())
			_ = q.Push(task, TaskPriorityMedium)
		}

		var wg sync.WaitGroup
		numGoroutines := 10
		tasksPopped := make(chan *Task, numTasks)

		wg.Add(numGoroutines)
		for range numGoroutines {
			go func() {
				defer wg.Done()
				for {
					task, err := q.Pop()
					if err == ErrQueueEmpty {
						return
					}
					if task != nil {
						tasksPopped <- task
					}
				}
			}()
		}

		wg.Wait()
		close(tasksPopped)

		// Count popped tasks
		count := 0
		for range tasksPopped {
			count++
		}

		if count != numTasks {
			t.Errorf("expected %d tasks popped, got %d", numTasks, count)
		}

		if q.Len() != 0 {
			t.Errorf("expected empty queue, got length %d", q.Len())
		}
	})

	t.Run("concurrent push and pop", func(t *testing.T) {
		q := NewTaskPriorityQueue()
		var wg sync.WaitGroup
		numOperations := 50

		// Concurrent pushes
		wg.Go(func() {
			for range numOperations {
				task := createTestTask(uuid.New().String())
				_ = q.Push(task, TaskPriorityMedium)
			}
		})

		// Concurrent pops (will get some errors for empty queue)
		wg.Go(func() {
			for range numOperations {
				_, _ = q.Pop() // Ignore errors
			}
		})

		wg.Wait()

		// Queue should either be empty or have remaining tasks
		length := q.Len()
		if length < 0 || length > numOperations {
			t.Errorf("unexpected queue length %d", length)
		}
	})

	t.Run("concurrent top operations", func(t *testing.T) {
		q := NewTaskPriorityQueue()
		highTask := createTestTask("00000000-0000-0000-0000-000000000001")
		_ = q.Push(highTask, TaskPriorityHigh)
		_ = q.Push(createTestTask("00000000-0000-0000-0000-000000000002"), TaskPriorityLow)

		var wg sync.WaitGroup
		numGoroutines := 50

		wg.Add(numGoroutines)
		for range numGoroutines {
			go func() {
				defer wg.Done()
				task, err := q.Top()
				if err != nil {
					t.Errorf("unexpected error from Top(): %v", err)
					return
				}
				if task.ID != highTask.ID {
					t.Errorf("expected high priority task, got %v", task.ID)
				}
			}()
		}

		wg.Wait()

		// Queue should still have both tasks
		if q.Len() != 2 {
			t.Errorf("expected queue length 2, got %d", q.Len())
		}
	})
}

func TestTaskPriorityQueue_PriorityOrdering(t *testing.T) {
	q := NewTaskPriorityQueue()

	// Create tasks with different priorities in random order
	tasks := []struct {
		task     *Task
		priority TaskPriority
	}{
		{createTestTask("00000000-0000-0000-0000-000000000001"), TaskPriorityMedium},
		{createTestTask("00000000-0000-0000-0000-000000000002"), TaskPriorityLow},
		{createTestTask("00000000-0000-0000-0000-000000000003"), TaskPriorityHigh},
		{createTestTask("00000000-0000-0000-0000-000000000004"), TaskPriorityLow},
		{createTestTask("00000000-0000-0000-0000-000000000005"), TaskPriorityHigh},
		{createTestTask("00000000-0000-0000-0000-000000000006"), TaskPriorityMedium},
	}

	for _, tt := range tasks {
		_ = q.Push(tt.task, tt.priority)
	}

	// Pop all tasks and verify priority ordering
	expectedOrder := []TaskPriority{
		TaskPriorityHigh,   // task 3
		TaskPriorityHigh,   // task 5
		TaskPriorityMedium, // task 1
		TaskPriorityMedium, // task 6
		TaskPriorityLow,    // task 2
		TaskPriorityLow,    // task 4
	}

	for i, expectedPriority := range expectedOrder {
		task, err := q.Pop()
		if err != nil {
			t.Fatalf("unexpected error at position %d: %v", i, err)
		}

		// Find the priority of the popped task
		var actualPriority TaskPriority
		for _, tt := range tasks {
			if tt.task.ID == task.ID {
				actualPriority = tt.priority
				break
			}
		}

		if actualPriority != expectedPriority {
			t.Errorf("at position %d: expected priority %v, got %v (task %v)",
				i, expectedPriority, actualPriority, task.ID)
		}
	}
}

func TestTaskPriorityQueue_MixedPriorityFIFO(t *testing.T) {
	q := NewTaskPriorityQueue()

	// Push tasks with specific IDs to verify FIFO within each priority level
	highTask1 := createTestTask("00000000-0000-0000-0000-000000000001")
	highTask2 := createTestTask("00000000-0000-0000-0000-000000000002")
	medTask1 := createTestTask("00000000-0000-0000-0000-000000000003")
	lowTask1 := createTestTask("00000000-0000-0000-0000-000000000004")
	medTask2 := createTestTask("00000000-0000-0000-0000-000000000005")
	highTask3 := createTestTask("00000000-0000-0000-0000-000000000006")

	// Push in mixed order
	_ = q.Push(highTask1, TaskPriorityHigh)
	_ = q.Push(medTask1, TaskPriorityMedium)
	_ = q.Push(highTask2, TaskPriorityHigh)
	_ = q.Push(lowTask1, TaskPriorityLow)
	_ = q.Push(medTask2, TaskPriorityMedium)
	_ = q.Push(highTask3, TaskPriorityHigh)

	// Expected order:
	// 1. High priority tasks in FIFO: highTask1, highTask2, highTask3
	// 2. Medium priority tasks in FIFO: medTask1, medTask2
	// 3. Low priority tasks in FIFO: lowTask1
	expectedOrder := []uuid.UUID{
		highTask1.ID,
		highTask2.ID,
		highTask3.ID,
		medTask1.ID,
		medTask2.ID,
		lowTask1.ID,
	}

	for i, expectedID := range expectedOrder {
		task, err := q.Pop()
		if err != nil {
			t.Fatalf("unexpected error at position %d: %v", i, err)
		}
		if task.ID != expectedID {
			t.Errorf("at position %d: expected task %v, got %v", i, expectedID, task.ID)
		}
	}

	if q.Len() != 0 {
		t.Errorf("expected empty queue, got length %d", q.Len())
	}
}

func TestErrQueueEmpty(t *testing.T) {
	if ErrQueueEmpty == nil {
		t.Error("ErrQueueEmpty should not be nil")
	}
	if ErrQueueEmpty.Error() != "priority queue is empty" {
		t.Errorf("unexpected error message: %s", ErrQueueEmpty.Error())
	}
}
