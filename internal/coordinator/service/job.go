package service

import (
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/nemanja-m/gomr/internal/coordinator/core"
	"github.com/nemanja-m/gomr/internal/shared/logging"
)


type jobService struct {
	jobStore   core.JobStore
	taskQueues map[uuid.UUID]core.TaskPriorityQueue

	mu sync.RWMutex

	logger logging.Logger
}

func NewJobService(jobStore core.JobStore, logger logging.Logger) core.JobService {
	return &jobService{
		jobStore:   jobStore,
		taskQueues: make(map[uuid.UUID]core.TaskPriorityQueue),
		logger:     logger,
	}
}

func (c *jobService) SubmitJob(job *core.Job) (err error) {
	if job.Input.Type != "local" {
		return fmt.Errorf("unsupported input type: %s", job.Input.Type)
	}

	c.logger.Info("Submitting job", "job_id", job.ID.String(), "name", job.Name)

	inputFiles, err := core.FindLocalFiles(job.Input.Paths)
	if err != nil {
		return err
	}
	if len(inputFiles) == 0 {
		return fmt.Errorf("no input files found for job %s", job.ID.String())
	}

	shuffleDir, err := core.CreateLocalShuffleDir(job.ID)
	if err != nil {
		return err
	}

	mapTasks := make([]*core.Task, 0, len(inputFiles))
	for mapperId, filePath := range inputFiles {
		task := &core.Task{
			ID:     uuid.New(),
			JobID:  job.ID,
			Type:   core.TaskTypeMap,
			Status: core.TaskStatusPending,
			Input: core.InputConfig{
				Type:   job.Input.Type,
				Format: job.Input.Format,
				Paths:  []string{filePath},
			},
			Output: core.OutputConfig{
				Type: job.Input.Type,
				Path: filepath.Join(shuffleDir, fmt.Sprintf("map-%016d", mapperId)),
			},
		}
		mapTasks = append(mapTasks, task)
	}

	reduceTasks := make([]*core.Task, 0, job.Config.NumReducers)
	for reducerId := 0; reducerId < job.Config.NumReducers; reducerId++ {
		shufflePattern := filepath.Join(shuffleDir, "map-*", fmt.Sprintf("part-%016d", reducerId))
		task := &core.Task{
			ID:     uuid.New(),
			JobID:  job.ID,
			Type:   core.TaskTypeReduce,
			Status: core.TaskStatusPending,
			Input: core.InputConfig{
				Paths: []string{shufflePattern},
			},
			Output: core.OutputConfig{
				Type: job.Output.Type,
				Path: filepath.Join(job.Output.Path, fmt.Sprintf("part-%016d", reducerId)),
			},
		}
		reduceTasks = append(reduceTasks, task)
	}
	allTasks := append(mapTasks, reduceTasks...)

	job.Progress = core.JobProgress{
		Map:    core.TaskProgress{Total: len(mapTasks)},
		Reduce: core.TaskProgress{Total: len(reduceTasks)},
	}
	job.Status = core.JobStatusRunning
	job.StartedAt = ptrTimeNow()

	err = c.jobStore.SaveJob(job, allTasks...)
	if err != nil {
		return err
	}

	// Push tasks to the in-memory priority queue.
	// Map tasks have higher priority than reduce tasks.
	c.mu.Lock()
	defer c.mu.Unlock()
	taskQueue, exists := c.taskQueues[job.ID]
	if !exists {
		taskQueue = core.NewTaskPriorityQueue()
		c.taskQueues[job.ID] = taskQueue
	}
	for _, task := range mapTasks {
		err = taskQueue.Push(task, core.TaskPriorityHigh)
		if err != nil {
			return err
		}
	}
	for _, task := range reduceTasks {
		err = taskQueue.Push(task, core.TaskPriorityLow)
		if err != nil {
			return err
		}
	}

	c.logger.Info(
		"Job submitted",
		"job_id", job.ID.String(),
		"num_map_tasks", len(mapTasks),
		"num_reduce_tasks", len(reduceTasks),
		"shuffle_dir", shuffleDir,
	)

	return nil
}

func (c *jobService) GetJob(id uuid.UUID) (*core.Job, error) {
	return c.jobStore.GetJobByID(id)
}

func (c *jobService) GetJobs(filter core.JobFilter) ([]*core.Job, int, error) {
	return c.jobStore.GetJobs(filter)
}

func (c *jobService) GetTasks(jobID uuid.UUID) ([]*core.Task, error) {
	return c.jobStore.GetTasksByJobID(jobID)
}

func (c *jobService) AssignTask(workerID uuid.UUID) (*core.Task, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, taskQueue := range c.taskQueues {
		if taskQueue.Len() == 0 {
			continue
		}

		top, err := taskQueue.Top()
		if err != nil {
			continue
		}

		// Check if reduce tasks can run (map phase must be complete)
		if top.Type == core.TaskTypeReduce {
			completed, err := c.jobStore.IsMapPhaseCompleted(top.JobID)
			if err != nil {
				return nil, err
			}
			if !completed {
				continue
			}
		}

		task, err := taskQueue.Pop()
		if err != nil {
			return nil, err
		}

		now := time.Now().UTC()
		task.Status = core.TaskStatusRunning
		task.WorkerID = &workerID
		task.StartedAt = &now
		task.Attempt++

		if err := c.jobStore.UpdateTask(task); err != nil {
			return nil, err
		}

		if err := c.updateJobProgress(task.JobID); err != nil {
			c.logger.Error("Failed to update job progress", "job_id", task.JobID, "error", err)
		}

		c.logger.Info("Task assigned",
			"task_id", task.ID,
			"job_id", task.JobID,
			"type", task.Type,
			"worker_id", workerID,
			"attempt", task.Attempt,
		)

		return task, nil
	}

	return nil, nil
}

func (c *jobService) CompleteTask(taskID uuid.UUID, workerID uuid.UUID) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	task, err := c.jobStore.GetTaskByID(taskID)
	if err != nil {
		return err
	}
	if task == nil {
		return fmt.Errorf("task not found: %s", taskID)
	}

	if task.WorkerID == nil || *task.WorkerID != workerID {
		return fmt.Errorf("task %s not assigned to worker %s", taskID, workerID)
	}

	now := time.Now().UTC()
	task.Status = core.TaskStatusCompleted
	task.EndedAt = &now

	if err := c.jobStore.UpdateTask(task); err != nil {
		return err
	}

	if err := c.updateJobProgress(task.JobID); err != nil {
		return err
	}

	c.logger.Info("Task completed",
		"task_id", taskID,
		"job_id", task.JobID,
		"type", task.Type,
		"worker_id", workerID,
	)

	if err := c.checkJobCompletion(task.JobID); err != nil {
		c.logger.Error("Failed to check job completion", "job_id", task.JobID, "error", err)
	}

	return nil
}

func (c *jobService) FailTask(taskID uuid.UUID, workerID uuid.UUID, errMsg string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	task, err := c.jobStore.GetTaskByID(taskID)
	if err != nil {
		return err
	}
	if task == nil {
		return fmt.Errorf("task not found: %s", taskID)
	}

	if task.WorkerID == nil || *task.WorkerID != workerID {
		return fmt.Errorf("task %s not assigned to worker %s", taskID, workerID)
	}

	job, err := c.jobStore.GetJobByID(task.JobID)
	if err != nil {
		return err
	}
	if job == nil {
		return fmt.Errorf("job not found: %s", task.JobID)
	}

	maxAttempts := job.Config.MaxMapAttempts
	if task.Type == core.TaskTypeReduce {
		maxAttempts = job.Config.MaxReduceAttempts
	}

	now := time.Now().UTC()
	task.EndedAt = &now
	task.Error = &errMsg
	task.WorkerID = nil

	// Permanent failure
	if task.Attempt >= maxAttempts {
		task.Status = core.TaskStatusFailed

		job.Errors = append(job.Errors, core.JobError{
			TaskID:    taskID,
			Attempt:   task.Attempt,
			Error:     errMsg,
			Timestamp: now,
		})

		job.Status = core.JobStatusFailed
		job.CompletedAt = &now

		if err := c.jobStore.UpdateJob(job); err != nil {
			return err
		}

		c.logger.Error("Task permanently failed",
			"task_id", taskID,
			"job_id", task.JobID,
			"attempts", task.Attempt,
			"error", errMsg,
		)
	} else {
		// Requeue for retry
		task.Status = core.TaskStatusPending
		task.StartedAt = nil
		task.EndedAt = nil
		task.Error = nil

		priority := core.TaskPriorityHigh
		if task.Type == core.TaskTypeReduce {
			priority = core.TaskPriorityLow
		}

		taskQueue, exists := c.taskQueues[task.JobID]
		if !exists {
			return fmt.Errorf("task queue not found for job %s", task.JobID)
		}
		if err := taskQueue.Push(task, priority); err != nil {
			return err
		}

		c.logger.Warn("Task failed, requeued for retry",
			"task_id", taskID,
			"job_id", task.JobID,
			"attempt", task.Attempt,
			"error", errMsg,
		)
	}

	if err := c.jobStore.UpdateTask(task); err != nil {
		return err
	}

	return c.updateJobProgress(task.JobID)
}

func (c *jobService) RequeueWorkerTasks(workerID uuid.UUID) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	tasks, err := c.jobStore.GetRunningTasksByWorkerID(workerID)
	if err != nil {
		return err
	}

	for _, task := range tasks {
		// Reset task for requeue
		task.Status = core.TaskStatusPending
		task.WorkerID = nil
		task.StartedAt = nil
		task.EndedAt = nil
		task.Error = nil

		priority := core.TaskPriorityHigh
		if task.Type == core.TaskTypeReduce {
			priority = core.TaskPriorityLow
		}

		taskQueue, exists := c.taskQueues[task.JobID]
		if !exists {
			c.logger.Warn("Task queue not found for job", "job_id", task.JobID)
			continue
		}

		if err := taskQueue.Push(task, priority); err != nil {
			c.logger.Error("Failed to requeue task", "task_id", task.ID, "error", err)
			continue
		}

		if err := c.jobStore.UpdateTask(task); err != nil {
			c.logger.Error("Failed to update requeued task", "task_id", task.ID, "error", err)
			continue
		}

		if err := c.updateJobProgress(task.JobID); err != nil {
			c.logger.Error("Failed to update job progress", "job_id", task.JobID, "error", err)
		}

		c.logger.Info("Requeued task from failed worker",
			"task_id", task.ID,
			"job_id", task.JobID,
			"worker_id", workerID,
		)
	}

	return nil
}

func (c *jobService) updateJobProgress(jobID uuid.UUID) error {
	tasks, err := c.jobStore.GetTasksByJobID(jobID)
	if err != nil {
		return err
	}

	job, err := c.jobStore.GetJobByID(jobID)
	if err != nil {
		return err
	}
	if job == nil {
		return fmt.Errorf("job not found: %s", jobID)
	}

	mapProgress := core.TaskProgress{}
	reduceProgress := core.TaskProgress{}

	for _, task := range tasks {
		var progress *core.TaskProgress
		if task.Type == core.TaskTypeMap {
			progress = &mapProgress
		} else {
			progress = &reduceProgress
		}

		progress.Total++
		switch task.Status {
		case core.TaskStatusPending:
			progress.Pending++
		case core.TaskStatusRunning:
			progress.Running++
		case core.TaskStatusCompleted:
			progress.Completed++
		case core.TaskStatusFailed:
			progress.Failed++
		}
	}

	job.Progress = core.JobProgress{
		Map:    mapProgress,
		Reduce: reduceProgress,
	}

	return c.jobStore.UpdateJob(job)
}

func (c *jobService) checkJobCompletion(jobID uuid.UUID) error {
	job, err := c.jobStore.GetJobByID(jobID)
	if err != nil {
		return err
	}
	if job == nil {
		return fmt.Errorf("job not found: %s", jobID)
	}

	// Job is complete when all map and reduce tasks are completed
	mapDone := job.Progress.Map.Completed == job.Progress.Map.Total
	reduceDone := job.Progress.Reduce.Completed == job.Progress.Reduce.Total

	if mapDone && reduceDone {
		now := time.Now().UTC()
		job.Status = core.JobStatusCompleted
		job.CompletedAt = &now

		if err := c.jobStore.UpdateJob(job); err != nil {
			return err
		}

		delete(c.taskQueues, jobID)

		c.logger.Info("Job completed",
			"job_id", jobID,
			"duration", job.Duration().String(),
			"map_tasks", job.Progress.Map.Total,
			"reduce_tasks", job.Progress.Reduce.Total,
		)
	}

	return nil
}

func ptrTimeNow() *time.Time {
	t := time.Now().UTC()
	return &t
}
