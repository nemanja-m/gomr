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

type JobService interface {
	SubmitJob(job *core.Job) error
	GetJob(id uuid.UUID) (*core.Job, error)
	GetJobs(filter core.JobFilter) ([]*core.Job, int, error)
	GetTasks(jobID uuid.UUID) ([]*core.Task, error)
	NextTask() (*core.Task, error)
}

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

func (c *jobService) NextTask() (*core.Task, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, taskQueue := range c.taskQueues {
		if taskQueue.Len() > 0 {
			// All map tasks must be completed before running reduce tasks.
			top, err := taskQueue.Top()
			if err != nil {
				return nil, err
			}
			if top.Type == core.TaskTypeReduce {
				completed, err := c.jobStore.IsMapPhaseCompleted(top.JobID)
				if err != nil {
					return nil, err
				}
				if !completed {
					continue
				}
			}
			return taskQueue.Pop()
		}
	}
	return nil, nil
}

func ptrTimeNow() *time.Time {
	t := time.Now().UTC()
	return &t
}
