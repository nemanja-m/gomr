package core

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/google/uuid"

	"github.com/nemanja-m/gomr/internal/shared/logging"
)

type JobController interface {
	SubmitJob(job *Job) error
	GetJob(id uuid.UUID) (*Job, error)
	GetJobs() ([]*Job, error)
	GetTasks(jobID uuid.UUID) ([]*Task, error)
}

type jobController struct {
	jobStore JobStore
	logger   logging.Logger
}

func NewJobController(jobStore JobStore, logger logging.Logger) JobController {
	return &jobController{
		jobStore: jobStore,
		logger:   logger,
	}
}

func (c *jobController) SubmitJob(job *Job) (err error) {
	if job.Input.Type != "local" {
		return fmt.Errorf("unsupported input type: %s", job.Input.Type)
	}

	c.logger.Info("Submitting job", "job_id", job.ID.String(), "name", job.Name)

	inputFiles, err := FindLocalFiles(job.Input.Paths)
	if err != nil {
		return err
	}
	if len(inputFiles) == 0 {
		return fmt.Errorf("no input files found for job %s", job.ID.String())
	}

	shuffleDir, err := CreateLocalShuffleDir(job.ID)
	if err != nil {
		return err
	}

	mapTasks := make([]*Task, 0, len(inputFiles))
	for mapperId, filePath := range inputFiles {
		task := &Task{
			ID:     uuid.New(),
			JobID:  job.ID,
			Type:   TaskTypeMap,
			Status: TaskStatusPending,
			Input: InputConfig{
				Type:   job.Input.Type,
				Format: job.Input.Format,
				Paths:  []string{filePath},
			},
			Output: OutputConfig{
				Type: job.Input.Type,
				Path: filepath.Join(shuffleDir, fmt.Sprintf("map-%016d", mapperId)),
			},
		}
		mapTasks = append(mapTasks, task)
	}

	reduceTasks := make([]*Task, 0, job.Config.NumReducers)
	for reducerId := 0; reducerId < job.Config.NumReducers; reducerId++ {
		shufflePattern := filepath.Join(shuffleDir, "map-*", fmt.Sprintf("part-%016d", reducerId))
		task := &Task{
			ID:     uuid.New(),
			JobID:  job.ID,
			Type:   TaskTypeReduce,
			Status: TaskStatusPending,
			Input: InputConfig{
				Paths: []string{shufflePattern},
			},
			Output: OutputConfig{
				Type: job.Output.Type,
				Path: filepath.Join(job.Output.Path, fmt.Sprintf("part-%016d", reducerId)),
			},
		}
		reduceTasks = append(reduceTasks, task)
	}
	allTasks := append(mapTasks, reduceTasks...)

	job.Progress = JobProgress{
		Map:    TaskProgress{Total: len(mapTasks)},
		Reduce: TaskProgress{Total: len(reduceTasks)},
	}
	job.Status = JobStatusRunning
	job.StartedAt = ptrTimeNow()

	err = c.jobStore.SaveJob(job, allTasks...)
	if err != nil {
		return err
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

func (c *jobController) GetJob(id uuid.UUID) (*Job, error) {
	return c.jobStore.GetJobByID(id)
}

func (c *jobController) GetJobs() ([]*Job, error) {
	return c.jobStore.GetJobs()
}

func (c *jobController) GetTasks(jobID uuid.UUID) ([]*Task, error) {
	return c.jobStore.GetTasksByJobID(jobID)
}

func ptrTimeNow() *time.Time {
	t := time.Now().UTC()
	return &t
}
