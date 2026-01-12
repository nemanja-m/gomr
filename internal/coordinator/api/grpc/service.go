package grpc

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/nemanja-m/gomr/internal/coordinator/core"
	"github.com/nemanja-m/gomr/internal/shared/logging"
	"github.com/nemanja-m/gomr/internal/shared/proto"
)

type CoordinatorService struct {
	proto.UnimplementedCoordinatorServiceServer

	workerService     core.WorkerService
	jobService        core.JobService
	heartbeatInterval time.Duration

	logger logging.Logger
}

func NewCoordinatorService(
	heartbeatInterval time.Duration,
	workerService core.WorkerService,
	jobService core.JobService,
	logger logging.Logger,
) *CoordinatorService {
	return &CoordinatorService{
		workerService:     workerService,
		jobService:        jobService,
		heartbeatInterval: heartbeatInterval,
		logger:            logger,
	}
}

func (s *CoordinatorService) RegisterWorker(
	ctx context.Context,
	req *proto.RegisterWorkerRequest,
) (*proto.RegisterWorkerResponse, error) {
	workerId, err := uuid.Parse(req.WorkerId)
	if err != nil {
		s.logger.Error("Invalid worker ID format", "worker_id", req.WorkerId, "error", err)
		return &proto.RegisterWorkerResponse{
			Status:  proto.RegistrationStatus_BAD_REQUEST,
			Message: "Invalid worker ID format. Expected UUID.",
		}, nil
	}
	worker := &core.Worker{
		ID:      workerId,
		Address: req.Address,
	}

	s.logger.Debug("Received worker registration", "worker_id", worker.ID.String(), "address", worker.Address)

	if err := s.workerService.RegisterWorker(worker); err != nil {
		s.logger.Error("Failed to register worker", "worker_id", worker.ID.String(), "error", err)
		return &proto.RegisterWorkerResponse{
			Status:  proto.RegistrationStatus_FAILED,
			Message: err.Error(),
		}, nil
	}

	s.logger.Info("Worker registered successfully",
		"worker_id", worker.ID.String(),
		"address", worker.Address,
	)

	return &proto.RegisterWorkerResponse{
		Status:                   proto.RegistrationStatus_SUCCESS,
		Message:                  "OK",
		HeartbeatIntervalSeconds: int32(s.heartbeatInterval.Seconds()),
	}, nil
}

func (s *CoordinatorService) Heartbeat(
	ctx context.Context,
	req *proto.HeartbeatRequest,
) (*proto.HeartbeatResponse, error) {
	workerID, err := uuid.Parse(req.WorkerId)
	if err != nil {
		s.logger.Error("Invalid worker ID in heartbeat", "worker_id", req.WorkerId, "error", err)
		return &proto.HeartbeatResponse{Acknowledged: false}, nil
	}

	if err := s.workerService.RecordHeartbeat(workerID); err != nil {
		s.logger.Error("Failed to record heartbeat", "worker_id", workerID, "error", err)
		return &proto.HeartbeatResponse{Acknowledged: false}, nil
	}

	s.logger.Debug("Heartbeat received", "worker_id", workerID)
	return &proto.HeartbeatResponse{Acknowledged: true}, nil
}

func (s *CoordinatorService) PullTask(
	ctx context.Context,
	req *proto.PullTaskRequest,
) (*proto.PullTaskResponse, error) {
	workerID, err := uuid.Parse(req.WorkerId)
	if err != nil {
		s.logger.Error("Invalid worker ID", "worker_id", req.WorkerId, "error", err)
		return &proto.PullTaskResponse{Task: nil}, nil
	}

	task, err := s.jobService.AssignTask(workerID)
	if err != nil {
		s.logger.Error("Failed to assign task", "worker_id", workerID, "error", err)
		return &proto.PullTaskResponse{Task: nil}, nil
	}

	if task == nil {
		return &proto.PullTaskResponse{Task: nil}, nil
	}

	job, err := s.jobService.GetJob(task.JobID)
	if err != nil {
		s.logger.Error("Failed to get job", "job_id", task.JobID, "error", err)
		return &proto.PullTaskResponse{Task: nil}, nil
	}

	var executor *proto.ExecutorSpec
	if task.Type == core.TaskTypeMap {
		executor = &proto.ExecutorSpec{
			Type: job.Executors.Map.Type,
			Uri:  job.Executors.Map.URI,
		}
	} else {
		executor = &proto.ExecutorSpec{
			Type: job.Executors.Reduce.Type,
			Uri:  job.Executors.Reduce.URI,
		}
	}

	return &proto.PullTaskResponse{
		Task: &proto.TaskAssignment{
			TaskId:   task.ID.String(),
			JobId:    task.JobID.String(),
			Type:     convertTaskType(task.Type),
			Input:    convertInputConfig(task.Input),
			Output:   convertOutputConfig(task.Output),
			Attempt:  int32(task.Attempt),
			Executor: executor,
		},
	}, nil
}

func (s *CoordinatorService) CompleteTask(
	ctx context.Context,
	req *proto.CompleteTaskRequest,
) (*proto.CompleteTaskResponse, error) {
	workerID, err := uuid.Parse(req.WorkerId)
	if err != nil {
		return &proto.CompleteTaskResponse{
			Acknowledged: false,
			Message:      "Invalid worker ID",
		}, nil
	}

	taskID, err := uuid.Parse(req.TaskId)
	if err != nil {
		return &proto.CompleteTaskResponse{
			Acknowledged: false,
			Message:      "Invalid task ID",
		}, nil
	}

	if err := s.jobService.CompleteTask(taskID, workerID); err != nil {
		s.logger.Error("Failed to complete task", "task_id", taskID, "error", err)
		return &proto.CompleteTaskResponse{
			Acknowledged: false,
			Message:      err.Error(),
		}, nil
	}

	return &proto.CompleteTaskResponse{
		Acknowledged: true,
		Message:      "OK",
	}, nil
}

func (s *CoordinatorService) FailTask(
	ctx context.Context,
	req *proto.FailTaskRequest,
) (*proto.FailTaskResponse, error) {
	workerID, err := uuid.Parse(req.WorkerId)
	if err != nil {
		return &proto.FailTaskResponse{
			Acknowledged: false,
			Message:      "Invalid worker ID",
		}, nil
	}

	taskID, err := uuid.Parse(req.TaskId)
	if err != nil {
		return &proto.FailTaskResponse{
			Acknowledged: false,
			Message:      "Invalid task ID",
		}, nil
	}

	if err := s.jobService.FailTask(taskID, workerID, req.Error); err != nil {
		s.logger.Error("Failed to fail task", "task_id", taskID, "error", err)
		return &proto.FailTaskResponse{
			Acknowledged: false,
			Message:      err.Error(),
		}, nil
	}

	return &proto.FailTaskResponse{
		Acknowledged: true,
		Message:      "OK",
	}, nil
}

func convertTaskType(t core.TaskType) proto.TaskType {
	if t == core.TaskTypeMap {
		return proto.TaskType_MAP
	}
	return proto.TaskType_REDUCE
}

func convertInputConfig(c core.InputConfig) *proto.TaskInput {
	return &proto.TaskInput{
		Type:   c.Type,
		Format: c.Format,
		Paths:  c.Paths,
	}
}

func convertOutputConfig(c core.OutputConfig) *proto.TaskOutput {
	return &proto.TaskOutput{
		Type: c.Type,
		Path: c.Path,
	}
}
