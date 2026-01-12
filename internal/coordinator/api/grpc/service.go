package grpc

import (
	"context"

	"github.com/google/uuid"

	"github.com/nemanja-m/gomr/internal/coordinator/core"
	"github.com/nemanja-m/gomr/internal/shared/logging"
	"github.com/nemanja-m/gomr/internal/shared/proto"
)

const (
	DefaultHeartbeatIntervalSeconds = 15
)

type CoordinatorService struct {
	proto.UnimplementedCoordinatorServiceServer

	workerService core.WorkerService

	logger logging.Logger
}

func NewCoordinatorService(workerService core.WorkerService, logger logging.Logger) *CoordinatorService {
	return &CoordinatorService{
		workerService: workerService,
		logger:        logger,
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

	s.logger.Info("Worker registered successfully", "worker_id", worker.ID.String())

	return &proto.RegisterWorkerResponse{
		Status:                   proto.RegistrationStatus_SUCCESS,
		Message:                  "OK",
		HeartbeatIntervalSeconds: DefaultHeartbeatIntervalSeconds,
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
