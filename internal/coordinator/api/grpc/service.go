package grpc

import (
	"context"

	"github.com/nemanja-m/gomr/internal/shared/logging"
	"github.com/nemanja-m/gomr/internal/shared/proto"
)

const (
	DefaultHeartbeatIntervalSeconds = 15
)

type CoordinatorService struct {
	proto.UnimplementedCoordinatorServiceServer

	logger logging.Logger
}

func NewCoordinatorService(logger logging.Logger) *CoordinatorService {
	return &CoordinatorService{
		logger: logger,
	}
}

func (s *CoordinatorService) RegisterWorker(
	ctx context.Context,
	req *proto.RegisterWorkerRequest,
) (*proto.RegisterWorkerResponse, error) {
	return &proto.RegisterWorkerResponse{
		Status:                   proto.RegistrationStatus_SUCCESS,
		Message:                  "OK",
		HeartbeatIntervalSeconds: DefaultHeartbeatIntervalSeconds,
	}, nil
}
