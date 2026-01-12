package grpc

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/google/uuid"
	"github.com/nemanja-m/gomr/internal/shared/proto"
)

type CoordinatorClient struct {
	conn   *grpc.ClientConn
	client proto.CoordinatorServiceClient

	workerID        uuid.UUID
	coordinatorAddr string
}

func NewCoordinatorClient(coordinatorAddr string, workerID uuid.UUID) (*CoordinatorClient, error) {
	conn, err := grpc.NewClient(
		coordinatorAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(
			keepalive.ClientParameters{
				Time:                10 * time.Second,
				Timeout:             5 * time.Second,
				PermitWithoutStream: true,
			},
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to coordinator: %w", err)
	}

	client := proto.NewCoordinatorServiceClient(conn)

	return &CoordinatorClient{
		conn:            conn,
		client:          client,
		workerID:        workerID,
		coordinatorAddr: coordinatorAddr,
	}, nil
}

func (c *CoordinatorClient) RegisterWorker(
	ctx context.Context,
	addr string,
	availableCpuCores uint32,
	availableMemoryBytes uint64,
) error {
	req := &proto.RegisterWorkerRequest{
		WorkerId:             c.workerID.String(),
		Address:              addr,
		AvailableCpuCores:    availableCpuCores,
		AvailableMemoryBytes: availableMemoryBytes,
	}
	resp, err := c.client.RegisterWorker(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to register worker: %w", err)
	}

	switch resp.Status {
	case proto.RegistrationStatus_BAD_REQUEST:
		return fmt.Errorf("bad request: %s", resp.Message)
	case proto.RegistrationStatus_REJECTED:
		return fmt.Errorf("coordinator rejected worker: %s", resp.Message)
	case proto.RegistrationStatus_FAILED:
		return fmt.Errorf("coordinator failed to register worker: %s", resp.Message)
	}

	return nil
}

func (c *CoordinatorClient) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}
