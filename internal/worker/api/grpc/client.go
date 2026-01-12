package grpc

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/google/uuid"

	"github.com/nemanja-m/gomr/internal/shared/config"
	"github.com/nemanja-m/gomr/internal/shared/logging"
	"github.com/nemanja-m/gomr/internal/shared/proto"
)

type CoordinatorClient struct {
	conn   *grpc.ClientConn
	client proto.CoordinatorServiceClient

	workerID        uuid.UUID
	coordinatorAddr string
}

func NewCoordinatorClient(coordinatorAddr string, grpcCfg config.WorkerGRPCConfig, workerID uuid.UUID) (*CoordinatorClient, error) {
	conn, err := grpc.NewClient(
		coordinatorAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(
			keepalive.ClientParameters{
				Time:                grpcCfg.KeepaliveTime,
				Timeout:             grpcCfg.KeepaliveTimeout,
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
) (time.Duration, error) {
	req := &proto.RegisterWorkerRequest{
		WorkerId:             c.workerID.String(),
		Address:              addr,
		AvailableCpuCores:    availableCpuCores,
		AvailableMemoryBytes: availableMemoryBytes,
	}
	resp, err := c.client.RegisterWorker(ctx, req)
	if err != nil {
		return 0, fmt.Errorf("failed to register worker: %w", err)
	}

	switch resp.Status {
	case proto.RegistrationStatus_BAD_REQUEST:
		return 0, fmt.Errorf("bad request: %s", resp.Message)
	case proto.RegistrationStatus_REJECTED:
		return 0, fmt.Errorf("coordinator rejected worker: %s", resp.Message)
	case proto.RegistrationStatus_FAILED:
		return 0, fmt.Errorf("coordinator failed to register worker: %s", resp.Message)
	}

	heartbeatInterval := time.Duration(resp.HeartbeatIntervalSeconds) * time.Second
	return heartbeatInterval, nil
}

func (c *CoordinatorClient) SendHeartbeat(ctx context.Context) error {
	req := &proto.HeartbeatRequest{
		WorkerId: c.workerID.String(),
	}
	resp, err := c.client.Heartbeat(ctx, req)
	if err != nil {
		return fmt.Errorf("heartbeat failed: %w", err)
	}
	if !resp.Acknowledged {
		return fmt.Errorf("heartbeat not acknowledged")
	}
	return nil
}

func (c *CoordinatorClient) StartHeartbeat(ctx context.Context, interval time.Duration, logger logging.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.SendHeartbeat(ctx); err != nil {
				logger.Error("Failed to send heartbeat", "error", err)
			} else {
				logger.Debug("Heartbeat sent successfully")
			}
		}
	}
}

func (c *CoordinatorClient) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}
