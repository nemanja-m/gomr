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
	"github.com/nemanja-m/gomr/internal/shared/proto"
)

type CoordinatorClient struct {
	conn     *grpc.ClientConn
	client   proto.CoordinatorServiceClient
	workerID uuid.UUID
}

func NewCoordinatorClient(coordinatorAddr string, grpcCfg config.WorkerGRPCConfig, workerID uuid.UUID) (*CoordinatorClient, error) {
	conn, err := grpc.NewClient(
		coordinatorAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                grpcCfg.KeepaliveTime,
			Timeout:             grpcCfg.KeepaliveTimeout,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to coordinator: %w", err)
	}

	return &CoordinatorClient{
		conn:     conn,
		client:   proto.NewCoordinatorServiceClient(conn),
		workerID: workerID,
	}, nil
}

func (c *CoordinatorClient) RegisterWorker(ctx context.Context, addr string, cpuCores uint32, memoryBytes uint64) (time.Duration, error) {
	req := &proto.RegisterWorkerRequest{
		WorkerId:             c.workerID.String(),
		Address:              addr,
		AvailableCpuCores:    cpuCores,
		AvailableMemoryBytes: memoryBytes,
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

	return time.Duration(resp.HeartbeatIntervalSeconds) * time.Second, nil
}

func (c *CoordinatorClient) SendHeartbeat(ctx context.Context) error {
	req := &proto.HeartbeatRequest{WorkerId: c.workerID.String()}
	resp, err := c.client.Heartbeat(ctx, req)
	if err != nil {
		return fmt.Errorf("heartbeat failed: %w", err)
	}
	if !resp.Acknowledged {
		return fmt.Errorf("heartbeat not acknowledged")
	}
	return nil
}

func (c *CoordinatorClient) PullTask(ctx context.Context) (*proto.TaskAssignment, error) {
	req := &proto.PullTaskRequest{WorkerId: c.workerID.String()}
	resp, err := c.client.PullTask(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("pull task failed: %w", err)
	}
	return resp.Task, nil
}

func (c *CoordinatorClient) CompleteTask(ctx context.Context, taskID string) error {
	req := &proto.CompleteTaskRequest{WorkerId: c.workerID.String(), TaskId: taskID}
	resp, err := c.client.CompleteTask(ctx, req)
	if err != nil {
		return fmt.Errorf("complete task failed: %w", err)
	}
	if !resp.Acknowledged {
		return fmt.Errorf("task completion not acknowledged: %s", resp.Message)
	}
	return nil
}

func (c *CoordinatorClient) FailTask(ctx context.Context, taskID string, errMsg string) error {
	req := &proto.FailTaskRequest{WorkerId: c.workerID.String(), TaskId: taskID, Error: errMsg}
	resp, err := c.client.FailTask(ctx, req)
	if err != nil {
		return fmt.Errorf("fail task failed: %w", err)
	}
	if !resp.Acknowledged {
		return fmt.Errorf("task failure not acknowledged: %s", resp.Message)
	}
	return nil
}

func (c *CoordinatorClient) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}
