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

func (c *CoordinatorClient) PullTask(ctx context.Context) (*proto.TaskAssignment, error) {
	req := &proto.PullTaskRequest{
		WorkerId: c.workerID.String(),
	}
	resp, err := c.client.PullTask(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("pull task failed: %w", err)
	}
	return resp.Task, nil
}

func (c *CoordinatorClient) CompleteTask(ctx context.Context, taskID string) error {
	req := &proto.CompleteTaskRequest{
		WorkerId: c.workerID.String(),
		TaskId:   taskID,
	}
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
	req := &proto.FailTaskRequest{
		WorkerId: c.workerID.String(),
		TaskId:   taskID,
		Error:    errMsg,
	}
	resp, err := c.client.FailTask(ctx, req)
	if err != nil {
		return fmt.Errorf("fail task failed: %w", err)
	}
	if !resp.Acknowledged {
		return fmt.Errorf("task failure not acknowledged: %s", resp.Message)
	}
	return nil
}

func (c *CoordinatorClient) StartTaskLoop(ctx context.Context, pullInterval time.Duration, logger logging.Logger) {
	ticker := time.NewTicker(pullInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			task, err := c.PullTask(ctx)
			if err != nil {
				logger.Error("Failed to pull task", "error", err)
				continue
			}

			if task == nil {
				logger.Debug("No tasks available")
				continue
			}

			logger.Info("Received task",
				"task_id", task.TaskId,
				"job_id", task.JobId,
				"type", task.Type.String(),
				"attempt", task.Attempt,
				"input_paths", task.Input.Paths,
				"output_path", task.Output.Path,
			)

			// Placeholder: Execute task (future implementation)
			// For now, just mark as complete
			if err := c.CompleteTask(ctx, task.TaskId); err != nil {
				logger.Error("Failed to complete task", "task_id", task.TaskId, "error", err)
				// Try to mark as failed
				if failErr := c.FailTask(ctx, task.TaskId, err.Error()); failErr != nil {
					logger.Error("Failed to report task failure", "task_id", task.TaskId, "error", failErr)
				}
			} else {
				logger.Info("Task completed", "task_id", task.TaskId)
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
