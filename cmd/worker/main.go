package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/nemanja-m/gomr/internal/shared/logging"
	"github.com/nemanja-m/gomr/internal/worker/api/grpc"
)

var (
	coordinatorAddr = ":9090"
	workerAddr      = ":50051"
)

func main() {
	if envAddr := os.Getenv("COORDINATOR_ADDR"); envAddr != "" {
		coordinatorAddr = envAddr
	}
	if envAddr := os.Getenv("WORKER_ADDR"); envAddr != "" {
		workerAddr = envAddr
	}

	logger := logging.NewSlogLogger(slog.LevelInfo)
	workerID := uuid.New()

	logger.Info("Starting worker", "worker_id", workerID.String())

	client, err := grpc.NewCoordinatorClient(coordinatorAddr, workerID)
	if err != nil {
		logger.Fatal("Failed to create coordinator client", "error", err)
	}
	defer client.Close()

	regCtx, regCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer regCancel()

	availableCPU := uint32(runtime.NumCPU())
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	availableMemory := memStats.Sys

	heartbeatInterval, err := client.RegisterWorker(regCtx, workerAddr, availableCPU, availableMemory)
	if err != nil {
		logger.Fatal("Failed to register worker", "error", err)
	}

	logger.Info("Worker registered successfully",
		"coordinator", coordinatorAddr,
		"worker_id", workerID.String(),
		"cpu_cores", availableCPU,
		"memory_bytes", availableMemory,
		"heartbeat_interval_seconds", heartbeatInterval.Seconds(),
	)

	heartbeatCtx, heartbeatCancel := context.WithCancel(context.Background())
	go client.StartHeartbeat(heartbeatCtx, heartbeatInterval, logger)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	heartbeatCancel()

	logger.Info("Shutting down worker", "worker_id", workerID.String())
}
