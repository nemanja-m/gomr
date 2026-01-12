package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/nemanja-m/gomr/internal/shared/config"
	"github.com/nemanja-m/gomr/internal/shared/logging"
	"github.com/nemanja-m/gomr/internal/worker/api/grpc"
	"github.com/nemanja-m/gomr/internal/worker/service"
)

func main() {
	configPath := flag.String("config", "", "path to config file")
	flag.Parse()

	cfg, err := config.LoadWorker(*configPath)
	if err != nil {
		slog.Error("Failed to load config", "error", err)
		os.Exit(1)
	}

	logger := logging.NewSlogLogger(slog.LevelInfo)
	workerID := uuid.New()

	client, err := grpc.NewCoordinatorClient(cfg.Coordinator.Addr, cfg.Coordinator.GRPC, workerID)
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

	heartbeatInterval, err := client.RegisterWorker(regCtx, cfg.Server.Addr, availableCPU, availableMemory)
	if err != nil {
		logger.Fatal("Failed to register worker", "error", err)
	}

	executor := service.NewNoopExecutor()
	workerService := service.NewWorkerService(client, executor, heartbeatInterval, logger)

	ctx, cancel := context.WithCancel(context.Background())
	go workerService.Run(ctx)

	logger.Info("Worker started",
		"worker_id", workerID.String(),
		"cpu_cores", availableCPU,
		"memory_bytes", availableMemory,
		"heartbeat", heartbeatInterval.String(),
	)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	cancel()

	logger.Info("Shutting down worker", "worker_id", workerID.String())
}
