package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nemanja-m/gomr/internal/coordinator/api/grpc"
	"github.com/nemanja-m/gomr/internal/coordinator/api/rest"
	"github.com/nemanja-m/gomr/internal/coordinator/service"
	"github.com/nemanja-m/gomr/internal/coordinator/storage"
	"github.com/nemanja-m/gomr/internal/shared/logging"
)

var (
	restServerAddr       = ":8080"
	grpcServerAddr       = ":9090"
	enableGRPCReflection = true
)

func main() {
	if envAddr := os.Getenv("COORDINATOR_REST_ADDR"); envAddr != "" {
		restServerAddr = envAddr
	}
	if envAddr := os.Getenv("COORDINATOR_GRPC_ADDR"); envAddr != "" {
		grpcServerAddr = envAddr
	}
	if envReflection := os.Getenv("COORDINATOR_ENABLE_GRPC_REFLECTION"); envReflection == "false" {
		enableGRPCReflection = false
	}

	logger := logging.NewSlogLogger(slog.LevelInfo)
	jobService := service.NewJobService(storage.NewInMemoryJobStore(), logger)
	restServer := rest.NewServer(restServerAddr, jobService, logger)

	workerService := service.NewWorkerService(storage.NewInMemoryWorkerStore(), logger)
	grpcServer := grpc.NewServer(grpcServerAddr, enableGRPCReflection, workerService, logger)
	healthChecker := service.NewWorkerHealthChecker(workerService, 5*time.Second, 15*time.Second, logger)

	go func() {
		logger.Info("Started REST server", "address", restServerAddr)
		if err := restServer.ListenAndServe(); err != nil && err.Error() != "http: Server closed" {
			logger.Fatal("REST server error", "error", err)
		}
	}()

	go func() {
		logger.Info("Started gRPC server", "address", grpcServerAddr)
		if err := grpcServer.Start(); err != nil {
			logger.Fatal("gRPC Server error", "error", err)
		}
	}()

	heartbeatCtx, heartbeatCancel := context.WithCancel(context.Background())
	go healthChecker.Start(heartbeatCtx)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	heartbeatCancel() // Stop health checker

	logger.Info("Shutting down server")

	// Give server 30 seconds to finish serving ongoing requests
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := restServer.Shutdown(shutdownCtx); err != nil {
		logger.Fatal("Server forced to shutdown", "error", err)
	}

	grpcServer.Stop()

	logger.Info("Server stopped")
}
