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
	"github.com/nemanja-m/gomr/internal/coordinator/core"
	"github.com/nemanja-m/gomr/internal/coordinator/storage"
	"github.com/nemanja-m/gomr/internal/shared/logging"
)

var (
	restServerAddr = ":8080"
	grpcServerAddr = ":9090"
)

func main() {
	if envAddr := os.Getenv("COORDINATOR_REST_ADDR"); envAddr != "" {
		restServerAddr = envAddr
	}
	if envAddr := os.Getenv("COORDINATOR_GRPC_ADDR"); envAddr != "" {
		grpcServerAddr = envAddr
	}

	logger := logging.NewSlogLogger(slog.LevelInfo)
	orchestrator := core.NewJobController(storage.NewInMemoryJobStore(), logger)
	restServer := rest.NewServer(restServerAddr, orchestrator, logger)
	grpcServer := grpc.NewServer(grpcServerAddr, true, logger)

	go func() {
		logger.Info("Starting coordinator API server", "address", restServerAddr)
		if err := restServer.ListenAndServe(); err != nil && err.Error() != "http: Server closed" {
			logger.Fatal("Server error", "error", err)
		}
	}()

	go func() {
		logger.Info("Starting coordinator gRPC server", "address", grpcServerAddr)
		if err := grpcServer.Start(); err != nil {
			logger.Fatal("gRPC Server error", "error", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down server")

	// Give server 30 seconds to finish serving ongoing requests
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := restServer.Shutdown(ctx); err != nil {
		logger.Fatal("Server forced to shutdown", "error", err)
	}

	grpcServer.Stop()

	logger.Info("Server stopped")
}
