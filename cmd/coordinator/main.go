package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nemanja-m/gomr/internal/coordinator/api/grpc"
	"github.com/nemanja-m/gomr/internal/coordinator/api/rest"
	"github.com/nemanja-m/gomr/internal/coordinator/service"
	"github.com/nemanja-m/gomr/internal/coordinator/storage"
	"github.com/nemanja-m/gomr/internal/shared/config"
	"github.com/nemanja-m/gomr/internal/shared/logging"
)

func main() {
	configPath := flag.String("config", "", "path to config file")
	flag.Parse()

	cfg, err := config.LoadCoordinator(*configPath)
	if err != nil {
		slog.Error("Failed to load config", "error", err)
		os.Exit(1)
	}

	logger := logging.NewSlogLogger(slog.LevelInfo)
	jobService := service.NewJobService(storage.NewInMemoryJobStore(), logger)
	restServer := rest.NewServer(cfg.REST, jobService, logger)

	workerService := service.NewWorkerService(storage.NewInMemoryWorkerStore(), logger)
	grpcServer := grpc.NewServer(cfg.GRPC, workerService, jobService, logger)
	healthChecker := service.NewWorkerHealthChecker(cfg.Health.CheckInterval, cfg.Health.StaleTimeout, workerService, jobService, logger)

	go func() {
		if err := restServer.ListenAndServe(); err != nil && err.Error() != "http: Server closed" {
			logger.Fatal("REST server error", "error", err)
		}
	}()

	go func() {
		if err := grpcServer.Start(); err != nil {
			logger.Fatal("gRPC Server error", "error", err)
		}
	}()

	heartbeatCtx, heartbeatCancel := context.WithCancel(context.Background())
	go healthChecker.Start(heartbeatCtx)

	logger.Info("Coordinator started",
		"rest_address", cfg.REST.Addr,
		"grpc_address", cfg.GRPC.Addr,
	)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	heartbeatCancel() // Stop health checker

	logger.Info("Shutting down coordinator")

	// Give server 30 seconds to finish serving ongoing requests
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := restServer.Shutdown(shutdownCtx); err != nil {
		logger.Fatal("REST server forced to shutdown", "error", err)
	}

	grpcServer.Stop()

	logger.Info("Coordinator stopped")
}
