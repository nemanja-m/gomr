package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nemanja-m/gomr/internal/coordinator/api/rest"
	"github.com/nemanja-m/gomr/internal/coordinator/core"
	"github.com/nemanja-m/gomr/internal/coordinator/storage"
	"github.com/nemanja-m/gomr/internal/shared/logging"
)

func main() {
	addr := ":8080"
	if envAddr := os.Getenv("COORDINATOR_ADDR"); envAddr != "" {
		addr = envAddr
	}

	logger := logging.NewSlogLogger(slog.LevelInfo)
	orchestrator := core.NewJobOrchestrator(storage.NewInMemoryJobStore())
	server := rest.NewServer(addr, orchestrator, logger)

	go func() {
		logger.Info("Starting coordinator API server", "address", addr)
		if err := server.ListenAndServe(); err != nil && err.Error() != "http: Server closed" {
			logger.Fatal("Server error", "error", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down server")

	// Give server 30 seconds to finish serving ongoing requests
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		logger.Fatal("Server forced to shutdown", "error", err)
	}

	logger.Info("Server stopped")
}
