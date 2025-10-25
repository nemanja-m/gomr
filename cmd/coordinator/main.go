package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nemanja-m/gomr/pkg/distributed/coordinator"
)

func main() {
	addr := ":8080"
	if envAddr := os.Getenv("COORDINATOR_ADDR"); envAddr != "" {
		addr = envAddr
	}

	server := coordinator.NewServer(addr)

	go func() {
		log.Printf("Starting coordinator API server on %s", addr)
		if err := server.ListenAndServe(); err != nil && err.Error() != "http: Server closed" {
			log.Fatalf("Server error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")

	// Give server 30 seconds to finish serving ongoing requests
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server stopped")
}
