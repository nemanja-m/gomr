package main

import (
	"bufio"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	coordinatorAddr   = "localhost:9000"
	heartbeatInterval = 5 * time.Second
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	conn, err := net.Dial("tcp", coordinatorAddr)
	if err != nil {
		log.Fatalf("Failed to connect to coordinator: %v", err)
	}
	defer conn.Close()

	log.Printf("Connected to coordinator at %s", coordinatorAddr)

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	go handle(conn)

	heartbeat(conn, shutdown)
}

func heartbeat(conn net.Conn, shutdown <-chan os.Signal) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if _, err := conn.Write([]byte("heartbeat\n")); err != nil {
				log.Printf("Failed to send heartbeat: %v", err)
				return
			}
		case <-shutdown:
			log.Println("Shutting down ...")
			return
		}
	}
}

func handle(conn net.Conn) {
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Text()
		log.Printf("Received: %s", line)
	}

	if err := scanner.Err(); err != nil {
		log.Println("Connection to coordinator closed")
	}
}
