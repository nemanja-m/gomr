package main

import (
	"bufio"
	"log"
	"net"
	"time"
)

const (
	workerSessionTimeout = 10 * time.Second
)

type WorkerConn struct {
	Conn            net.Conn
	Address         string
	LastHeartbeatAt time.Time
}

var (
	workers = make(map[string]*WorkerConn)
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	listener, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	go removeInactiveWorkers()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go handleWorkerConnection(conn)
	}
}

func handleWorkerConnection(conn net.Conn) {
	defer conn.Close()

	workerAddr := conn.RemoteAddr().String()
	log.Printf("Worker %s connected\n", workerAddr)

	workers[workerAddr] = &WorkerConn{
		Conn:            conn,
		Address:         workerAddr,
		LastHeartbeatAt: time.Now(),
	}

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		ln := scanner.Text()

		if ln == "heartbeat" {
			conn.Write([]byte("ok\n"))
			log.Printf("Received heartbeat from %s\n", workerAddr)

			worker := workers[workerAddr]
			worker.LastHeartbeatAt = time.Now()
		} else {
			log.Printf("Received: %s\n", ln)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Println(err)
	}

	delete(workers, workerAddr)

	log.Printf("Worker %s disconnected\n", workerAddr)
}

func removeInactiveWorkers() {
	ticker := time.NewTicker(workerSessionTimeout)
	defer ticker.Stop()

	for range ticker.C {
		for addr, worker := range workers {
			if time.Since(worker.LastHeartbeatAt) > workerSessionTimeout {
				log.Printf("Removing inactive worker %s\n", addr)
				worker.Conn.Close()
				delete(workers, addr)
			}
		}
	}
}
