package main

import (
	"log"
	"net"
)

func main() {
	masterRequestChannel := make(chan serverRequest)
	replicaRequestChannel := make(chan serverRequest)
	serverStatusUpdateChannel := make(chan serverStatusUpdate)

	go serverStatusOracle(masterRequestChannel, replicaRequestChannel, serverStatusUpdateChannel)
	go manageBackendKeyDataStorage()

	backends := []string{
		"host=127.0.0.1 port=5432 user=postgres dbname=postgres password=password sslmode=disable",
		"host=127.0.0.1 port=5433 user=postgres dbname=postgres password=password sslmode=disable",
		"host=127.0.0.1 port=5434 user=postgres dbname=postgres password=password sslmode=disable",
		"host=127.0.0.1 port=5435 user=postgres dbname=postgres password=password sslmode=disable",
		"host=127.0.0.1 port=5436 user=postgres dbname=postgres password=password sslmode=disable",
		"host=127.0.0.1 port=5437 user=postgres dbname=postgres password=password sslmode=disable",
		"host=127.0.0.1 port=5438 user=postgres dbname=postgres password=password sslmode=disable",
	}
	for _, backend := range backends {
		go monitorBackend(backend, serverStatusUpdateChannel)
	}

	ln, err := net.Listen("tcp", ":7432")
	if err != nil {
		log.Fatal(err)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go handleIncomingConnection(conn, masterRequestChannel, replicaRequestChannel)
	}
}
