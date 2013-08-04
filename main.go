package main

import (
	"code.google.com/p/gcfg"
	"log"
	"net"
)

type config struct {
	Pgreplicaproxy struct {
		Listen  []string
		Backend []string
	}
}

var masterRequestChannel = make(chan serverRequest)
var replicaRequestChannel = make(chan serverRequest)
var serverStatusUpdateChannel = make(chan serverStatusUpdate)
var exitChan = make(chan bool)

func main() {
	cfg := config{}
	err := gcfg.ReadFileInto(&cfg, "pgreplicaproxy.cfg")
	if err != nil {
		log.Fatal(err)
	}

	go serverStatusOracle()
	go manageBackendKeyDataStorage()
	for _, backend := range cfg.Pgreplicaproxy.Backend {
		go monitorBackend(backend)
	}
	for _, listen := range cfg.Pgreplicaproxy.Listen {
		go listenFrontend(listen)
	}

	// Don't finish main()
	<-exitChan
}

func listenFrontend(listen string) {
	ln, err := net.Listen("tcp", listen)
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
