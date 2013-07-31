package main

import (
	"bytes"
	"container/ring"
	"database/sql"
	"encoding/binary"
	_ "github.com/lib/pq"
	"io"
	"log"
	"net"
	"time"
)

type serverRequest struct {
	responseChannel chan<- *string
}

const (
	StatusDown = iota
	StatusBroken
	StatusMaster
	StatusReplica
)

type serverStatusUpdate struct {
	status  int
	backend string
}

func addToRing(r *ring.Ring, s string) *ring.Ring {
	n := ring.New(1)
	n.Value = s
	return n.Link(r)
}

func removeFromRing(ring *ring.Ring, s string) *ring.Ring {
	if ring.Len() == 0 {
		return ring
	}
	var start = ring
	var current = ring
	for {
		if current.Value == s {
			if current.Len() == 1 {
				return nil
			} else {
				current.Move(-1).Unlink(1)
			}
		}
		current = current.Next()
		if current == start {
			break
		}
	}
	return ring
}

func serverStatusOracle(masterRequestChannel <-chan serverRequest, replicaRequestChannel <-chan serverRequest, serverStatusUpdateChannel <-chan serverStatusUpdate) {
	var masterServer *string
	var replicaServers = ring.New(0)

	for {
		select {
		case masterRequest := (<-masterRequestChannel):
			log.Printf("masterRequest: %v", masterRequest)
			masterRequest.responseChannel <- masterServer

		case replicaRequest := (<-replicaRequestChannel):
			log.Printf("replicaRequest: %v", replicaRequest)
			if replicaServers.Len() == 0 {
				replicaRequest.responseChannel <- nil
			} else {
				replicaServers = replicaServers.Next()
				replica := replicaServers.Value.(string)
				replicaRequest.responseChannel <- &replica
			}

		case statusUpdate := (<-serverStatusUpdateChannel):
			if statusUpdate.status == StatusMaster {
				// This is now master
				masterServer = &statusUpdate.backend
				// And it's no longer a replica, if it ever was.
				replicaServers = removeFromRing(replicaServers, statusUpdate.backend)
			} else if statusUpdate.status == StatusReplica {
				// No longer master if it was
				if masterServer != nil && *masterServer == statusUpdate.backend {
					masterServer = nil
				}
				// Make sure backend is only in the ring once by removing first
				replicaServers = removeFromRing(replicaServers, statusUpdate.backend)
				replicaServers = addToRing(replicaServers, statusUpdate.backend)
			} else {
				// No longer master if it was
				if masterServer != nil && *masterServer == statusUpdate.backend {
					masterServer = nil
				}
				// And it's no longer a replica, if it ever was.
				replicaServers = removeFromRing(replicaServers, statusUpdate.backend)
			}

			master := "-none-"
			if masterServer != nil {
				master = *masterServer
			}
			log.Printf("statusUpdate: %v, master='%v', replicas='%v'", statusUpdate, master, replicaServers)
		}
	}
}

func monitorBackend(backend string, serverStatusUpdateChannel chan<- serverStatusUpdate) {
	first := true

	for {
		if !first {
			time.Sleep(time.Minute)
		}
		first = false

		log.Printf("%v Testing connection", backend)

		db, err := sql.Open("postgres", "user=postgres dbname=postgres password=password sslmode=disable "+backend)
		if err != nil {
			serverStatusUpdateChannel <- serverStatusUpdate{StatusDown, backend} // I'm  DOWN!
			log.Printf("%v Connection open failed: %v", backend, err)
			continue
		}

		log.Printf("%v Connection open", backend)

		rows, err := db.Query("SELECT pg_is_in_recovery()")
		if err != nil {
			serverStatusUpdateChannel <- serverStatusUpdate{StatusDown, backend} // I'm DOWN!
			log.Printf("%v Query failed: %v", backend, err)
			continue
		}

		var inRecovery bool
		for rows.Next() {
			err = rows.Scan(&inRecovery)
			if err != nil {
				serverStatusUpdateChannel <- serverStatusUpdate{StatusBroken, backend} // I'm BROKEN?
				log.Printf("%v .Scan() failed: %v", backend, err)
				continue
			}
		}
		err = rows.Err()
		if err != nil {
			serverStatusUpdateChannel <- serverStatusUpdate{StatusBroken, backend} // I'm BROKEN?
			log.Printf("%v Query rows failed: %v", backend, err)
			continue
		}

		if inRecovery {
			serverStatusUpdateChannel <- serverStatusUpdate{StatusReplica, backend} // I'm a replica
			log.Printf("%v I'm a replica!", backend)
		} else {
			serverStatusUpdateChannel <- serverStatusUpdate{StatusMaster, backend} // I'm a master
			log.Printf("%v I'm a master", backend)
		}
	}
}

func main() {

	masterRequestChannel := make(chan serverRequest)
	replicaRequestChannel := make(chan serverRequest)
	serverStatusUpdateChannel := make(chan serverStatusUpdate)
	go serverStatusOracle(masterRequestChannel, replicaRequestChannel, serverStatusUpdateChannel)

	backends := []string{"host=127.0.0.1 port=5432", "host=127.0.0.1 port=5433"}
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
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {

	defer conn.Close()

	// One-minute timeout to read the startup message
	conn.SetReadDeadline(time.Now().Add(time.Minute))

	var startupMessageSize int32
	err := binary.Read(conn, binary.BigEndian, &startupMessageSize)
	if err != nil {
		log.Print(err)
		return
	}

	if startupMessageSize < 0 || startupMessageSize > 8096 {
		log.Printf("Terminating connection that provided an abnormally sized startup message packet")
		return
	}

	log.Printf("startup packet was %v bytes", startupMessageSize)

	startupMessage := make([]byte, startupMessageSize-4)
	_, err = io.ReadFull(conn, startupMessage)
	if err != nil {
		log.Print(err)
		return
	}

	log.Printf("startup packet read")

	var protocolVersionNumber int32
	buf := bytes.NewBuffer(startupMessage)
	err = binary.Read(buf, binary.BigEndian, &protocolVersionNumber)
	if err != nil {
		log.Print(err)
		return
	}

	if protocolVersionNumber == 80877103 {
		// FIXME: this is ugly, but we just repeat all the startup packet reading after saying we won't support SSL
		log.Printf("SSLRequest received; returning N")
		conn.Write([]byte{'N'})
		err = binary.Read(conn, binary.BigEndian, &startupMessageSize)
		if err != nil {
			log.Print(err)
			return
		}
		if startupMessageSize < 0 || startupMessageSize > 8096 {
			log.Printf("Terminating connection that provided an abnormally sized startup message packet")
			return
		}
		log.Printf("startup packet was %v bytes", startupMessageSize)
		startupMessage = make([]byte, startupMessageSize-4)
		_, err = io.ReadFull(conn, startupMessage)
		if err != nil {
			log.Print(err)
			return
		}
		log.Printf("startup packet read")
		buf := bytes.NewBuffer(startupMessage[:4])
		err = binary.Read(buf, binary.BigEndian, &protocolVersionNumber)
		if err != nil {
			log.Print(err)
			return
		}
	}

	if protocolVersionNumber != 196608 {
		log.Printf("Unexpected protocol version number %v, expected 196608", protocolVersionNumber)
		return
	}

	newStartupMessageExcludingSize := &bytes.Buffer{}
	//newStartupMessageExcludingSize.Grow(startupMessageSize)
	binary.Write(newStartupMessageExcludingSize, binary.BigEndian, protocolVersionNumber)

	startupMessage = startupMessage[4:]
	startupParameters := make(map[string]string)
	for {
		nextZero := bytes.IndexByte(startupMessage, 0)
		if nextZero == -1 {
			log.Printf("null terminator not found")
			return
		} else if nextZero == 0 {
			break
		}

		key := string(startupMessage[:nextZero])
		startupMessage = startupMessage[nextZero+1:]

		nextZero = bytes.IndexByte(startupMessage, 0)
		if nextZero == -1 {
			log.Printf("null terminator not found")
			return
		}
		value := string(startupMessage[:nextZero])
		startupMessage = startupMessage[nextZero+1:]

		log.Printf("key = %v, value = %v", key, value)
		startupParameters[key] = value
	}

	dbName, ok := startupParameters["database"]
	if !ok {
		dbName, ok = startupParameters["user"]
		if !ok {
			log.Printf("Expected database or user parameter, neither found")
			return
		}
	}

	if dbName == "tng1_asp_replica" {
		startupParameters["database"] = "tng1_asp"
		log.Printf("Rewriting database name from %v to %v", dbName, startupParameters["database"])
	}

	for key, value := range startupParameters {
		newStartupMessageExcludingSize.Write([]byte(key))
		newStartupMessageExcludingSize.Write([]byte{0})
		newStartupMessageExcludingSize.Write([]byte(value))
		newStartupMessageExcludingSize.Write([]byte{0})
	}
	// Terminating startup packet byte
	newStartupMessageExcludingSize.Write([]byte{0})

	// Reset read deadline to no timeout
	conn.SetReadDeadline(time.Time{})

	upstream, err := net.Dial("tcp", "127.0.0.1:5432")
	if err != nil {
		log.Print(err)
		return
	}

	err = binary.Write(upstream, binary.BigEndian, int32(newStartupMessageExcludingSize.Len()+4))
	if err != nil {
		log.Print(err)
		return
	}

	_, err = upstream.Write(newStartupMessageExcludingSize.Bytes())
	if err != nil {
		log.Print(err)
		return
	}

	log.Printf("Beginning dual Copy invocations")
	go func() {
		numCopied, err := io.Copy(upstream, conn)
		log.Printf("Copy(upstream, conn) -> %v, %v", numCopied, err)
	}()
	numCopied, err := io.Copy(conn, upstream)
	log.Printf("Copy(conn, upstream) -> %v, %v", numCopied, err)
	if err != nil {
		log.Print(err)
		return
	}

	// FIXME: this is probably unreachable?
	log.Printf("Terminating connection")
}
