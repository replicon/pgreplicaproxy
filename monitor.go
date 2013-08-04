package main

import (
	"container/ring"
	"database/sql"
	"log"
	"time"

	_ "github.com/lib/pq"
)

type serverRequest struct {
	responseChannel chan<- *string
}

const (
	StatusUnknown = iota
	StatusDown
	StatusBroken
	StatusMaster
	StatusReplica
)

type serverStatusUpdate struct {
	status  int
	backend string
}

// Maintains the status of backend severs, and allows a client to request a
// replica or master connection.
func serverStatusOracle() {
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
			log.Printf("statusUpdate: master='%v', %v replicas are up", master, replicaServers.Len())
		}
	}
}

// Monitors a single Postgres server and reports changes in status to the
// serverStatusUpdateChannel provided.
func monitorBackend(backend string) {
	first := true
	status := StatusUnknown

	for {
		if !first {
			time.Sleep(time.Second * 5)
		}
		first = false

		db, err := sql.Open("postgres", backend)
		if err != nil {
			if status != StatusDown {
				status = StatusDown
				serverStatusUpdateChannel <- serverStatusUpdate{StatusDown, backend} // I'm  DOWN!
				log.Printf("%v Connection open failed: %v", backend, err)
			}
			continue
		}

		rows, err := db.Query("SELECT pg_is_in_recovery()")
		if err != nil {
			if status != StatusDown {
				status = StatusDown
				serverStatusUpdateChannel <- serverStatusUpdate{StatusDown, backend} // I'm  DOWN!
				log.Printf("%v Query failed: %v", backend, err)
			}
			continue
		}

		var inRecovery bool
		for rows.Next() {
			err = rows.Scan(&inRecovery)
			if err != nil {
				if status != StatusBroken {
					status = StatusBroken
					serverStatusUpdateChannel <- serverStatusUpdate{StatusBroken, backend} // I'm  DOWN!
					log.Printf("%v .Scan() failed: %v", backend, err)
				}
				continue
			}
		}
		err = rows.Err()
		if err != nil {
			if status != StatusBroken {
				status = StatusBroken
				serverStatusUpdateChannel <- serverStatusUpdate{StatusBroken, backend} // I'm  DOWN!
				log.Printf("%v Query rows failed: %v", backend, err)
			}
			continue
		}

		if inRecovery {
			if status != StatusReplica {
				status = StatusReplica
				serverStatusUpdateChannel <- serverStatusUpdate{StatusReplica, backend} // I'm a replica!
				log.Printf("%v I'm a replica!", backend)
			}
		} else {
			if status != StatusMaster {
				status = StatusMaster
				serverStatusUpdateChannel <- serverStatusUpdate{StatusMaster, backend} // I'm the master!
				log.Printf("%v I'm a master", backend)
			}
		}
	}
}
