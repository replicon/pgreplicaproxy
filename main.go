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
	"os"
	"path"
	"strings"
	"time"
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

func addToRing(r *ring.Ring, s string) *ring.Ring {
	n := ring.New(1)
	n.Value = s
	return n.Link(r)
}

func removeFromRing(r *ring.Ring, s string) *ring.Ring {
	newRing := ring.New(0)
	r.Do(func(v interface{}) {
		if v != s {
			newRing = addToRing(newRing, v.(string))
		}
	})
	return newRing
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
			log.Printf("statusUpdate: master='%v', %v replicas are up", master, replicaServers.Len())
		}
	}
}

func monitorBackend(backend string, serverStatusUpdateChannel chan<- serverStatusUpdate) {
	first := true
	status := StatusUnknown

	for {
		if !first {
			time.Sleep(time.Second * 5)
		}
		first = false

		db, err := sql.Open("postgres", "user=postgres dbname=postgres password=password sslmode=disable "+backend)
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

func main() {

	masterRequestChannel := make(chan serverRequest)
	replicaRequestChannel := make(chan serverRequest)
	serverStatusUpdateChannel := make(chan serverStatusUpdate)
	go serverStatusOracle(masterRequestChannel, replicaRequestChannel, serverStatusUpdateChannel)

	backends := []string{
		"host=127.0.0.1 port=5432",
		"host=127.0.0.1 port=5433",
		"host=127.0.0.1 port=5434",
		"host=127.0.0.1 port=5435",
		"host=127.0.0.1 port=5436",
		"host=127.0.0.1 port=5437",
		"host=127.0.0.1 port=5438",
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
		go handleConnection(conn, masterRequestChannel, replicaRequestChannel)
	}
}

func handleConnection(conn net.Conn, masterRequestChannel, replicaRequestChannel chan<- serverRequest) {

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
	newStartupMessageExcludingSize.Grow(int(startupMessageSize))
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

	wantReplica := false

	if strings.HasSuffix(dbName, "_replica") {
		wantReplica = true
		startupParameters["database"] = dbName[:len(dbName)-8]
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

	responseChannel := make(chan *string)
	request := serverRequest{responseChannel}
	if wantReplica {
		replicaRequestChannel <- request
	} else {
		masterRequestChannel <- request
	}
	backend := <-responseChannel

	if backend == nil {
		// FIXME: it'd be nice to return an error message to the client for some of these failures...
		log.Println("Unable to find satisfactory backend server")
		return
	}

	log.Printf("backend to connect to: %v", *backend)
	upstream, err := net.Dial(network(*backend))
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

	log.Printf("Connection closed softly")
}

//// BEGIN: Copy/hacked from lib/pq
func network(name string) (string, string) {
	o := make(Values)

	// A number of defaults are applied here, in this order:
	//
	// * Very low precedence defaults applied in every situation
	// * Environment variables
	// * Explicitly passed connection information
	o.Set("host", "localhost")
	o.Set("port", "5432")

	for k, v := range parseEnviron(os.Environ()) {
		o.Set(k, v)
	}

	parseOpts(name, o)

	// Don't care about user, just host & port
	//// If a user is not provided by any other means, the last
	//// resort is to use the current operating system provided user
	//// name.
	//if o.Get("user") == "" {
	//	u, err := userCurrent()
	//	if err != nil {
	//		return nil, err
	//	} else {
	//		o.Set("user", u)
	//	}
	//}

	host := o.Get("host")

	if strings.HasPrefix(host, "/") {
		sockPath := path.Join(host, ".s.PGSQL."+o.Get("port"))
		return "unix", sockPath
	}

	return "tcp", host + ":" + o.Get("port")
}

type Values map[string]string

func (vs Values) Set(k, v string) {
	vs[k] = v
}

func (vs Values) Get(k string) (v string) {
	return vs[k]
}

func parseOpts(name string, o Values) {
	if len(name) == 0 {
		return
	}

	name = strings.TrimSpace(name)

	ps := strings.Split(name, " ")
	for _, p := range ps {
		kv := strings.Split(p, "=")
		if len(kv) < 2 {
			log.Fatalf("invalid option: %q", p)
		}
		o.Set(kv[0], kv[1])
	}
}

// parseEnviron tries to mimic some of libpq's environment handling
//
// To ease testing, it does not directly reference os.Environ, but is
// designed to accept its output.
//
// Environment-set connection information is intended to have a higher
// precedence than a library default but lower than any explicitly
// passed information (such as in the URL or connection string).
func parseEnviron(env []string) (out map[string]string) {
	out = make(map[string]string)

	for _, v := range env {
		parts := strings.SplitN(v, "=", 2)

		accrue := func(keyname string) {
			out[keyname] = parts[1]
		}

		// The order of these is the same as is seen in the
		// PostgreSQL 9.1 manual, with omissions briefly
		// noted.
		switch parts[0] {
		case "PGHOST":
			accrue("host")
		case "PGHOSTADDR":
			accrue("hostaddr")
		case "PGPORT":
			accrue("port")
		case "PGDATABASE":
			accrue("dbname")
		case "PGUSER":
			accrue("user")
		case "PGPASSWORD":
			accrue("password")
		// skip PGPASSFILE, PGSERVICE, PGSERVICEFILE,
		// PGREALM
		case "PGOPTIONS":
			accrue("options")
		case "PGAPPNAME":
			accrue("application_name")
		case "PGSSLMODE":
			accrue("sslmode")
		case "PGREQUIRESSL":
			accrue("requiressl")
		case "PGSSLCERT":
			accrue("sslcert")
		case "PGSSLKEY":
			accrue("sslkey")
		case "PGSSLROOTCERT":
			accrue("sslrootcert")
		case "PGSSLCRL":
			accrue("sslcrl")
		case "PGREQUIREPEER":
			accrue("requirepeer")
		case "PGKRBSRVNAME":
			accrue("krbsrvname")
		case "PGGSSLIB":
			accrue("gsslib")
		case "PGCONNECT_TIMEOUT":
			accrue("connect_timeout")
		case "PGCLIENTENCODING":
			accrue("client_encoding")
			// skip PGDATESTYLE, PGTZ, PGGEQO, PGSYSCONFDIR,
			// PGLOCALEDIR
		}
	}

	return out
}

//// END: Copy/hacked from lib/pq
