package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"path"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

var startupPacketSizeInvalid = errors.New("Terminating connection that provided an abnormally sized startup message packet")
var unsupportedProtocolVersion = errors.New("Unexpected protocol version number; expected 196608")
var incorrectlyFormattedPacket = errors.New("Incorrectly formatted protocol packet")

type startupMessage map[string]string

func sendError(conn net.Conn, errorMessage string) {
	errorMessageExcludingSize := &bytes.Buffer{}
	errorMessageExcludingSize.Grow(1024)

	errorMessageExcludingSize.Write([]byte("S"))
	errorMessageExcludingSize.Write([]byte("ERROR"))
	errorMessageExcludingSize.Write([]byte{0})

	errorMessageExcludingSize.Write([]byte("C"))
	errorMessageExcludingSize.Write([]byte("08000")) // connection exception
	errorMessageExcludingSize.Write([]byte{0})

	errorMessageExcludingSize.Write([]byte("M"))
	errorMessageExcludingSize.Write([]byte(errorMessage))
	errorMessageExcludingSize.Write([]byte{0})

	// Terminating ErrorResponse byte
	errorMessageExcludingSize.Write([]byte{0})

	// Send the error message on the connection.  No error handling here; the connection
	// isn't likely to live for long now anyways. :-)
	conn.Write([]byte{'E'})
	binary.Write(conn, binary.BigEndian, int32(errorMessageExcludingSize.Len()+4))
	conn.Write(errorMessageExcludingSize.Bytes())
}

func readStartupMessage(conn net.Conn) (*startupMessage, error) {
	return readStartupMessageInternal(conn, true)
}

func readStartupMessageInternal(conn net.Conn, allowRecursion bool) (*startupMessage, error) {
	var startupMessageSize int32
	err := binary.Read(conn, binary.BigEndian, &startupMessageSize)
	if err != nil {
		return nil, err
	}

	if startupMessageSize < 0 || startupMessageSize > 8096 {
		sendError(conn, "Startup packet size invalid")
		return nil, startupPacketSizeInvalid
	}

	log.Printf("startup packet was %v bytes", startupMessageSize)

	startupMessageData := make([]byte, startupMessageSize-4)
	_, err = io.ReadFull(conn, startupMessageData)
	if err != nil {
		sendError(conn, "Socket read error")
		return nil, err
	}

	log.Printf("startup packet read")

	var protocolVersionNumber int32
	buf := bytes.NewBuffer(startupMessageData)
	err = binary.Read(buf, binary.BigEndian, &protocolVersionNumber)
	if err != nil {
		sendError(conn, "Socket read error")
		return nil, err
	}

	if protocolVersionNumber == 80877103 && allowRecursion {
		log.Printf("SSLRequest received; returning N")
		conn.Write([]byte{'N'})
		return readStartupMessageInternal(conn, false)
	} else if protocolVersionNumber == 80877102 {
		// CancelRequest message; if possible, match the processId and
		// secretKey to an existing connection and proxy the cancel to
		// the correct backend.

		key := backendKeyDataMessage{}
		err = binary.Read(buf, binary.BigEndian, &key.processId)
		if err != nil {
			return nil, err
		}
		err = binary.Read(buf, binary.BigEndian, &key.secretKey)
		if err != nil {
			return nil, err
		}

		log.Printf("Received CancelRequest, pid=%v, secret=%v", key.processId, key.secretKey)

		backend := getBackendForBackendKeyData(key)
		if backend != nil {
			log.Printf("CancelRequest will be proxied to matching backend, %v", *backend)
			backendConn, err := net.Dial(network(*backend))
			if err == nil {
				binary.Write(backendConn, binary.BigEndian, &startupMessageSize)
				binary.Write(backendConn, binary.BigEndian, &protocolVersionNumber)
				binary.Write(backendConn, binary.BigEndian, &key.processId)
				binary.Write(backendConn, binary.BigEndian, &key.secretKey)
				backendConn.Close()
			}
		}

		return nil, nil
	} else if protocolVersionNumber != 196608 {
		sendError(conn, "Unsupported protocol version")
		return nil, unsupportedProtocolVersion
	}

	startupMessageData = startupMessageData[4:]
	startupParameters := make(startupMessage)
	for {
		nextZero := bytes.IndexByte(startupMessageData, 0)
		if nextZero == -1 {
			sendError(conn, "Malformed startup packet")
			return nil, incorrectlyFormattedPacket
		} else if nextZero == 0 {
			break
		}

		key := string(startupMessageData[:nextZero])
		startupMessageData = startupMessageData[nextZero+1:]

		nextZero = bytes.IndexByte(startupMessageData, 0)
		if nextZero == -1 {
			sendError(conn, "Malformed startup packet")
			return nil, incorrectlyFormattedPacket
		}
		value := string(startupMessageData[:nextZero])
		startupMessageData = startupMessageData[nextZero+1:]

		log.Printf("key = %v, value = %v", key, value)
		startupParameters[key] = value
	}

	return &startupParameters, nil
}

func handleIncomingConnection(conn net.Conn, masterRequestChannel, replicaRequestChannel chan<- serverRequest) {
	defer conn.Close()

	// One-minute timeout to read the startup message
	conn.SetReadDeadline(time.Now().Add(time.Minute))

	startupMessage, err := readStartupMessage(conn)
	if err != nil {
		log.Print(err)
		return
	} else if startupMessage == nil {
		// Occurs in a CancelRequest connection
		return
	}
	startupParameters := *startupMessage

	// Reset read deadline to no timeout
	conn.SetReadDeadline(time.Time{})

	// Check if we're going to connect to a replica or to the master
	dbName, ok := startupParameters["database"]
	wantReplica := false
	if !ok {
		dbName, ok = startupParameters["user"]
		if !ok {
			sendError(conn, "Missing database or user parameter")
			log.Printf("Expected database or user parameter, neither found")
			return
		}
	}
	if strings.HasSuffix(dbName, "_replica") {
		wantReplica = true
		startupParameters["database"] = dbName[:len(dbName)-8]
		log.Printf("Rewriting database name from %v to %v", dbName, startupParameters["database"])
	}

	// Fetch a backend server, either a master or a replica
	responseChannel := make(chan *string)
	request := serverRequest{responseChannel}
	if wantReplica {
		replicaRequestChannel <- request
	} else {
		masterRequestChannel <- request
	}
	backend := <-responseChannel
	if backend == nil {
		sendError(conn, "Unable to find satisfactory backend server")
		log.Println("Unable to find satisfactory backend server")
		return
	}

	// Create the new startup message w/ the possibly different startupParameters
	var protocolVersion int32 = 196608
	newStartupMessageExcludingSize := &bytes.Buffer{}
	newStartupMessageExcludingSize.Grow(1024)
	binary.Write(newStartupMessageExcludingSize, binary.BigEndian, protocolVersion)
	for key, value := range startupParameters {
		newStartupMessageExcludingSize.Write([]byte(key))
		newStartupMessageExcludingSize.Write([]byte{0})
		newStartupMessageExcludingSize.Write([]byte(value))
		newStartupMessageExcludingSize.Write([]byte{0})
	}
	// Terminating startup packet byte
	newStartupMessageExcludingSize.Write([]byte{0})

	// Send the new connection our startup packet
	log.Printf("backend to connect to: %v", *backend)
	upstream, err := net.Dial(network(*backend))
	if err != nil {
		sendError(conn, "Unable to connect to backend server")
		log.Print(err)
		return
	}
	err = binary.Write(upstream, binary.BigEndian, int32(newStartupMessageExcludingSize.Len()+4))
	if err != nil {
		sendError(conn, "Backend network error")
		log.Print(err)
		return
	}
	_, err = upstream.Write(newStartupMessageExcludingSize.Bytes())
	if err != nil {
		sendError(conn, "Backend network error")
		log.Print(err)
		return
	}

	// Begin copying all input from the client to the upstream connection.
	go func() {
		numCopied, err := io.Copy(conn, upstream)
		log.Printf("Copy(conn, upstream) -> %v, %v", numCopied, err)
	}()

	// Proxy upstream -> conn, but attempting to extract the BackendKeyData packet
	backendKeyData, err := proxyPacketsUntilBackendKeyDataReceived(conn, upstream)
	if err != nil {
		sendError(conn, err.Error())
		log.Print(err)
		return
	}

	registerBackendKey(*backendKeyData, *backend)
	defer deregisterBackedKey(*backendKeyData)

	// Stream data between the two network connections
	// Also begin copying all input from the upstream connection to the client.
	numCopied, err := io.Copy(upstream, conn)
	log.Printf("Copy(upstream, conn) -> %v, %v", numCopied, err)
	if err != nil {
		log.Print(err)
		return
	}

	log.Printf("Connection closed softly")
}

// Proxy backend -> client, but attempting to extract the BackendKeyData packet
func proxyPacketsUntilBackendKeyDataReceived(client, backend net.Conn) (*backendKeyDataMessage, error) {

	typeBuffer := make([]byte, 1)
	bufferedClient := bufio.NewWriter(client)
	var messageSize int32

	for {
		_, err := io.ReadFull(backend, typeBuffer)
		if err != nil {
			return nil, err
		}
		_, err = bufferedClient.Write(typeBuffer)
		if err != nil {
			return nil, err
		}

		err = binary.Read(backend, binary.BigEndian, &messageSize)
		if err != nil {
			return nil, err
		} else if messageSize < 0 || messageSize > 8096 {
			return nil, startupPacketSizeInvalid
		}
		err = binary.Write(bufferedClient, binary.BigEndian, &messageSize)
		if err != nil {
			return nil, err
		}

		// BackendKeyData message
		if typeBuffer[0] == 'K' {
			retval := backendKeyDataMessage{}
			err = binary.Read(backend, binary.BigEndian, &retval.processId)
			if err != nil {
				return nil, err
			}
			err = binary.Read(backend, binary.BigEndian, &retval.secretKey)
			if err != nil {
				return nil, err
			}

			err = binary.Write(bufferedClient, binary.BigEndian, &retval.processId)
			if err != nil {
				return nil, err
			}
			err = binary.Write(bufferedClient, binary.BigEndian, &retval.secretKey)
			if err != nil {
				return nil, err
			}

			log.Printf("backendKeyData %v %v", retval.processId, retval.secretKey)

			err = bufferedClient.Flush()
			if err != nil {
				return nil, err
			}

			return &retval, nil
		} else {
			messageBuffer := make([]byte, messageSize-4) // size includes the Int32 containing the size
			_, err = io.ReadFull(backend, messageBuffer)
			if err != nil {
				return nil, err
			}
			_, err = bufferedClient.Write(messageBuffer)
			if err != nil {
				return nil, err
			}

			err = bufferedClient.Flush()
			if err != nil {
				return nil, err
			}
		}
	}
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
	//  u, err := userCurrent()
	//  if err != nil {
	//      return nil, err
	//  } else {
	//      o.Set("user", u)
	//  }
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
