// +build linux

package main

import (
	"io"
	"log"
	"net"
	"syscall"
)

func connectSockets(conn1, conn2 *net.TCPConn) {

	//	conn1File, err := conn1.File()
	//	if err != nil {
	//		log.Fatal(err)
	//	}
	//	defer conn1File.Close()
	//
	//	conn2File, err := conn2.File()
	//	if err != nil {
	//		log.Fatal(err)
	//	}
	//	defer conn2File.Close()

	go func() {
		/*numCopied, err :=*/ io.Copy(conn1, conn2)
		//spliceCopy(conn1File.Fd(), conn2File.Fd())
	}()

	/*numCopied, err :=*/ io.Copy(conn2, conn1)
	//spliceCopy(conn2File.Fd(), conn1File.Fd())
}

func spliceCopy(source, dest uintptr) {
	pipeFds := make([]int, 2)
	err := syscall.Pipe2(pipeFds, syscall.O_NONBLOCK)
	if err != nil {
		log.Fatal(err)
	}

	for {
		bytesBuffered, err := syscall.Splice(
			int(source), nil,
			pipeFds[1], nil,
			8192,
			//1048576, // /proc/sys/fs/pipe-max-size
			3) // SPLICE_F_NONBLOCK | SPLICE_F_MOVE
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("source -> pipe copied %v bytes", bytesBuffered)

		if bytesBuffered == 0 {
			return
		}

		// var offset int64 = 0
		for bytesBuffered > 0 {
			bytesWritten, err := syscall.Splice(
				pipeFds[0], nil,
				int(dest), nil,
				int(bytesBuffered),
				0) // SPLICE_F_NONBLOCK | SPLICE_F_MOVE
			if err != nil {
				log.Fatal(err)
			}
			if bytesWritten == 0 {
				log.Printf("Splice wrote zero bytes when %v were buffered?", bytesBuffered)
				bytesBuffered -= bytesWritten
				bytesWritten, err = syscall.Splice(
					pipeFds[0], nil,
					int(dest), nil,
					int(bytesBuffered),
					0) // SPLICE_F_NONBLOCK | SPLICE_F_MOVE
				log.Fatalf("One more try, bytesWritten: %v", bytesWritten)
			}
			log.Printf("pipe -> dest copied %v bytes", bytesWritten)
			bytesBuffered -= bytesWritten
			// offset += bytesWritten
		}
	}
}
