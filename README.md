pgreplicaproxy is a load-balancing proxy for PostgreSQL connections, intended
to simplify the deployment of one or more read-replica database servers.

This is currently a proof-of-concept application; it is not production ready.

pgreplicaproxy is configured with a list of IP addresses that represent all
the PostgreSQL nodes in your database cluster.  It monitors all the configured
servers to determine whether they are available, and whether they are a master
or a replica.  It then receives client database connections, and based upon
the database name that the client attempts to connect to, pgreplicaproxy will
proxy the connection to the online master server or an online read-replica
server.  If the database name ends in `_replica`, then a replica
connection will be used, and the `_replica` suffix will be removed.

There are a few major issues that prevent pgreplicaproxy from being generally
useful today:

* When a backend server changes state (master -> replica, replica -> master,
  etc.) it should close the current network connections to that server so that
  they're not mistakenly running against the wrong server type. (issue #3)

* Lack of testing under high load and concurrency situations.

Installing
----------

1. Install Go 1.1 for your OS (http://golang.org/doc/install).

2. Set your GOPATH environment variable.  If you're not familiar with Go, this
   is the root of where you'd like to store projects and compiled binaries.

3. Execute `go get github.com/replicon/pgreplicaproxy`.

4. A compiled `pgreplicaproxy` binary should now be in $GOPATH/bin.


Using
-----

1. Copy example.cfg into pgreplicaproxy.cfg, and edit it as desired.

2. Run pgreplicaproxy in the same directory as pgreplicaproxy.cfg.

3. Connect to the host/port provided on the "listen" line of the config file,
   with the appropriate username, password, and database name to use one of
   the `backend` connections from the .cfg file.

4. Try connecting again, and append `_replica` to the database name to
   connect to a replica instead (if one is up and available).

