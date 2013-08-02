pgreplicaproxy is a load-balancing proxy for PostgreSQL connections, intended
to simplify the deployment of one or more read-replica database servers.

This is currently a proof-of-concept application; it is not production ready.

pgreplicaproxy is configured with a list of IP addresses that represent all
the PostgreSQL nodes in your database cluster.  It monitors all the configured
servers to determine whether they are available, and whether they are a master
or a replica.  It then receives client database connections, and based upon
the database name that the client attempts to connect to, pgreplicaproxy will
proxy the connection to the online master server or an online read-replica
server.

There are a few major issues that prevent pgreplicaproxy from being generally
useful today:

* It needs to use a configuration file; those are pretty handy. (issue #1)
* It doesn't support cancelling backend database queries. (issue #2)
* When a backend server changes state (master -> replica, replica -> master,
  etc.) it should close the current network connections to that server so that
  they're not mistakenly running against the wrong server type. (issue #3)
