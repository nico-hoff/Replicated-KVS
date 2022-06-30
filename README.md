# Task3 - Replicated KVS
In this task you will have to implement Raft operations and make replicated
KVS using it. You do not need to implement sharding in this task.

## Task
### 3.1 - Raft normal operations
Implement servers that support Raft's normal operations to replicate an
operation received from a client.

#### Server
Each server listens to two ports: one for communication with a client and the
other for servers. The server process should take the following arguments:

1. `-r [<port1>,<port2>, ...]`: the port numbers listened by the server0, server1, ..., respectively for raft RPCs
2. `-i <server_id>`: the server id (0-origin)
3. `--leader` (optional argument): Add this option if the server is leader at start-up
4. `-p <kvs_port>` : the port number for communication with a kvs client

#### Client
The client should take the following arguments:

1. `-p <port>`: the port number of the server that the client connects to
2. `-o <operation>`: type of operations (GET, PUT, DIRECT_GET).
3. `-k <key>`: key for the operation
4. `-v <value>`: value for the operation

The "DIRECT_GET" operation is special.  If a server receives the DIRECT_GET
operation, the server performs GET operation to its local rocksdb and returns
the value without any RAFT operation. This is for debugging and testing.

The exit code of a client should be
- 0: OK
- 1: Client error
- 2: Server is not a leader
- 3: No key
- 4: Server error

Also, for GET and DIRECT_GET operations, a client should print the GET-ted
value to stdout.

#### RPC interface
Use the following protobufs for RPCs (modify them if necessary).

- [./source/kvs.proto](./source/kvs.proto): KVS RPCs
- [./source/raft.proto](./source/raft.proto): Raft RPCs

#### Example

The following commands starts three servers that constitute a raft cluster.
Each server listens to port 1025, 1026, 1027 respectively for raft RPCs and
the server 0 is the leader at start-up.


```
./build/dev/svr -r 1025,1026,1027 -i 0 -p 2025 --leader
./build/dev/svr -r 1025,1026,1027 -i 1 -p 2026
./build/dev/svr -r 1025,1026,1027 -i 2 -p 2027
```

Then a client can send a request to the leader server like the following.

```
% ./build/dev/clt -p 2025 -o PUT -k 10 -v 100
% ./build/dev/clt -p 2025 -o GET -k 10
100
```

#### Implementation hint
- In the original raft algorithm, a leader election would be conducted first,
  but in this task, the leader can be determined by command line parameters.
- You can assume there are no failures during the test.
- The first index of raft log entries is one in the paper.
- If a follower server (non-leader server) receives a request other than
  DIRECT_GET from a client, it should discard it and return an error.

### 3.2 - Raft leader election
Implement Raft leader election. The server process should take the same
arguments as 3.1.

## Tests
### Test 3.1
Three servers launch. One server is a leader at start-up. The client sends
multiple GET/PUT operations to the leader server. Then check if all servers
have the correct replicated values by sending DIRECT_GET operations to each
server.

### Test 3.2
Three servers launch in the same way as Test 3.1 without the `--leader` argument.
The servers start leader election and choose a leader.

A client finds the leader by sending GET operations to each server. The server
responding without error is the leader.  Then, the client sends multiple
GET/PUT operations to the leader server.  Finally, the client check if all
servers have the correct replicated values by sending DIRECT_GET operations to
each server.

### Test 3.3
Same as Test 3.2, but with 5 servers.

## Going further
Raft paper [2] describes several additional features and optimization methods
that are not covered in this task:

- Cluster configuration changes (Chapter 6)
- Log compaction (Chapter 7)


## References
- [1] The Raft Consensus Algorithm
    - [https://raft.github.io/](https://raft.github.io/)
- [2] In Search of an Understandable Consensus Algorithm (Extended Version)
    - [https://raft.github.io/raft.pdf](https://raft.github.io/raft.pdf)
    - See Figure 2. for the summary of the Raft algorithm
