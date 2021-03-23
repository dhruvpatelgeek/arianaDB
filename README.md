# Spirit of Fire: Distributed Hash Table
Team members: Dhruv Patel, Caleb Sattler, Danny Lee, Patrick Huang

### Instructions
1. To build the project, run the command: `docker build -t dht .`
2. To run the project, run the command: `docker run --network host -p 7262/udp -v /root/mount:/etc/cpen431 dht ./dht-server 7262 /etc/cpen431/servers.txt &`
    - For example, if servers.txt exists in "/root/mount/servers.txt" in the host, then run the command: `docker run --network host -p 7262/udp -v /root/mount:/etc/cpen431 dht ./dht-server 7262 /etc/cpen431/servers.txt &`

### System Overview
![Basic system architecture](images/M1_Arch.png)


Spirit of Fire's distributed hash table (DHT) is composed of 3 components in Milestone 1: the transport layer, the storage component, and the group membership service.

Sitting at the base of the application, the transport layer allows the storage component and the group membership service to communicate with other nodes via UDP while upholding at-most-once semantics via a message cache.

The storage service manages the distributed hash table functionality. When it receives a request from the transport layer either sent by a client or another node, the storage service processes it by:
1. obtaining a list of the current nodes in the system from the group membership service,
2. determining who could be responsible for the request by consistent-hashing.
3. Forward the request to another node if itself is not responsible, or handle the request.
When the client sends to a request to one node, it may receive a response from another if the initial destination was not responsible for the message in the message sample space.

The replication follows a similar replica placement strategy as the [Hibari](http://www.snookles.com/scott/publications/erlang2010-slf.pdf) placement strategy: Each node contains a head, a middle, and a tail table.
As long as there are three or more nodes in the system, each of these tables is a part of a different chain.


| Node 1     | Node 2    | Node 3   |
|------------|-----------|----------|
| **Head1**  | Middle1   | Tail1    |
| Tail2      | **Head2** | Middle2  |
| Middle3    | Tail3     | **Head3**|

Figure: Replica placement strategy

The group membership service maintains a list of all nodes in the system using a [push-based gossip protocol described in Indranil Gupta](https://courses.engr.illinois.edu/cs425/fa2014/L4.fa14.pdf). To summarize, each node sends a heartbeat message to a subset of its membership list every cycle. When a node receives a heartbeat message, it marks the local time at which the sender sent a heartbeat message. Periodically, it runs a fail routine and cleanup routine which marks nodes as failed if no heartbeat was sent.


### Transport Layer
We used the same transport layer from PA2 but we refactored it to take less space (10X less) and we switched from modular to OOP paradigm 
### Storage Service
The storage service module maintains a map based key-value store, and executes consistent hasing for key distribution amongst nodes using SHA256.

### Coordinator Service
CoordinatorService is responsible for responding to external inputs (i.e.: client-to-server requests, server-to-server requests) and internal inputs (GMS events). Client update requests are routed to the head node of the chain, then propagated down its chain to the successor's middle table and the grand-successor's tail table. After the request has been handled at the tail node, the response is sent to the client. When the GMS notifies the coordinator about new nodes or failed nodes, the coordinator will re-distribute the keys in the system to maintain the chain replication's fault-tolerance invariant.

### Group Membership Service
Spirit of Fire's Group Membership Service (GMS) implements a push-based gossip protocol based on [Lecture 4: Failure Detection and Membership by Indranil Gupta (Indy)](https://courses.engr.illinois.edu/cs425/fa2014/L4.fa14.pdf). GMS uses periodic heartbeats to maintain the membership service. It detects node joins by giving the new node a copy of the membership list and letting the new node pulsate to existing nodes. GMS detects node failures when a heartbeat hasn't been received in a threshold period.