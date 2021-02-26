# Spirit of Fire
To build the project, run the command: `docker build -t dht .`
To run the project, run the command: `docker run -it -p 3000:3000/udp -v <folder-path-to-servers.txt>:/etc/cpen431 cpen431 ./dht-server 3000 /etc/cpen431/servers.txt`
For example, if servers.txt exists in "/hello/world/server.txt" in the host, then run the command: `docker run -it -p 3000:3000/udp -v /hello/world/server.txt:/etc/cpen431 cpen431 ./dht-server 3000 /etc/cpen431/servers.txt`

### System Overview
TODO: 
### Transport Layer
TODO:
### Storage Service
TODO: 
### Group Membership Service
Spirit of Fire's Group Membership Service (GMS) implements a push-based gossip protocol based on [Lecture 4: Failure Detection and Membership by Indranil Gupta (Indy)](https://courses.engr.illinois.edu/cs425/fa2014/L4.fa14.pdf). 

When a new node **A** joins the system by requesting to join **B**, **A** exchanges its membership list with **B**. After the exchange, each respective nodes merge the contents into their own merged lists.
To let others know that itself is alive, every node periodically (i.e.: every ***T_heartbeat*** ms) sends a heartbeat message to a subset of its membership list. When a node receives a heartbeat messages, it updates a timestamp which tracks the local time at which the receiver received a heartbeat from the sender.
To monitor failures, the node periodically checks for failures and periodically. More specifically, 
1. Every ***T_fail*** ms, the node scans through the membership list, and marks a node as "failed" if a heartbeat message hasn't been received in the window \[T_now-T_fail, T_now\]. 
2. Every ***T_cleanup*** ms, the node scans through the membership list, and removes the node from the membership list who were marked as "failed" by the previous routine.

In this implementation, the number of heartbeat messages at any given round grows with O(n^2). 
Although inefficient when we scale, we implemented this algorithm because the maximum number of nodes is constrained to approximately 50 by milestone 3.

We implemented two timeouts to be more tolerant to packet losses. If we removed nodes when the node detects for failures every ***T_fail*** ms, there is a chance that a heartbeat is not received due to packet loss or because by chance, the sender did not choose to send to this node.
By having a cleanup period ***T_cleanup*** >>> ***T_fail***, we can build more confidence that a node has truly failied. 