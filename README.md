# Spirit of Fire
To build the project, run the command: `docker build -t dht .`
To run the project, run the command: `docker run -it -p 3000:3000/udp -v <folder-path-to-servers.txt>:/etc/cpen431 cpen431 ./dht-server 3000 /etc/cpen431/servers.txt`
For example, if servers.txt exists in "/hello/world/server.txt" in the host, then run the command: `docker run -it -p 3000:3000/udp -v /hello/world/server.txt:/etc/cpen431 cpen431 ./dht-server 3000 /etc/cpen431/servers.txt`

#### System Overview
TODO: 
#### Transport Layer
TODO:
#### Storage Service
TODO: 
#### Group Membership Service
Spirit of Fire's Group Membership Service (GMS) implements a push-based gossip protocol based on the slide set from . When a new node joins the system 