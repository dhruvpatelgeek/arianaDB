package coordinator

import (
	"dht/google_protocol_buffer/pb/protobuf"
	"dht/src/structure"
	"dht/src/transport"
	"fmt"
)

/** TODO: refactor storage and coordinator so that:
1. storage is exclusively for committing to KVStore & responding to client
2. coordinator makes the decision to route to different nodes.

TODO: what is the purpose of the coordinator?
- responsible for the mechanics of the re-routing
	- ask ReplicationService where to process/forward
		- if itself, send to storage
		- if not, re-route to the correct node.

*/
type CoordinatorService struct {
	/** TODO: channels
	1. GMS to Coordinator channel (for node joins and fails) (TODO: to be moved to the replication service)
	2. Transport to Coordinator channel: (aka: receiveChannel inside storage)
		- listens for
	3. Coordinator to Storage channel:
		- this is a channel which sends a message to
			- commit the value
			- respond to the client if necessary
	4. Coordinator to ReplicationService channel:
		- forward to replication service
		- key migration

	TODO: function calls
	1. transport: yes
		- for re-routing to the correct head node in the system (SendCoordinatorToCoordinator)
	2.
	*/
	gmsToCoordinatorChannel         chan structure.GMSEventMessage
	transportToCoordinatorChannel   chan protobuf.InternalMsg
	coordinatorToStorageChannel     chan protobuf.InternalMsg
	coordinatorToReplicationChannel chan int

	transport *transport.TransportModule
}

func New(
	gmsToCoordinatorChannel chan structure.GMSEventMessage,
	transportToCoordinatorChannel chan protobuf.InternalMsg,
	coordinatorToStorageChannel chan protobuf.InternalMsg,
	coordinatorToReplicationChannel chan int,

	transport *transport.TransportModule) (*CoordinatorService, error) {

	coordinator := new(CoordinatorService)
	coordinator.gmsToCoordinatorChannel = gmsToCoordinatorChannel
	coordinator.transportToCoordinatorChannel = transportToCoordinatorChannel
	coordinator.coordinatorToStorageChannel = coordinatorToStorageChannel
	coordinator.coordinatorToReplicationChannel = coordinatorToReplicationChannel

	coordinator.transport = transport

	// TODO: bootstrap worker threads
	go coordinator.processIncomingMessages()
	go coordinator.processGMSEvent()

	return coordinator, nil
}

/** TODO: refactor functionality:
- process incoming messages in the TransportToCoordinator channel
	- by asking ReplicationService, determine whether to commit or forward
		- TODO: note that in the replication propagation, Coordinator may have to commit AND forward
		- commit bool: if true, process here
		- forward bool: if true, forward to next client
- process join/fail events:
	- TODO: this is for later
	- for now, we should just create a process which will dequeue the message off the channel
	- and repeat forever
*/
func (cor *CoordinatorService) processIncomingMessages() {
	for {
		incomingMessage := <-cor.transportToCoordinatorChannel

		fmt.Println(incomingMessage)
	}
}

func (cor *CoordinatorService) processGMSEvent() {
	// TODO: 1. Figure out
	for {
		gmsEventMessage := <-cor.gmsToCoordinatorChannel
		fmt.Println(gmsEventMessage)
	}
}
