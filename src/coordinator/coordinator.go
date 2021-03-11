package coordinator

import (
	"dht/google_protocol_buffer/pb/protobuf"
	"dht/src/replication"
	"dht/src/structure"
	"dht/src/transport"
	"fmt"

	"google.golang.org/protobuf/proto"
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


		TODO:
		1. put,get,remove,wipeout
		2. shutdown, isalive, getprocessid, getMembershipcount (TODO: not storage related)
			- shutdown: coordinator?
			- isAlive: coordinator
			- getProcessID: coordinator
			- getMembershipCount: gms
	*/
	gmsEventChannel         chan structure.GMSEventMessage
	incomingMessagesChannel chan protobuf.InternalMsg
	toStorageChannel        chan protobuf.InternalMsg
	toReplicationChannel    chan structure.GMSEventMessage

	transport          *transport.TransportModule
	replicationService *replication.ReplicationService

	hostIP   string
	hostPort string
	hostIPv4 string
}

func New(
	gmsToCoordinatorChannel chan structure.GMSEventMessage,
	transportToCoordinatorChannel chan protobuf.InternalMsg,
	coordinatorToStorageChannel chan protobuf.InternalMsg,
	coordinatorToReplicationChannel chan structure.GMSEventMessage,

	transport *transport.TransportModule,
	replicationService *replication.ReplicationService,

	hostIP string,
	hostPort string) (*CoordinatorService, error) {

	coordinator := new(CoordinatorService)
	coordinator.gmsEventChannel = gmsToCoordinatorChannel
	coordinator.incomingMessagesChannel = transportToCoordinatorChannel
	coordinator.toStorageChannel = coordinatorToStorageChannel
	coordinator.toReplicationChannel = coordinatorToReplicationChannel

	coordinator.transport = transport
	coordinator.replicationService = replicationService

	coordinator.hostIP = hostIP
	coordinator.hostPort = hostPort
	coordinator.hostIPv4 = hostIP + ":" + hostPort

	// bootstrap worker threads for processing incoming messages & gms events
	go coordinator.processIncomingMessages()
	go coordinator.processGMSEvent()

	return coordinator, nil
}

/**	TODO:
1. get a message from the transport layer
2. get the key from KVRequest by unmarshaling the InternalMsg.payload
3. ask replication service what to do with the message (commit/forward) -> return ip of where to return
4. if ip == self, then
	- send message to storage (InternalMsg)
5. otherwise,
	- re-route to new destination
*/
func (coordinator *CoordinatorService) processIncomingMessages() {
	for {
		// retrieve incoming message
		incomingMessage := <-coordinator.incomingMessagesChannel
		kvRequest := &protobuf.KVRequest{}
		err := proto.Unmarshal(incomingMessage.Payload, kvRequest)
		if err != nil {
			fmt.Println("Failed to unmarshal the KVRequest in incomingMessage.payload in CoordinatorService. Ignoring this message.", err)
			continue
		}

		// ask replicationService which node should handle this message.
		destination := coordinator.replicationService.GetNextNode(kvRequest.Key)

		// process message locally or forward
		if selfIP := coordinator.hostIPv4; destination == selfIP {
			coordinator.toStorageChannel <- incomingMessage
		} else {
			marshalledIncomingMessage, err := proto.Marshal(&incomingMessage)
			if err != nil {
				fmt.Println("Failed to marshal IncomingMsg in CoordinatorService for forwarding. Ignoring this message.", err)
				continue
			}

			coordinator.transport.SendCoordinatorToCoordinator(marshalledIncomingMessage, []byte("reques"+string(incomingMessage.Message)), destination)
		}
	}
}

func (coordinator *CoordinatorService) processGMSEvent() {
	for {
		gmsEventMessage := <-coordinator.gmsEventChannel
		fmt.Println("Coordinator received GMS event: ", gmsEventMessage)
		coordinator.toReplicationChannel <- gmsEventMessage
	}
}
