package coordinator

import (
	"dht/google_protocol_buffer/pb/protobuf"
	"dht/src/constants"
	"dht/src/replication"
	"dht/src/storage"
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
	toRaftChan              chan protobuf.RaftPayload
	transport_raft          chan protobuf.RaftPayload
	transport               *transport.TransportModule
	replicationService      *replication.ReplicationService
	storageService          *storage.StorageService

	hostIP   string
	hostPort string
	hostIPv4 string
}

func New(
	gmsToCoordinatorChannel chan structure.GMSEventMessage,
	transportToCoordinatorChannel chan protobuf.InternalMsg,
	coordinatorToStorageChannel chan protobuf.InternalMsg,
	coordinatorToReplicationChannel chan structure.GMSEventMessage,
	coordinatorToRaftChan chan protobuf.RaftPayload,
	transportToCoordinatorChannel_RAFT chan protobuf.RaftPayload,

	transport *transport.TransportModule,
	replicationService *replication.ReplicationService,
	storageService *storage.StorageService,

	hostIP string,
	hostPort string) (*CoordinatorService, error) {

	coordinator := new(CoordinatorService)
	coordinator.gmsEventChannel = gmsToCoordinatorChannel
	coordinator.incomingMessagesChannel = transportToCoordinatorChannel
	coordinator.toStorageChannel = coordinatorToStorageChannel
	coordinator.toReplicationChannel = coordinatorToReplicationChannel

	// for the raft protocol------------------------------
	coordinator.transport_raft = transportToCoordinatorChannel_RAFT
	coordinator.toRaftChan = coordinatorToRaftChan

	coordinator.transport = transport
	coordinator.replicationService = replicationService
	coordinator.storageService = storageService

	coordinator.hostIP = hostIP
	coordinator.hostPort = hostPort
	coordinator.hostIPv4 = hostIP + ":" + hostPort

	// bootstrap worker threads for processing incoming messages & gms events
	go coordinator.processIncomingMessages()
	go coordinator.processGMSEvent()
	go coordinator.processRaftMessages()
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
		err := proto.Unmarshal(incomingMessage.KVRequest, kvRequest)
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

			coordinator.transport.SendCoordinatorToCoordinator(marshalledIncomingMessage, []byte("reques"+string(incomingMessage.MessageID)), destination)
		}
	}
}

func (coordinator *CoordinatorService) processGMSEvent() {
	for {
		gmsEventMessage := <-coordinator.gmsEventChannel
		if gmsEventMessage.IsJoined && coordinator.replicationService.IsPredecessor(gmsEventMessage.Node) {
			destination := gmsEventMessage.Node
			lowerbound, upperbound := coordinator.replicationService.GetMigrationRange(gmsEventMessage.Node)

			originatingTable := constants.Head
			destinationTable := constants.Middle
			err := coordinator.storageService.MigratePartialTable(destination, originatingTable, destinationTable, lowerbound, upperbound)
			if err != nil {
				fmt.Printf("[Coordinator] Failed to migrate partial table. Caused by: \n\t%s \n", err.Error())
			}
		}
	}
}

func (coordinator *CoordinatorService) processRaftMessages() {
	// simply forward it to the raft module
	for {
		messageForRaft := protobuf.RaftPayload{}
		select {
		case messageForRaft = <-coordinator.transport_raft:
			coordinator.toRaftChan <- messageForRaft
		}
	}

}
