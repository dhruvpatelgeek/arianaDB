package coordinator

import (
	"dht/google_protocol_buffer/pb/protobuf"
	"dht/src/constants"
	"dht/src/coordinatorAvailability"
	"dht/src/replication"
	"dht/src/storage"
	"dht/src/structure"
	"dht/src/transport"
	"errors"
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

	//coordinatorAvailability. *CoordinatorAvailability
	coordinatorAvailability          *coordinatorAvailability.CoordinatorAvailability
	hasCompleteInitializedHeadTable  bool
	isPredeccessorPredeccessorFailed bool
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

	var err error
	coordinator.coordinatorAvailability, err = coordinatorAvailability.New()
	if err != nil {
		fmt.Println("[COOD AVAILIBITY ERR]")
	}
	coordinator.hasCompleteInitializedHeadTable = false
	coordinator.isPredeccessorPredeccessorFailed = false

	// bootstrap worker threads for processing incoming messages & gms events
	go coordinator.processIncomingMessages()
	// go coordinator.processGMSEvent()
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

		command := incomingMessage.GetCommand()
		switch constants.InternalMessageCommands(command) {
		case constants.ProcessClientKVRequest:
			coordinator.processClientRequest(incomingMessage, kvRequest)
		case constants.ProcessPropagatedKVRequest:
			coordinator.processPropagatedRequest(incomingMessage, kvRequest)
		}

	}
}

func (coordinator *CoordinatorService) processClientRequest(incomingMessage protobuf.InternalMsg, kvRequest *protobuf.KVRequest) {
	destinationAddress := coordinator.replicationService.GetNextNode(string(kvRequest.Key))
	destinationTable := uint32(constants.Head)
	respondToClient := false

	if selfIP := coordinator.hostIPv4; destinationAddress == selfIP {
		coordinator.toStorageChannel <- incomingMessage
		destinationAddress = coordinator.replicationService.GetNextNode(selfIP)
		destinationTable = uint32(constants.Middle)
	}

	outgoingMessage := incomingMessage
	outgoingMessage.Command = uint32(constants.ProcessPropagatedKVRequest)
	outgoingMessage.DestinationNodeTable = &destinationTable
	outgoingMessage.RespondToClient = &respondToClient

	coordinator.propagateRequest(outgoingMessage, destinationAddress)
}

func (coordinator *CoordinatorService) processPropagatedRequest(incomingMessage protobuf.InternalMsg, kvRequest *protobuf.KVRequest) {
	coordinator.toStorageChannel <- incomingMessage

	currTable := incomingMessage.GetDestinationNodeTable()
	if constants.TableSelection(currTable) != constants.Tail {
		destinationAddress := coordinator.replicationService.GetNextNode(coordinator.hostIPv4)
		destinationTable := uint32(constants.TableSelection(incomingMessage.GetDestinationNodeTable() + 1))
		respondToClient := false

		if destinationTable == uint32(constants.Tail) {
			respondToClient = true
		}
		
		outgoingMessage := incomingMessage
		outgoingMessage.Command = uint32(constants.ProcessPropagatedKVRequest)
		outgoingMessage.DestinationNodeTable = &destinationTable
		outgoingMessage.RespondToClient = &respondToClient

		coordinator.propagateRequest(outgoingMessage, destinationAddress)
	}
}

func (coordinator *CoordinatorService) propagateRequest(outgoingMessage protobuf.InternalMsg, destinationAddress string) {
	marshalledOutgoingMessage, err := proto.Marshal(&outgoingMessage)
	if err != nil {
		fmt.Println("Failed to marshal IncomingMsg in CoordinatorService for forwarding. Ignoring this message.", err)
		return
	}

	coordinator.transport.SendCoordinatorToCoordinator(marshalledOutgoingMessage, []byte("reques"+string(outgoingMessage.MessageID)), destinationAddress)
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////
// Join Request:

// Received message from master to start migrating:
/*	Master will send an InternalMsg which will be received by the Coordinator.
	Coordinator will take appropriate action based on the InternalMsg
*/
func (coordinator *CoordinatorService) processJoinMigrationRequest() error {
	if !coordinator.coordinatorAvailability.CanStartMigrating() {
		return nil
	}

	// distribute keys from local to the newly joined node
	predecessor := coordinator.replicationService.FindPredecessorNode(coordinator.hostIPv4)
	err := coordinator.distributeKeys(predecessor, constants.Head)
	if err != nil {
		return err
		// TODO: consider what we would do if we failed to distributeKeys
		// TODO: catastrophic. send leader a response saying we failed?
	}

	// tell other nodes to replicate their head tables

	/* TODO:
	1. sends InternalMsg to other nodes telling them to replicate their tables (assumes they're all aware of the new node in their gms)
		- TODO: make sure that before a node starts the migration process, the "Leader" tells everyone in the service of the new node.
		- note: for buffered join requests, "Leader" will process buffered request once the migration stuff is complete.
		- TODO: replace Propagating with Migration...
	*/

	// mark coordinator as available to accept new joint node.u
	coordinator.coordinatorAvailability.FinishedDistributeKeys()
	coordinator.hasCompleteInitializedHeadTable = true

	// ask its predecessor to do migrating
	// TODO: create a function for sending a request to replicate its head table
	// TODO: grab the predecessor from the replication service
	// TODO: consider how this will work when there's only 1 node & 2 nodes
	err = coordinator.sendReplicationRequest(predecessor, constants.Head)

	// ask its predecessor's predecessor to do migrating (i.e.: like great-grandfather?)
	// TODO: FindPredecessorNode(referenceNode) returns an error if there is no predecessor
	greatPredecessor := coordinator.replicationService.FindPredecessorNode(predecessor)
	err = coordinator.sendReplicationRequest(greatPredecessor, constants.Head)

	// TODO: come up with a better name scheme
	greatGreatPredecessor := coordinator.replicationService.FindPredecessorNode(greatPredecessor)
	err = coordinator.sendReplicationRequest(greatGreatPredecessor, constants.Head)

	// ask itself to do replicate its head table
	coordinator.replicateTable(constants.Head)

	// TODO: send an acknowledgement to the leader?

	return nil
}

// change the head KV-store of the predecessor and itself:
// TODO: this is for redistributing the local head table to the newly joined node
func (coordinator *CoordinatorService) distributeKeys(predecessor string, tableSelection constants.TableSelection) error {
	lowerBound, upperBound := coordinator.replicationService.GetMigrationRange(predecessor)

	//TODO: call function
	toStorage := protobuf.InternalMsg{
		MessageID:                   structure.GenerateMessageID(),
		Command:                     uint32(constants.ProcessKeyMigrationRequest),
		MigrationRangeLowerbound:    &lowerBound,
		MigrationRangeUpperbound:    &upperBound,
		MigrationDestinationAddress: &(predecessor),
	}
	coordinator.toStorageChannel <- toStorage

	return nil
}

// The following four function will send a propagate request to the corresponding destination,
// and the destination will "propagate" its head table to replication nodes if it has received
// kv-store from its successor. otherwise it'll not propagate.

// This function will be called when migrating head has been finished, this is called from the storage layer.

/*
  TODO:
	 - whenever storage finished divided the kv-store and send it to the new joint node, it will:
	 		- call propagatingMyHead() function to start migrating head.
			- send a message to the new joint node to tell it that its head table is
				complete and can start migrating it to the replication nodes
	 - There is a variable in every node to indicate that whether the head table of this node has been
	 	initialized, if it hasn't been initialized within timeout, it'll send rejoin request to master
	 - Whenever nodes receive the propagatingHead function, it'll first check if its "head completeness"
	 	variable has been changed to true:
		 	- true: start migrating head
			- false: do nothing
*/
// TODO: write some docs
// TODO: come up with a better name for ipv (the guy to whom we send the request)
func (coordinator *CoordinatorService) sendReplicationRequest(destinationIPV4 string, tableSelection constants.TableSelection) error {
	// figured out who is my predecessor:

	return nil
}

func (coordinator *CoordinatorService) replicateTable(constants.TableSelection) error {
	if !coordinator.hasCompleteInitializedHeadTable {
		// if head node hasn't been initialized, then it will not propagate anything
		return nil
	}

	// TODO: send internal message to the storage layer to send the head replication to its successor:
	successor := ""
	err := coordinator.storageService.MigrateTable(successor, constants.Head, constants.Middle)
	if err != nil {
		return err
	}

	// TODO: send internal message to the storage layer to send the head replication to the successor of the current successor
	greatSuccessor := ""

	err = coordinator.storageService.MigrateTable(greatSuccessor, constants.Head, constants.Tail)
	if err != nil {
		return err
	}

	return nil
}

///////////////////////////////////////////////////////////////////////////////////////////
// failure request

func (coordinator *CoordinatorService) processFailMigrationRequest() error {
	// merge local tables
	coordinator.hasCompleteInitializedHeadTable = false
	if !coordinator.isPredeccessorPredeccessorFailed {
		coordinator.combineHeadMiddleTable()
	} else {
		coordinator.combineHeadMiddleTailTable()
	}

	coordinator.isPredeccessorPredeccessorFailed = false
	coordinator.hasCompleteInitializedHeadTable = true

	//
	// ask its predecessor to do migrating
	// TODO: create a function for sending a request to replicate its head table
	// TODO: grab the predecessor from the replication service
	predecessor := coordinator.replicationService.FindPredecessorNode(coordinator.hostIPv4)
	err := coordinator.sendReplicationRequest(predecessor, constants.Head)
	if err != nil {
		return err
	}

	// ask its predecessor's predecessor to do migrating (i.e.: like great-grandfather?)
	greatPredecessor := coordinator.replicationService.FindPredecessorNode(predecessor)
	err = coordinator.sendReplicationRequest(greatPredecessor, constants.Head)
	if err != nil {
		return err
	}

	// TODO:
	coordinator.replicateTable(constants.Head)

	// TODO: send a response to the Leader?
	return nil
}

func (coordinator *CoordinatorService) combineHeadMiddleTable() error {
	err := coordinator.storageService.MergeTables(constants.Head, constants.Middle)
	if err != nil {
		return errors.New("[Coordinator] Failed to merge middle table into head table. Caused by: \n" + err.Error())
	}
	return nil
}

func (coordinator *CoordinatorService) combineHeadMiddleTailTable() error {
	err := coordinator.storageService.MergeTables(constants.Head, constants.Middle)
	if err != nil {
		return errors.New("[Coordinator] Failed to merge middle table into head table. Caused by: \n\t" + err.Error())
	}
	err = coordinator.storageService.MergeTables(constants.Head, constants.Tail)
	if err != nil {
		return errors.New("[Coordinator] Failed to merge tail table into head table. Caused by: \n\t" + err.Error())
	}
	return nil
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
