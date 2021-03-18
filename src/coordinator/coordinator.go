package coordinator

import (
	"dht/google_protocol_buffer/pb/protobuf"
	"dht/src/constants"
	"strconv"

	"dht/src/membership"

	"dht/src/coordinatorAvailability"

	"dht/src/replication"
	"dht/src/storage"
	"dht/src/structure"
	"dht/src/transport"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
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

	gms *membership.MembershipService

	//coordinatorAvailability. *CoordinatorAvailability
	coordinatorAvailability          *coordinatorAvailability.CoordinatorAvailability
	hasCompleteInitializedHeadTable  bool
	isPredeccessorPredeccessorFailed bool
}

func New(
	transportToCoordinatorChannel chan protobuf.InternalMsg,
	coordinatorToStorageChannel chan protobuf.InternalMsg,
	coordinatorToReplicationChannel chan structure.GMSEventMessage,
	coordinatorToRaftChan chan protobuf.RaftPayload,
	transportToCoordinatorChannel_RAFT chan protobuf.RaftPayload,

	transport *transport.TransportModule,
	replicationService *replication.ReplicationService,
	storageService *storage.StorageService,

	hostIP string,
	hostPort string,

	gms *membership.MembershipService) (*CoordinatorService, error) {

	coordinator := new(CoordinatorService)
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

	coordinator.gms = gms

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

	// start the time out thread
	go coordinator.setTimeOut()

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
	// requestNum := 1
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
		case 69:
			// TODO: so the coordinator is responsible for deleting stuff from gms & adding stuff to gms?
			if *incomingMessage.JoinType == "FAIL" {
				coordinator.gms.DeleteItem(*incomingMessage.FailIP)
				if *incomingMessage.FailOption == constants.GrandSuccessor {
					coordinator.isPredeccessorPredeccessorFailed = true
				} else {
					_ = coordinator.processFailMigrationRequest(*incomingMessage.FailIP)
				}
				fmt.Println("[RECIVED REQUEST]  AS ", *incomingMessage.FailOption, "OF A FAILURE")
			} else {
				_ = coordinator.processJoinMigrationRequest()
				fmt.Println("[RECIVED REQUEST] AS ", *incomingMessage.JoinType)
			}
		case 70:
			if *incomingMessage.JoinType == "FAIL" {
				coordinator.gms.DeleteItem(*incomingMessage.FailIP)
			}
			coordinator.replicateTable(constants.Head)
		case 71:
			coordinator.hasCompleteInitializedHeadTable = true
		case 72:
			coordinator.isPredeccessorPredeccessorFailed = false
		}

	}
}

func (coordinator *CoordinatorService) processClientRequest(incomingMessage protobuf.InternalMsg, kvRequest *protobuf.KVRequest) {
	destinationAddress := coordinator.replicationService.FindSuccessorNode(string(kvRequest.Key))
	destinationTable := uint32(constants.Head)
	respondToClient := false

	if selfIP := coordinator.hostIPv4; destinationAddress == selfIP {
		coordinator.toStorageChannel <- incomingMessage
		destinationAddress = coordinator.replicationService.FindSuccessorNode(selfIP)
		destinationTable = uint32(constants.Middle)
	}

	if kvRequest.GetCommand() == 4 {
		return
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
		destinationAddress := coordinator.replicationService.FindSuccessorNode(coordinator.hostIPv4)
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

// Time out:
func (coordinator *CoordinatorService) setTimeOut() {

	for {
		time.Sleep(7000 * time.Millisecond)
		if coordinator.hasCompleteInitializedHeadTable {
			break
		}

		coordinator.gms.Bootstrap() // TODO:
	}
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
	err := coordinator.distributeKeys(predecessor)
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
	coordinator.sentCompleteInitializedHeadTableToNewNode(predecessor)

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

func (coordinator *CoordinatorService) sentCompleteInitializedHeadTableToNewNode(destinationIPV4 string) error {
	msg := protobuf.InternalMsg{
		MessageID: structure.GenerateMessageID(),

		Command: 71,
	}

	marshalledMsg, error := coordinator.marshalInternalMessage(msg)
	if error != nil {
		return error
	}
	coordinator.transport.ReplicationRequest(marshalledMsg, parsePort(destinationIPV4, 1))

	return nil
}

// change the head KV-store of the predecessor and itself:
// TODO: this is for redistributing the local head table to the newly joined node
func (coordinator *CoordinatorService) distributeKeys(predecessor string) error {
	lowerbound, upperbound := coordinator.replicationService.GetMigrationRange(predecessor)

	err := coordinator.storageService.MigratePartialTable(predecessor, constants.Head, constants.Head, lowerbound, upperbound)
	if err != nil {
		return err
	}

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
	joinType := "JOINED"
	msg := protobuf.InternalMsg{
		MessageID: []byte(uuid.New().String()),

		//NOT SURE IF THERE IS AN ERROR OCCUR HERE:
		Command:  70,
		JoinType: &joinType,
	}

	marshalledMsg, error := coordinator.marshalInternalMessage(msg)
	if error != nil {
		return error
	}
	coordinator.transport.ReplicationRequest(marshalledMsg, parsePort(destinationIPV4, 1))

	return nil
}

func (coordinator *CoordinatorService) sendReplicationRequestAndUpdateMembershipList(failIP string, destinationIPV4 string, tableSelection constants.TableSelection) error {
	// figured out who is my predecessor:
	joinType := "FAIL"
	msg := protobuf.InternalMsg{
		MessageID: []byte(uuid.New().String()),

		//NOT SURE IF THERE IS AN ERROR OCCUR HERE:
		Command:  70,
		JoinType: &joinType,
		FailIP:   &failIP,
	}

	marshalledMsg, error := coordinator.marshalInternalMessage(msg)
	if error != nil {
		return error
	}
	coordinator.transport.ReplicationRequest(marshalledMsg, parsePort(destinationIPV4, 1))

	return nil
}

func parsePort(address string, offset int) string {
	var port_num string
	for i := 0; i < len(address); i++ {
		if address[i] == ':' {
			port_num = address[i+1:]
			casted_port, err := strconv.Atoi(port_num)
			if err != nil {
				fmt.Println("ERROR CASTING")
			}
			casted_port += offset
			new_address := address[:i+1] + strconv.Itoa(casted_port)
			//fmt.Println("NEW ADDRESS->",new_address)
			return new_address
		}
	}
	return "127.0.0.1:3000" // dummy port
}

func (coordinator *CoordinatorService) replicateTable(constants.TableSelection) error {
	if !coordinator.hasCompleteInitializedHeadTable {
		// if head node hasn't been initialized, then it will not propagate anything
		return nil
	}

	// TODO: send internal message to the storage layer to send the head replication to its successor:
	successor := coordinator.replicationService.FindSuccessorNode(coordinator.hostIPv4)
	err := coordinator.storageService.MigrateEntireTable(successor, constants.Head, constants.Middle)
	if err != nil {
		return err
	}

	// TODO: send internal message to the storage layer to send the head replication to the successor of the current successor
	greatSuccessor := coordinator.replicationService.FindSuccessorNode(successor)

	err = coordinator.storageService.MigrateEntireTable(greatSuccessor, constants.Head, constants.Tail)
	if err != nil {
		return err
	}

	return nil
}

///////////////////////////////////////////////////////////////////////////////////////////
// failure request

func (coordinator *CoordinatorService) processFailMigrationRequest(failIP string) error {
	// merge local tables
	coordinator.hasCompleteInitializedHeadTable = false
	if !coordinator.isPredeccessorPredeccessorFailed {
		coordinator.combineHeadMiddleTable()
	} else {
		coordinator.combineHeadMiddleTailTable()
	}

	coordinator.isPredeccessorPredeccessorFailed = false
	coordinator.hasCompleteInitializedHeadTable = true
	// TODO: send an internal message to its successor saying command 71
	successor := coordinator.replicationService.FindSuccessorNode(coordinator.hostIPv4)
	internalMsg := &protobuf.InternalMsg{
		MessageID: []byte(uuid.New().String()),
		Command:   72,
	}
	marshalledMsg, error := coordinator.marshalInternalMessage(*internalMsg)
	if error != nil {
		return error
	}
	coordinator.transport.ReplicationRequest(marshalledMsg, parsePort(successor, 1))

	//
	// ask its predecessor to do migrating
	// TODO: create a function for sending a request to replicate its head table
	// TODO: grab the predecessor from the replication service
	predecessor := coordinator.replicationService.FindPredecessorNode(coordinator.hostIPv4)
	err := coordinator.sendReplicationRequestAndUpdateMembershipList(failIP, predecessor, constants.Head)
	if err != nil {
		return err
	}

	// ask its predecessor's predecessor to do migrating (i.e.: like great-grandfather?)
	greatPredecessor := coordinator.replicationService.FindPredecessorNode(predecessor)
	err = coordinator.sendReplicationRequestAndUpdateMembershipList(failIP, greatPredecessor, constants.Head)
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

func (coordinator *CoordinatorService) marshalInternalMessage(msg protobuf.InternalMsg) ([]byte, error) {
	byteMsg, err := proto.Marshal(&msg)
	if err != nil {
		return nil, err
	}
	return byteMsg, nil
}
