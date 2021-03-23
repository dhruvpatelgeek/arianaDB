package coordinator

import (
	"dht/google_protocol_buffer/pb/protobuf"
	"dht/src/constants"
	"errors"

	"dht/src/membership"
	"dht/src/replication"
	"dht/src/storage"
	"dht/src/transport"
	"fmt"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

// Storage Command numbers
const PUT = 1
const GET = 2
const REMOVE = 3
const SHUTDOWN = 4
const WIPEOUT = 5
const IS_ALIVE = 6
const GET_PID = 7
const GET_MC = 8

/** CoordinatorService is responsible for responding to external inputs (i.e.: client-to-server requests,
server-to-server requests) and internal inputs (gms events).

TODO: add some comments on how forwarding and propagation works.

TODO: add some comments on how node joins & fails are handled.
*/
type CoordinatorService struct {
	gmsEventChannel         chan membership.GMSEventMessage
	incomingMessagesChannel chan protobuf.InternalMsg
	toStorageChannel        chan protobuf.InternalMsg
	transport               *transport.TransportModule
	replicationService      *replication.ReplicationService
	storageService          *storage.StorageService

	hostname string
	hostport string
	hostIPv4 string

	gms *membership.MembershipService
}

func New(
	gmsEventChannel chan membership.GMSEventMessage,
	transportToCoordinatorChannel chan protobuf.InternalMsg,
	coordinatorToStorageChannel chan protobuf.InternalMsg,

	transport *transport.TransportModule,
	replicationService *replication.ReplicationService,
	storageService *storage.StorageService,

	hostIP string,
	hostPort string,

	gms *membership.MembershipService) (*CoordinatorService, error) {

	coordinator := new(CoordinatorService)
	coordinator.gmsEventChannel = gmsEventChannel
	coordinator.incomingMessagesChannel = transportToCoordinatorChannel
	coordinator.toStorageChannel = coordinatorToStorageChannel

	coordinator.transport = transport
	coordinator.replicationService = replicationService
	coordinator.storageService = storageService

	coordinator.hostname = hostIP
	coordinator.hostport = hostPort
	coordinator.hostIPv4 = hostIP + ":" + hostPort

	coordinator.gms = gms

	// bootstrap worker threads for processing incoming messages & gms events
	go coordinator.processIncomingMessages()
	go coordinator.processGMSEvent()

	return coordinator, nil
}

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

		case constants.ProcessMigratingHeadTableRequest:
			middleTableDestination := incomingMessage.GetReplicateMiddleTableDestination()
			tailTableDestination := incomingMessage.GetReplicateTailTableDestination()
			fmt.Printf("[Coordinator] Received a command from another node to replicate my head table to %s's middle table and %s's tail table.\n",
				middleTableDestination, tailTableDestination)
			go coordinator.replicateTable(constants.Head, middleTableDestination, tailTableDestination)

		default:
			fmt.Printf("[Coordinator] [Warning] Received an unrecognized command. \n")
			fmt.Printf("[Coordinator] [Warning] Supported commands are ProcessClientKVRequest (%d)\n", constants.ProcessClientKVRequest)
			fmt.Printf("[Coordinator] [Warning] Supported commands are ProcessPropagatedKVRequest (%d)\n", constants.ProcessPropagatedKVRequest)
			fmt.Printf("[Coordinator] [Warning] Supported commands are ProcessMigratingHeadTableRequest (%d)\n", constants.ProcessMigratingHeadTableRequest)
		}
	}
}

func (coordinator *CoordinatorService) processClientRequest(incomingMessage protobuf.InternalMsg, kvRequest *protobuf.KVRequest) {
	destinationAddress := coordinator.replicationService.FindSuccessorNode(string(kvRequest.Key))
	destinationTable := uint32(constants.Head)
	respondToClient := false

	storageCommand := kvRequest.GetCommand()
	if selfIP := coordinator.hostIPv4; destinationAddress == selfIP {
		if storageCommand == GET {
			sendGetResponseToClient := true
			incomingMessage.RespondToClient = &sendGetResponseToClient
			coordinator.toStorageChannel <- incomingMessage
			return
		}

		coordinator.toStorageChannel <- incomingMessage
		destinationAddress = coordinator.replicationService.FindSuccessorNode(selfIP)
		destinationTable = uint32(constants.Middle)
	}

	// Don't propagate shutdowns
	if storageCommand == SHUTDOWN {
		return
	}

	if destinationTable == uint32(constants.Head) && storageCommand == GET {
		respondToClient = true
	}

	outgoingMessage := incomingMessage
	outgoingMessage.Command = uint32(constants.ProcessPropagatedKVRequest)
	outgoingMessage.DestinationNodeTable = &destinationTable
	outgoingMessage.RespondToClient = &respondToClient

	coordinator.propagateRequest(outgoingMessage, destinationAddress)
}

func (coordinator *CoordinatorService) processPropagatedRequest(incomingMessage protobuf.InternalMsg, kvRequest *protobuf.KVRequest) {
	coordinator.toStorageChannel <- incomingMessage

	if kvRequest.GetCommand() != GET {
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
}

func (coordinator *CoordinatorService) propagateRequest(outgoingMessage protobuf.InternalMsg, destinationAddress string) {
	marshalledOutgoingMessage, err := proto.Marshal(&outgoingMessage)
	if err != nil {
		fmt.Println("Failed to marshal IncomingMsg in CoordinatorService for forwarding. Ignoring this message.", err)
		return
	}

	coordinator.transport.SendCoordinatorToCoordinator(marshalledOutgoingMessage, []byte("reques"+string(outgoingMessage.MessageID)), destinationAddress)
}

func (coordinator *CoordinatorService) processGMSEvent() {
	for {
		gmsEvent := <-coordinator.gmsEventChannel
		eventType := gmsEvent.EventType

		switch eventType { // TODO: define gmsEvent in a constant
		case membership.Joined:
			err := coordinator.processJoinEvent(gmsEvent)
			if err != nil {
				fmt.Printf("[Coordinator] [Error] failed to process GMS join event for %v Error: %s\n", gmsEvent.Nodes, err.Error())
			}
		case membership.Failed:
			err := coordinator.processFailEvent(gmsEvent)
			if err != nil {
				fmt.Printf("[Coordinator] [Error] failed to process GMS fail event for %v Error: %s\n", gmsEvent.Nodes, err.Error())
			}
		}
	}
}

/**	In response to a GMS join event, processJoinEvent() will take action based on its heritage relative to the new node.
1. If we are the successor of the new node, we:
	a. split our head table and migrate a subset of the keys to the new node,
	b. replicate our head table to our successor's middle table and great-successor's tail table
2. If we are a predecessor or greatpredecessor of the new node, we rreplicate our head table to our successor's middle table and great-successor's tail table
3. Otherwise, we ignore the join event.
*/
func (self *CoordinatorService) processJoinEvent(gmsEvent membership.GMSEventMessage) error {
	numNewNodes := len(gmsEvent.Nodes)
	if numNewNodes != 1 {
		return fmt.Errorf("[Coordinator] [Error] Expected join event message from GMS to contain exactly 1 new node. This join event message contains the new nodes: %v", gmsEvent.Nodes)
	}
	newNode := gmsEvent.Nodes[0]

	// get self's heritage in relation to the new node
	successor := self.replicationService.FindSuccessorNode(newNode) // TODO: for cleanliness later, consider using constants if self can only be one of the following
	predecessor := self.replicationService.FindPredecessorNode(newNode)
	greatPredecessor := self.replicationService.FindPredecessorNode(predecessor)

	isSuccessor := self.hostIPv4 == successor
	isPredecessor := self.hostIPv4 == predecessor
	isGreatPredecessor := self.hostIPv4 == greatPredecessor
	fmt.Printf("[Coordinator] Join event received. Self-heritage isSuccessor: %t, isPredecessor: %t, isGreatPredecessor: %t \n", isSuccessor, isPredecessor, isGreatPredecessor)

	if isSuccessor {
		fmt.Printf("[Coordinator] New node (%s) joined. We are the successor of the new node. \n", newNode)
		err := self.processJointReqSuccessor(newNode)
		if err != nil {
			return err
		}
	} else if isPredecessor {
		fmt.Printf("[Coordinator] New node (%s) joined. We are the predecessor of the new node. \n", newNode)
		err := self.processJointReqPredecessor(newNode)
		if err != nil {
			return err
		}
	} else if isGreatPredecessor {
		fmt.Printf("[Coordinator] New node (%s) joined. We are the greatPredecessor of the new node. \n", newNode)
		err := self.processJointReqGreatPredecessor(newNode)
		if err != nil {
			return err
		}
	} else {
		fmt.Printf("[Coordinator] [Info] Self is neither a successor, predecessor, nor greatPredecessor of the new node: %s. Ignoring this event.\n", newNode)
		return nil
	}
	return nil
}

// if it's the successor of the new joint node:
func (coordinator *CoordinatorService) processJointReqSuccessor(newNodeIP string) error {
	// split our head table's keys with the new node.
	err := coordinator.distributeKeys(newNodeIP)
	if err != nil {
		return err
	}

	// send a replication request to the new node to replicate its head
	myself := coordinator.hostIPv4
	mySuccessor := coordinator.replicationService.FindSuccessorNode(myself)
	err = coordinator.sendReplicationRequest(newNodeIP, myself, mySuccessor)
	if err != nil {
		fmt.Printf("[Coordinator] [Warning] failed to tell the new node %s to replicate its head table.\n", newNodeIP)
	}

	successor := coordinator.replicationService.FindSuccessorNode(coordinator.hostIPv4)
	err = coordinator.storageService.MigrateEntireTable(successor, constants.Head, constants.Middle)
	if err != nil {
		return err
	}

	grandSuccessor := coordinator.replicationService.FindSuccessorNode(successor)
	err = coordinator.storageService.MigrateEntireTable(grandSuccessor, constants.Head, constants.Tail)
	return err
}

func (coordinator *CoordinatorService) processJointReqPredecessor(newNodeIP string) error {
	// replicate prdecessor of the new node's head table to new node's middle table.
	err := coordinator.storageService.MigrateEntireTable(newNodeIP, constants.Head, constants.Middle)
	if err != nil {
		return err
	}

	// replicate prdecessor of the new node's head table to new node's successor's middle table.
	successor := coordinator.replicationService.FindSuccessorNode(newNodeIP)
	err = coordinator.storageService.MigrateEntireTable(successor, constants.Head, constants.Tail)
	if err != nil {
		return err
	}

	return nil
}

func (coordinator *CoordinatorService) processJointReqGreatPredecessor(newNodeIP string) error {
	// replicate prdecessor of the new node's head table to new node's successor's middle table.
	err := coordinator.storageService.MigrateEntireTable(newNodeIP, constants.Head, constants.Tail)
	if err != nil {
		return err
	}

	return nil
}

// migrates a subset of the head table's keys who belong to the new node
func (coordinator *CoordinatorService) distributeKeys(predecessor string) error {
	lowerbound, upperbound := coordinator.replicationService.GetMigrationRange(predecessor)

	err := coordinator.storageService.MigratePartialTable(predecessor, constants.Head, constants.Head, lowerbound, upperbound)
	if err != nil {
		return err
	}

	return nil
}

func (coordinator *CoordinatorService) sendReplicationRequest(newNodeIP string, replicateMiddleTableDestination string, replicateTailTableDestination string) error {
	msg := protobuf.InternalMsg{
		MessageID:                       []byte(uuid.New().String()),
		Command:                         uint32(constants.ProcessMigratingHeadTableRequest),
		ReplicateMiddleTableDestination: &replicateMiddleTableDestination, // destination of the middle table
		ReplicateTailTableDestination:   &replicateTailTableDestination,   // destination of the tail table
	}

	marshalledMsg, err := coordinator.marshalInternalMessage(msg)
	if err != nil {
		return err
	}
	err = coordinator.transport.ReplicationRequest(marshalledMsg, newNodeIP)
	return err
}

func (coordinator *CoordinatorService) replicateTable(originTableToReplicate constants.TableSelection, middleTableDestination string, tailTableDestination string) {
	err := coordinator.storageService.MigrateEntireTable(middleTableDestination, constants.Head, constants.Middle)
	if err != nil {
		fmt.Printf("[Coordinator] Failed to replicate head table to destination's (%s) middle table. Caused by: %s \n", middleTableDestination, err.Error())
		return
	}

	err = coordinator.storageService.MigrateEntireTable(tailTableDestination, constants.Head, constants.Tail)
	if err != nil {
		fmt.Printf("[Coordinator] Failed to replicate head table to destination's (%s) middle table. Caused by: %s \n", middleTableDestination, err.Error())
	}
	return
}

/**	TODO: write some docs
 */
func (self *CoordinatorService) processFailEvent(gmsEvent membership.GMSEventMessage) error {
	// merge local tables TODO:

	numNewNodes := len(gmsEvent.Nodes)
	if numNewNodes <= 0 {
		return fmt.Errorf("[Coordinator] [Error] Expected fail event message from GMS to contain at least 1 failed node. This failed event message contains the failed nodes: %v", gmsEvent.Nodes)
	}
	failedNodes := gmsEvent.Nodes

	/* TODO: what should we do when we have multiple failures (not just 2, but more)
	- regardless of how many fail,
		- for the successor,
			- if it detects that its predecessor + great predecessor failed, then merge head+middle+tail
			- else if it detects that its predecessor failed & the great predecessor did not fail, merge head + middle
			- else (neither predecessor + greatpredecessor failed), then do no merging

		- TODO: how to fix the race condition:
			- successor tells the predecessor where to replicate its head table to
			- successor tells the greatPredecessor where to replicate its head table to
	*/
	// check self's heritage in relation to the new node
	isSuccessor := self.isSuccessorOfAnyFailNode(failedNodes)
	isPredecessor := self.isPredecessorOfAnyFailNode(failedNodes)
	isGreatPredecessor := self.isGreatPredecessorOfAnyFailNode(failedNodes)
	fmt.Printf("[Coordinator] [Debug] Fail event: isSuccessor: %t, isPredecessor: %t, isGreatPredecessor: %t \n", isSuccessor, isPredecessor, isGreatPredecessor)

	if isSuccessor {
		fmt.Printf("[Coordinator] Nodes (%v) failed. We are the successor of a failed node. \n", failedNodes)
		err := self.processFailReqSuccessor(failedNodes)
		if err != nil {
			return err
		}
	} else {
		fmt.Printf("[Coordinator] [Info] Self is neither a successor, predecessor, nor greatPredecessor of the failed nodes: %v. Ignoring this event.\n", failedNodes)
		return nil
	}
	return nil

}

/** isSuccessor(failedNodes) returns true if I am the successor of any of any of the failed nodes
 */
func (self *CoordinatorService) isSuccessorOfAnyFailNode(failedNodes []string) bool {
	for _, failedNode := range failedNodes {
		if self.hostIPv4 == self.replicationService.FindSuccessorNode(failedNode) {
			return true
		}
	}

	return false
}

func (self *CoordinatorService) isPredecessorOfAnyFailNode(failedNodes []string) bool {

	for _, failedNode := range failedNodes {
		if self.hostIPv4 == self.replicationService.FindPredecessorNode(failedNode) {
			return true
		}
	}

	return false
}

func (self *CoordinatorService) isGreatPredecessorOfAnyFailNode(failedNodes []string) bool {

	for _, failedNode := range failedNodes {
		if self.hostIPv4 == self.replicationService.FindPredecessorNode(self.replicationService.FindPredecessorNode(failedNode)) {
			return true
		}
	}

	return false
}

func (coordinator *CoordinatorService) processFailReqGreatPredecessor() error {
	successor := coordinator.replicationService.FindSuccessorNode(coordinator.hostIPv4)
	greatSuccessor := coordinator.replicationService.FindSuccessorNode(successor)
	err := coordinator.storageService.MigrateEntireTable(greatSuccessor, constants.Head, constants.Tail)
	if err != nil {
		return err
	}
	return nil
}

func (coordinator *CoordinatorService) processFailReqPredecessor() error {
	successor := coordinator.replicationService.FindSuccessorNode(coordinator.hostIPv4)
	err := coordinator.storageService.MigrateEntireTable(successor, constants.Head, constants.Middle)
	if err != nil {
		return err
	}

	greatSuccessor := coordinator.replicationService.FindSuccessorNode(successor)
	err = coordinator.storageService.MigrateEntireTable(greatSuccessor, constants.Head, constants.Tail)
	if err != nil {
		return err
	}
	return nil
}

func (coordinator *CoordinatorService) processFailReqSuccessor(failedNodes []string) error {
	// if we are the successor, merge the head table with other tables
	numFailedNodesBetweenSelfAndNextAlivePredecessor := coordinator.replicationService.GetNumFailedNodesBetweenSelfAndNextAlivePredecessor(failedNodes)
	if numFailedNodesBetweenSelfAndNextAlivePredecessor >= 2 {
		err := coordinator.combineHeadMiddleTailTable()
		if err != nil {
			return err
		}
	} else if numFailedNodesBetweenSelfAndNextAlivePredecessor == 1 {
		err := coordinator.combineHeadMiddleTable()
		if err != nil {
			return err
		}
	}

	// TODO: send internal message to the predecessor and the great predecessor where to migrate its head table to
	myPredecessor := coordinator.replicationService.FindPredecessorNode(coordinator.hostIPv4)
	mySelf := coordinator.hostIPv4
	mySuccessor := coordinator.replicationService.FindSuccessorNode(mySelf)
	myGreatSuccessor := coordinator.replicationService.FindSuccessorNode(mySuccessor)

	myGreatPredecessor := coordinator.replicationService.FindPredecessorNode(myPredecessor)

	err := coordinator.sendReplicationRequest(myPredecessor, mySelf, mySuccessor)
	if err != nil {
		fmt.Printf("[Coordinator] [Error] Unable to instruct my predecessor (%s) to replicate its head table while processing fail event.\n", myPredecessor)
		return err
	}

	err = coordinator.sendReplicationRequest(myGreatPredecessor, myPredecessor, mySelf)
	if err != nil {
		fmt.Printf("[Coordinator] [Error] Unable to instruct my great predecessor (%s) to replicate its head table while processing fail event.\n", myPredecessor)
		return err
	}
	coordinator.replicateTable(constants.Head, mySuccessor, myGreatSuccessor)

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

func (coordinator *CoordinatorService) marshalInternalMessage(msg protobuf.InternalMsg) ([]byte, error) {
	byteMsg, err := proto.Marshal(&msg)
	if err != nil {
		return nil, err
	}
	return byteMsg, nil
}
