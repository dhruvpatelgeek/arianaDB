package coordinator

import (
	"dht/google_protocol_buffer/pb/protobuf"
	"dht/src/constants"
	"errors"
	"log"

	"dht/src/membership"
	"dht/src/processWrite"
	"dht/src/replication"
	"dht/src/storage"
	"dht/src/transport"
	"fmt"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

/** CoordinatorService is responsible for responding to external inputs (i.e.: client-to-server requests,
server-to-server requests) and internal inputs (gms events).
*/
type CoordinatorService struct {
	gmsEventChannel         chan membership.GMSEventMessage
	incomingMessagesChannel chan protobuf.InternalMsg
	toStorageChannel        chan protobuf.InternalMsg
	transport               *transport.TransportModule
	replicationService      *replication.ReplicationService
	storageService          *storage.StorageService
	writeManager            *processWrite.ProcessWrite

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

	coordinator.writeManager = processWrite.New()

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
		case constants.LogKVRequest:
			coordinator.processLogRequest(incomingMessage)
		case constants.CommitKVRequest:
			coordinator.processCommitRequest(incomingMessage, kvRequest)

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

// Handles client requests received by the callingnode. If the calling node is responsible for the request then it will be handled,
// and forwarded to the next node in the chain if necessary. If the calling node is not responsible for the request,
// then the request is sent to the responsible node
func (coordinator *CoordinatorService) processClientRequest(incomingMessage protobuf.InternalMsg, kvRequest *protobuf.KVRequest) {
	destinationAddress := coordinator.replicationService.FindSuccessorNode(string(kvRequest.Key))
	destinationTable := uint32(constants.Head)
	respondToClient := false

	storageCommand := kvRequest.GetCommand()
	if selfIP := coordinator.hostIPv4; destinationAddress == selfIP {
		if isUpdateRequest(kvRequest) {
			incomingMessage.Command = uint32(constants.LogKVRequest)
			coordinator.processLogRequest(incomingMessage)
			return
		} else if storageCommand == storage.GET {
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
	if storageCommand == storage.SHUTDOWN {
		return
	}

	if destinationTable == uint32(constants.Head) && storageCommand == storage.GET {
		respondToClient = true
	}

	outgoingMessage := incomingMessage
	outgoingMessage.DestinationNodeTable = &destinationTable
	outgoingMessage.RespondToClient = &respondToClient

	if isUpdateRequest(kvRequest) {
		outgoingMessage.Command = uint32(constants.LogKVRequest)
	} else {
		outgoingMessage.Command = uint32(constants.ProcessPropagatedKVRequest)
	}

	coordinator.propagateRequest(outgoingMessage, destinationAddress)
}

// Checks if the given request updates the KV Store
func isUpdateRequest(kvRequest *protobuf.KVRequest) bool {
	command := kvRequest.GetCommand()
	return command == storage.PUT || command == storage.REMOVE || command == storage.WIPEOUT
}

// Handles any requests that have been forwarded by other nodes, and forwards the request if necessary
func (coordinator *CoordinatorService) processPropagatedRequest(incomingMessage protobuf.InternalMsg, kvRequest *protobuf.KVRequest) {
	coordinator.toStorageChannel <- incomingMessage

	if kvRequest.GetCommand() != storage.GET {
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

// Sends a request to the storage layer when the appropriate number of commit messages has been received
func (coordinator *CoordinatorService) processCommitRequest(incomingMessage protobuf.InternalMsg, kvRequest *protobuf.KVRequest) {
	request, commit := coordinator.writeManager.Commit(incomingMessage)
	currTable := incomingMessage.GetDestinationNodeTable()
	if commit && currTable == uint32(constants.Head) {
		coordinator.sendCommitsToBackups(incomingMessage)
		responseToClient := true
		request.RespondToClient = &responseToClient
		coordinator.toStorageChannel <- request
	} else if commit {
		coordinator.toStorageChannel <- request
	}

}

// Finds the current node's backup nodes (successor and grand successor),
// and sends a commit message to each of them
func (coordinator *CoordinatorService) sendCommitsToBackups(incomingMessage protobuf.InternalMsg) {
	destinationTable := uint32(constants.Middle)
	command := uint32(constants.CommitKVRequest)
	respondToClient := false

	// Send message so the backup commits the request (successor)
	outgoingMessage := incomingMessage
	outgoingMessage.DestinationNodeTable = &destinationTable
	outgoingMessage.Command = command
	outgoingMessage.RespondToClient = &respondToClient
	coordinator.sendToSuccessor(outgoingMessage)

	// Sends message so the backup commits the request (grand successor)
	destinationTable = uint32(constants.Tail)

	outgoingMessage.DestinationNodeTable = &destinationTable
	coordinator.sendToGrandSuccessor(outgoingMessage)
}

// Logs requests that will be committed when a corresponding commit message is received in the future
func (coordinator *CoordinatorService) processLogRequest(incomingMessage protobuf.InternalMsg) {
	currTable := incomingMessage.GetDestinationNodeTable()
	if currTable == uint32(constants.Head) {
		coordinator.writeManager.Log(incomingMessage, 2)

		coordinator.sendLogsToBackups(incomingMessage)
	} else {
		coordinator.writeManager.Log(incomingMessage, 1)
		coordinator.sendCommitToPrimary(incomingMessage)
	}
}

// Sends a commit message to the primary node.
// This is used by the backup nodes once they have logged a request from the primary node
func (coordinator *CoordinatorService) sendCommitToPrimary(incomingMessage protobuf.InternalMsg) {
	currTable := incomingMessage.GetDestinationNodeTable()
	destinationTable := uint32(constants.Head)
	command := uint32(constants.CommitKVRequest)
	outgoingMessage := incomingMessage
	outgoingMessage.DestinationNodeTable = &destinationTable
	outgoingMessage.Command = command

	if currTable == uint32(constants.Middle) {
		coordinator.sendToPredecessor(outgoingMessage)
	} else if currTable == uint32(constants.Tail) {
		coordinator.sendToGrandPredecessor(outgoingMessage)
	} else {
		log.Print("[ERROR] NOT MIDDEL OR TAIL 67gh7iub ")
	}

}

// Sends log messages to the two backup nodes (current node's successor and grand successor)
// Used by the primary node when it receives a request from the client
func (coordinator *CoordinatorService) sendLogsToBackups(incomingMessage protobuf.InternalMsg) {
	// Send to successor
	destinationTable := uint32(constants.Middle)
	command := uint32(constants.LogKVRequest)
	respondToClient := false

	outgoingMessage := incomingMessage
	outgoingMessage.DestinationNodeTable = &destinationTable
	outgoingMessage.Command = command
	outgoingMessage.RespondToClient = &respondToClient
	coordinator.sendToSuccessor(outgoingMessage)

	// Send to Grand successor
	destinationTable = uint32(constants.Tail)

	outgoingMessage.DestinationNodeTable = &destinationTable
	coordinator.sendToGrandSuccessor(outgoingMessage)
}

// Finds the successor of the current node and sends the given message to it
func (coordinator *CoordinatorService) sendToSuccessor(outgoingMessage protobuf.InternalMsg) {
	destinationAddress := coordinator.replicationService.FindSuccessorNode(coordinator.hostIPv4)

	coordinator.propagateRequest(outgoingMessage, destinationAddress)
}

// Finds the grand successor (successor's successor) of the current node and sends the given message to it
func (coordinator *CoordinatorService) sendToGrandSuccessor(outgoingMessage protobuf.InternalMsg) {
	successor := coordinator.replicationService.FindSuccessorNode(coordinator.hostIPv4)
	destinationAddress := coordinator.replicationService.FindSuccessorNode(successor)

	coordinator.propagateRequest(outgoingMessage, destinationAddress)
}

// Finds the predecessor of the current node and sends the given message to it
func (coordinator *CoordinatorService) sendToPredecessor(outgoingMessage protobuf.InternalMsg) {
	destinationAddress := coordinator.replicationService.FindPredecessorNode(coordinator.hostIPv4)

	coordinator.propagateRequest(outgoingMessage, destinationAddress)
}

// Finds the grand predecessor (the predecessor's predecessor) of the current node and sends the given message to it
func (coordinator *CoordinatorService) sendToGrandPredecessor(outgoingMessage protobuf.InternalMsg) {
	predecessor := coordinator.replicationService.FindPredecessorNode(coordinator.hostIPv4)
	destinationAddress := coordinator.replicationService.FindPredecessorNode(predecessor)

	coordinator.propagateRequest(outgoingMessage, destinationAddress)
}

// Sends a message to another node with the given ip address (destinationAddress).
// The message is not guaranteed to be delivered to the destination.
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

		switch eventType {
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
	successor := self.replicationService.FindSuccessorNode(newNode)
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

/**
The coordinator layer will ask the replication layer the relationship between this node and the fail node:
- if this node is the successor of the failnode, then it will
	- merge the ***HEAD*** table and ***MIDDLE*** table
	- send replication request to the predecessor and grand predecessor of the fail node to replicate theirs ***HEAD*** table to their successor and grand successor
	- replicate this node's ***HEAD*** table to its successor and grant successor
- if this node is at least the grand successor of the fail node, then it will:
	- merge the ***HEAD*** table, ***MIDDLE*** table, and ***TAIL*** table.
	- send replication request to the predecessor and grand predecessor of the fail node to replicate theirs ***HEAD*** table to their successor and grand successor
	- replicate this node's ***HEAD*** table to its successor and grant successor
- otherwise do nothing
*/
func (self *CoordinatorService) processFailEvent(gmsEvent membership.GMSEventMessage) error {
	// merge local tables
	numNewNodes := len(gmsEvent.Nodes)
	if numNewNodes <= 0 {
		return fmt.Errorf("[Coordinator] [Error] Expected fail event message from GMS to contain at least 1 failed node. This failed event message contains the failed nodes: %v", gmsEvent.Nodes)
	}
	failedNodes := gmsEvent.Nodes

	// check self's heritage in relation to the new node
	isSuccessor := self.isSuccessorOfAnyFailNode(failedNodes)

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

/**
 - If there are only 1 node between me(this node) and my current predecessor, then this node will merge
its Head table and Middle table
 - else, it will merge its Head table, Middle table, and Tail table.
*/
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

	mySelf := coordinator.hostIPv4
	mySuccessor := coordinator.replicationService.FindSuccessorNode(mySelf)
	myGreatSuccessor := coordinator.replicationService.FindSuccessorNode(mySuccessor)
	myPredecessor := coordinator.replicationService.FindPredecessorNode(mySelf)
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

// This Function will ask the storage layer to merge its Head and Middle table
func (coordinator *CoordinatorService) combineHeadMiddleTable() error {
	err := coordinator.storageService.MergeTables(constants.Head, constants.Middle)
	if err != nil {
		return errors.New("[Coordinator] Failed to merge middle table into head table. Caused by: \n" + err.Error())
	}
	return nil
}

// This function will ask the storage layer to merge its Head, Middle and Tail table.
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

// Marshal Internal Message
func (coordinator *CoordinatorService) marshalInternalMessage(msg protobuf.InternalMsg) ([]byte, error) {
	byteMsg, err := proto.Marshal(&msg)
	if err != nil {
		return nil, err
	}
	return byteMsg, nil
}
