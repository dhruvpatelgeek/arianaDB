package membership

import (
	"dht/google_protocol_buffer/pb/protobuf"
	"dht/src/transport"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strconv"

	"sync"
	"time"

	"github.com/golang/protobuf/proto"
)

// TODO: consider moving the members list to its own file under the same package for cleanliness & atomic operations.
type membersListValue struct {
	isAlive            bool
	heartbeatTimestamp int64
}

type MembershipService struct {
	address                string
	members                map[string]*membersListValue
	membersListLock        sync.Mutex
	transport              *transport.TransportModule
	receiveChannel         chan []byte
	GMSEventMessageChannel chan GMSEventMessage

	// This should be recorded in case that the node "failed" to join a system.
	initialMembers []string
}

// New creates a new instance of the GroupMembershipService which manages the set of members in the service.
// On start up, the node will spawn a thread for listening to the receiveChannel. All group membership service operations
// must be requested through the receiveChannel.
//
// During the construction, this node will bootstrap by attempting to connect to all members given as the "initialMembers".
// If this node failed to connect to a node in the "initialMembers", it will ignore the failed node.
//
// Current implementation uses 4 threads:
// 	- worker thread who listens and processes messages
//	- heartbeat thread who periodically sends a heartbeat message to a subset of its memberslist
//	- "fail" thread who periodically checks if a heartbeat was received in a reasonable amount of time.
//	- "cleanup" thread who periodically removes all "failed" nodes from the members list.
//
func New(
	initialMembers []string,
	transport *transport.TransportModule,
	receiveChannel chan []byte,
	myAddress string,
	myPort string,
	GMSEventMessageChannel chan GMSEventMessage) (*MembershipService, error) {

	gms := new(MembershipService)
	gms.transport = transport
	gms.receiveChannel = receiveChannel
	gms.members = make(map[string]*membersListValue) // note: self.address is included in the members list

	// set self's address
	if ip := net.ParseIP(myAddress); ip == nil {
		return nil, errors.New("invalid self-ip address to create new membership service")
	}
	parsedPort, err := strconv.Atoi(myPort)
	if err != nil {
		return nil, errors.New("Failed to parse port")
	}
	if parsedPort < 0 || parsedPort > 65535 {
		return nil, errors.New("Invalid port number")
	}

	gms.address = myAddress + ":" + myPort

	// add itself to the membership
	gms.membersListLock.Lock()
	gms.members[gms.address] = createNewMembersListValue() // add itself to the membership
	gms.members[gms.address].isAlive = true
	gms.membersListLock.Unlock()
	gms.GMSEventMessageChannel = GMSEventMessageChannel

	gms.initialMembers = initialMembers

	// begin a thread for listening to the receive channel and processing messages
	go gms.listenToReceiveChannel()

	go gms.bootstrap()

	// spawn heartbeat threads
	go gms.failCheck()
	go gms.cleanup()
	go gms.heartbeat()

	//go gms.monitorMembership()

	return gms, nil
}

/** failCheck() is a blocking function which periodically checks for nodes that haven't sent a heartbeat
within a the FailCheckPeriod and marks the node as "failed" for cleanup.
*/
func (gms *MembershipService) failCheck() {
	for {
		time.Sleep(FailCheckPeriod * time.Millisecond)

		gms.membersListLock.Lock()
		failedNodes := []string{}
		for addr := range gms.members {
			if addr != gms.address && gms.members[addr].heartbeatTimestamp < getCurrentTimeInMilliSec()-FailCheckPeriod {
				gms.members[addr].isAlive = false
			}
			if gms.members[addr].isAlive == false {
				failedNodes = append(failedNodes, addr)
			}
		}
		if len(failedNodes) > 0 {
			log.Printf("[GMS] [Debug] Failures detected: %v \n", failedNodes)
		}
		gms.membersListLock.Unlock()
	}
}

/** cleanup() is a blocking function which periodically scans the membership list for nodes that are suspected to be failed.
If the suspected nodes did not send a heartbeat in that time, the cleanup() thread deletes the node.
The act of removing a "failed node" from the membership list is called "cleanup"

Every CleanupPeriod, the cleanup() thread will notify the system about all nodes that have been cleanedup by
generating a GMSEventmessage for every cleaned up node.

Warning: binning failures by CleanupPeriod may cause two nodes who failed near the same time to be reported
in two separate GMSEventMessages.
*/
func (gms *MembershipService) cleanup() {
	for {
		time.Sleep(CleanupPeriod * time.Millisecond)
		var removedMembers []string

		// clean up membership of failed suspects
		gms.membersListLock.Lock()
		for failedSuspect, element := range gms.members {
			if !element.isAlive && element.heartbeatTimestamp < getCurrentTimeInMilliSec()-CleanupPeriod {
				delete(gms.members, failedSuspect)

				removedMembers = append(removedMembers, failedSuspect)
			}
		}
		gms.membersListLock.Unlock()

		// notify the system of all nodes that were cleaned up if there are any
		if len(removedMembers) > 0 {
			fmt.Println("[GMS] node failure detected: ", removedMembers)
			msg := GMSEventMessage{
				EventType: Failed,
				Nodes:     removedMembers, // TODO:
			}
			gms.GMSEventMessageChannel <- msg
		}
	}
}

/** heartbeat() is a blocking function which periodically sends a hearbeat message
to randomly chosen subset of nodes in the service, where:
	1. the period is every HeartbeatPeriod
	2. the subset of nodes excludes itself
	3. the number of nodes in the subset is constrained by size min(FANOUT, numOtherMembers)
*/
func (gms *MembershipService) heartbeat() {
	for {
		time.Sleep(HeartbeatPeriod * time.Millisecond)
		if len(gms.members) > 1 {
			gossipGroup := gms.getGossipGroup()

			for _, address := range gossipGroup {
				err := gms.sendHeartbeat(address)
				if err != nil {
					fmt.Printf("[GMS] [Error] Failed to send a heartbeat to %s. \n\t Caused by %s\n", address, err.Error())
				}
			}
		}
	}
}

/** getGossipGroup() returns a random subset of other nodes to gossip to of size min(FANOUT, numOtherMembers).
"Other nodes" refers to all nodes in the membership excluding itself.
*/
func (gms *MembershipService) getGossipGroup() []string {
	// create a list of other members excluding itself.
	var otherMembers []string

	gms.membersListLock.Lock()
	for member, _ := range gms.members {
		if member != gms.address {
			otherMembers = append(otherMembers, member)
		}
	}
	gms.membersListLock.Unlock()

	var numOtherMembers = len(otherMembers)

	// randomize
	rand.Shuffle(numOtherMembers, func(i, j int) {
		otherMembers[i], otherMembers[j] = otherMembers[j], otherMembers[i]
	})

	// return a subset of size min(FANOUT, numOtherMembers)
	var subsetSize int
	if subsetSize = numOtherMembers; int(FANOUT) < numOtherMembers {
		subsetSize = int(FANOUT)
	}
	return otherMembers[:subsetSize]
}

// Get the current time in milli sec
func getCurrentTimeInMilliSec() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

// Process when received a heartbeat message
func (gms *MembershipService) processHeartbeat(request *protobuf.MembershipReq) error {
	destination := request.GetSourceAddress()
	if destination == "" {
		return errors.New("[ERROR] Received a send request without a destination")
	}

	gms.membersListLock.Lock()
	addresses := request.GetMembersList()
	for _, address := range addresses {
		if _, ok := gms.members[address]; !ok {
			gms.members[address] = createNewMembersListValue()

			// notify the system that a new node has joined
			if destination != gms.address { // TODO: maybe it's better not to include itself in the membership list
				msg := GMSEventMessage{
					EventType: Joined,
					Nodes:     []string{address},
				}
				gms.GMSEventMessageChannel <- msg
			}
		}
	}

	gms.members[destination].heartbeatTimestamp = getCurrentTimeInMilliSec()
	gms.members[destination].isAlive = true
	gms.membersListLock.Unlock()

	return nil
}

/** bootstrap() tries to join an existing system by contacting its initial members list once.
Assumes that at least one node
*/
func (gms *MembershipService) bootstrap() {
	fmt.Println("[GMS] [Info] [Bootstrap]: sending join requests to initial members: ", gms.initialMembers)
	for _, initialMember := range gms.initialMembers {
		err := gms.sendHeartbeat(initialMember)
		if err != nil {
			fmt.Printf("[GMS] [Info] Failed to send a heartbeat to the initial member %s during bootstrap.\n", initialMember)
		}
	}
}

// process when received message from channel
func (gms *MembershipService) listenToReceiveChannel() {
	for {
		select {
		case msgReceived := <-gms.receiveChannel:
			err := gms.processMessage(msgReceived)
			if err != nil {
				fmt.Printf("[GMS] [Error] unable to process heartbeat message from other nodes. \n\t %s\n", err.Error())
			}
		}
	}
}

// Whenever received a message from channel, this function tries to classify what kind of
// message it is and use different functions to handle different kinds of message.
func (gms *MembershipService) processMessage(msgReceived []byte) error {
	membershipRequest, err := unmarshalMembershipRequest(msgReceived)
	if err != nil {
		return err
	}

	switch membershipRequest.GetCommand() {
	case HeartbeatGossipCommand:
		err := gms.processHeartbeat(membershipRequest)
		if err != nil {
			return err
		}

	default:
		return fmt.Errorf("[GMS] [Error] Undefined command received in gms")
	}
	return nil
}

/** sendHeartbeat() sends a heartbeat message to the destination address by packaging its
membership list for the destination node to compare.
*/
func (gms *MembershipService) sendHeartbeat(destination string) error {
	membershipRequest := &protobuf.MembershipReq{
		SourceAddress:          gms.address,
		Command:                HeartbeatGossipCommand,
		MembersList:            gms.GetAllNodes(),
		JoinDestinationAddress: &destination,
	}
	err := gms.transport.SendHeartbeat(membershipRequest, destination)
	if err != nil {
		return err
	}
	return nil
}

/** GetAllNodes() returns a list of all alive nodes in the system.
This function assumes that the caller doesn't hold the membership lock.If the caller
holds the lock and calls the function, then the caller will encounter a re-entrant deadlock.
*/
func (gms *MembershipService) GetAllNodes() []string {
	var allNodes []string

	gms.membersListLock.Lock()
	for key, val := range gms.members {
		if val.isAlive {
			allNodes = append(allNodes, key)
		}
	}
	gms.membersListLock.Unlock()

	return allNodes
}

func (gms *MembershipService) monitorMembership() {
	for {
		time.Sleep(5 * time.Second)
		fmt.Println("[GMS] [Info] members: ", gms.GetAllNodes())
	}
}

func unmarshalMembershipRequest(list []byte) (*protobuf.MembershipReq, error) {
	MReq := &protobuf.MembershipReq{}
	err := proto.Unmarshal(list, MReq)
	if err != nil {
		return nil, err
	}
	return MReq, nil
}

func createNewMembersListValue() *membersListValue {
	val := membersListValue{
		isAlive:            false,
		heartbeatTimestamp: 0,
	}
	return &val
}
