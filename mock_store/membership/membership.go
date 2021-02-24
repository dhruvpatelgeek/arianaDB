package membership

import (
	"dht/google_protocol_buffer/pb/protobuf"
	"dht/mock_store/transport"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
)

// SendJoinCommand is a command sent by new joins node
const SendJoinCommand uint32 = 12
// HeartbeatGossipCommand is a command for heartbeat protocol
const HeartbeatGossipCommand uint32 = 13

// TimeHeartbeat is time period for Heartbeat
const TimeHeartbeat = 100
// TimeFail is time period for failCheck Check
const TimeFail = 4000
// TimeCleanup is time period for Clean up failChecked node
const TimeCleanup = 10000

// TODO: consider moving the members list to its own file under the same package for cleanliness & atomic operations.
type membersListValue struct {
	isAlive            bool
	heartbeatTimestamp int64
}

type MembershipService struct {
	address         string
	members         map[string]*membersListValue // TODO: check if this works
	membersListLock sync.Mutex
	transport       *transport.TransportModule
	receiveChannel  chan []byte
}



// New Creates a new instance of the GroupMembershipService which manages the set of members in the service.
// On start up, the node will spawn a thread for listening to the receiveChannel. All group membership service operations
// must be requested through the receiveChannel.
//
// During the construction, this node will bootstrap by attempting to connect to all members given as the "initialMembers".
// If this node failChecked to connect to a node in the "initialMembers", it will gossip to others that the node failChecked.
//
// Current implementation listens and processes messages in a single-thread to minimize memory footprint.
//
// - assumes all arguments are not nil
func New(
	initialMembers []string,
	transport *transport.TransportModule,
	receiveChannel chan []byte,
	myAddress string,
	myPort string) (*MembershipService, error) {

	gms := new(MembershipService)
	gms.transport = transport
	gms.receiveChannel = receiveChannel
	gms.members = make(map[string]*membersListValue) // note: self.address is included in the members list

	// set self's address
	if ip := net.ParseIP(myAddress); ip == nil {
		return nil, errors.New("invalid self-ip address to create new membership service")
	}
	gms.address = myAddress + ":" + myPort

	// add itself to the membership
	gms.membersListLock.Lock()
	fmt.Println("[DEBUG] [GMS] Itself acquired lock: ", gms.members)
	gms.members[gms.address] = createNewMembersListValue() // add itself to the membership
	gms.members[gms.address].isAlive = true
	fmt.Println("[DEBUG] [GMS] Itself acquired lock: ", gms.members)
	gms.membersListLock.Unlock()

	// begin a thread for listening to the receive channel and processing messages
	go gms.listenToReceiveChannel()

	gms.bootstrap(initialMembers)

	// spawn heartbeat threads
	go gms.failCheck()
	go gms.cleanupCheck()
	go gms.heartbeat()

	return gms, nil
}

func (gms *MembershipService) failCheck() {

	for {
		// sleep for a certain amount of time
		time.Sleep(TimeFail * time.Millisecond)

		// iterate through membership lists and check if failChecked:
		gms.membersListLock.Lock()
		for addr := range gms.members {
			if addr != gms.address && gms.members[addr].heartbeatTimestamp < getCurrentTimeInMilliSec()-TimeFail {
				gms.members[addr].isAlive = false 
				fmt.Sprintln("Node %s failChecked, but won't clean up yet")
			}
		}
		gms.membersListLock.Unlock()
	}
}


func (gms *MembershipService) cleanupCheck() {

	for {
		// sleep for a certain amount of time
		time.Sleep(TimeCleanup * time.Millisecond)

		// iterate through membership lists and check if failChecked:
		gms.membersListLock.Lock()
		for member, element := range gms.members { // TODO: see if we can modify the value using regular syntax instead of worrying about pass-by-value?
			// TODO: Need to modify here:
			if !element.isAlive && element.heartbeatTimestamp < getCurrentTimeInMilliSec()-TimeCleanup {
				fmt.Sprintln("removing node %s from membership during cleanupCheck", member)
				delete(gms.members, member)
			}
		}
		gms.membersListLock.Unlock()

	}
}

func (gms *MembershipService) heartbeat() {
	// TODO: implement this

	for {
		// sleep for a cretain amount of time.
		time.Sleep(TimeHeartbeat * time.Millisecond)
		if len(gms.members) > 1 {
			// send hearbeat:
			// randomly choose one node to send message:
			gms.membersListLock.Lock()
			address := gms.chooseRandomKey()
			gms.membersListLock.Unlock()
			fmt.Sprintln("sending a heartbeat message to: %s", address)

			gms.sendList(address)
		}
	}
}


func (gms *MembershipService) chooseRandomKey() string {
	i := int(float32(len(gms.members)-1) * rand.Float32())
	for k, _ := range gms.members {
		if k == gms.address {
			continue
		}
		if i == 0 {
			return k
		} else {
			i--
		}
	}
	return ""
}

// TODO: modify this function later:
// func MapRandomKeyGet(mapI interface{}) interface{} {
// 	keys := reflect.ValueOf(mapI).MapKeys() // TODO: error being thrown here

// 	return keys[rand.Intn(len(keys))].Interface()
// }
func getCurrentTimeInMilliSec() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func (gms *MembershipService) processHeartbeat(request *protobuf.MembershipReq) error {
	destination := request.GetSourceAddress()
	if destination == "" {
		return errors.New("[ERROR] Received a send request without a destination")
	}

	// compare the difference between those two lists and merge
	gms.membersListLock.Lock()
	addresses := request.GetMembersList()
	for _, address := range addresses {
		if _, ok := gms.members[address]; !ok {
			gms.members[address] = createNewMembersListValue()
		}
	}

	gms.members[destination].heartbeatTimestamp = getCurrentTimeInMilliSec()
	gms.members[destination].isAlive = true
	gms.membersListLock.Unlock()

	return nil
}

func (gms *MembershipService) bootstrap(initialMembers []string) {
	fmt.Println("initial members list:", initialMembers)
	for _, address := range initialMembers {
		if address != gms.address {
			joinRequest := &protobuf.MembershipReq{
				SourceAddress:          gms.address,
				Command:                SendJoinCommand,
				MembersList:            gms.GetAllNodes(),
				JoinDestinationAddress: &address,
			}

			marshalledJoinRequest, err := proto.Marshal(joinRequest)
			if err == nil {
				fmt.Println("Sending join request to ", address, joinRequest)
				gms.transport.Send(marshalledJoinRequest, generateMessageID(), address)
			} else {
				fmt.Println(err)
			}
		}
	}
}

func (gms *MembershipService) listenToReceiveChannel() {
	for {
		select {
		case msgReceived := <-gms.receiveChannel:
			gms.processMessage(msgReceived)
		}
	}
}

func (gms *MembershipService) processMessage(msgReceived []byte) {
	membershipRequest, err := unmarshalMembershipRequest(msgReceived)
	if err != nil {
		fmt.Println(err)
		return
	}

	switch membershipRequest.GetCommand() {
	case SendJoinCommand:
		err := gms.processSendJoinRequest(membershipRequest)
		if err != nil {
			fmt.Println(err)
		}

	case HeartbeatGossipCommand:
		err := gms.processHeartbeat(membershipRequest)
		if err != nil {
			fmt.Println(err)
		}

	default:
		fmt.Printf("[ERROR] Undefined command received in Group Membership Service")
	}
}


func (gms *MembershipService) processSendJoinRequest(request *protobuf.MembershipReq) error {
	requestor := request.GetJoinDestinationAddress()
	if requestor != gms.address {
		return errors.New("[ERROR] Received a send request from a node other than myself")
	}
	destination := request.GetSourceAddress()
	if destination == "" {
		return errors.New("[ERROR] Received a send request without a destination")
	}

	gms.membersListLock.Lock()
	gms.members[destination] = createNewMembersListValue()
	gms.members[destination].heartbeatTimestamp = getCurrentTimeInMilliSec()
	gms.members[destination].isAlive = true
	gms.membersListLock.Unlock()
	// create a join request to be sent to destination
	MReq := &protobuf.MembershipReq{
		SourceAddress: gms.address,
		Command:       HeartbeatGossipCommand,
		MembersList:   gms.GetAllNodes(),
	}

	payload, err := proto.Marshal(MReq)
	if err != nil {
		return err
	}

	gms.transport.Send(payload, generateMessageID(), destination)
	return nil
}

// sendList(): send this node's membership list to the destination address
func (gms *MembershipService) sendList(destination string) {
	// TODO: Here we should probably modify the command to HeartbeatGossipCommand
	isSuccessful, payload := gms.marshalMembershipRequest(HeartbeatGossipCommand, gms.GetAllNodes())
	if !isSuccessful {
		return
	}

	gms.transport.Send(payload, generateMessageID(), destination)
}


func (gms *MembershipService) GetAllNodes() []string {
	var allNodes []string

	// TODO: put a lock around it
	gms.membersListLock.Lock()
	for key, val := range gms.members {
		if val.isAlive {
			allNodes = append(allNodes, key)
		}
	}
	gms.membersListLock.Unlock()

	return allNodes
}

func generateMessageID() []byte {
	id := uuid.New().String()

	return []byte("gossip" + id)
}


func (gms *MembershipService) marshalMembershipRequest(command uint32, list []string) (bool, []byte) {
	MReq := &protobuf.MembershipReq{
		SourceAddress: gms.address,
		Command:       command,
		MembersList:   list,
	}

	data, err := proto.Marshal(MReq)
	if err != nil {
		fmt.Println("failChecked to encode MembershipReq: ", err)
		return false, nil
	}
	return true, data
}

func unmarshalMembershipRequest(list []byte) (*protobuf.MembershipReq, error) {
	MReq := &protobuf.MembershipReq{}
	err := proto.Unmarshal(list, MReq)
	if err != nil {
		return nil, err
	}
	return MReq, nil
}

// TODO: atomic operations for accessing members lists
func createNewMembersListValue() *membersListValue {
	val := membersListValue{
		isAlive:            false,
		heartbeatTimestamp: 0,
	}
	return &val
}
