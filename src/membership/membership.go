package membership

import (
	"dht/proto/pb/protobuf"
	"dht/src/transport"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
)

const FANOUT int = 10
const SEND_JOIN_COMMAND uint32 = 12
const HEARTBEAT_GOSSIP_COMMAND uint32 = 13

// unit: in milliseconds
const TIME_HEARTBEAT = 100
const TIME_FAIL_CHECK = 5000
const TIME_CLEANUP = 10000

type membershipService struct {
	address         string
	members         map[string]*membersListValue // TODO: check if this works
	membersListLock sync.Mutex                   // TODO: use this lock
	sendChannel     chan transport.Message
	receiveChannel  chan []byte
}

// TODO: consider moving the members list to its own file under the same package for cleanliness & atomic operations.
type membersListValue struct {
	isAlive            bool
	heartbeatTimestamp int64
}

// New Creates a new instance of the GroupMembershipService which manages the set of members in the service.
// On start up, the node will spawn a thread for listening to the receiveChannel. All group membership service operations
// must be requested through the receiveChannel.
//
// During the construction, this node will bootstrap by attempting to connect to all members given as the "initialMembers".
// If this node failed to connect to a node in the "initialMembers", it will gossip to others that the node failed.
//
// Current implementation listens and processes messages in a single-thread to minimize memory footprint.
//
// - assumes all arguments are not nil
func New(
	initialMembers []string,
	sendChannel chan transport.Message,
	receiveChannel chan []byte,
	myAddress string,
	myPort string) (*membershipService, error) {

	gms := new(membershipService)
	gms.sendChannel = sendChannel
	gms.receiveChannel = receiveChannel
	gms.members = make(map[string]*membersListValue) // note: self.address is included in the members list

	// set self's address
	if ip := net.ParseIP(myAddress); ip == nil {
		return nil, errors.New("Invalid self-ip address to create new membership service.")
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
	go gms.fail()
	go gms.cleanup()
	go gms.heartbeat()

	return gms, nil
}

func (gms *membershipService) fail() {
	// TODO: implement this
	for true {
		// sleep for a certain amount of time
		time.Sleep(TIME_FAIL_CHECK * time.Millisecond)

		// iterate through membership lists and check if failed:
		gms.membersListLock.Lock()
		for addr := range gms.members {
			if addr != gms.address && gms.members[addr].heartbeatTimestamp < getCurrentTimeInMilliSec()-TIME_FAIL_CHECK {
				gms.members[addr].isAlive = false // cannot assign to struct field gms.members[addr].isFailed in mapcompiler
				fmt.Sprintln("Node %s failed, but won't clean up yet")
			}
		}
		gms.membersListLock.Unlock()
	}
}

func (gms *membershipService) cleanup() {
	// TODO: implement this
	for true {
		// sleep for a certain amount of time
		time.Sleep(TIME_CLEANUP * time.Millisecond)

		// iterate through membership lists and check if failed:
		gms.membersListLock.Lock()
		for member, element := range gms.members { // TODO: see if we can modify the value using regular syntax instead of worrying about pass-by-value?
			// TODO: Need to modify here:
			if !element.isAlive && element.heartbeatTimestamp < getCurrentTimeInMilliSec()-TIME_CLEANUP {
				// delete the items:
				fmt.Sprintln("removing node %s from membership during cleanup", member)
				delete(gms.members, member)
			}
		}
		gms.membersListLock.Unlock()

	}
}

func (gms *membershipService) heartbeat() {
	// TODO: implement this

	for {
		time.Sleep(TIME_HEARTBEAT * time.Millisecond)
		if len(gms.members) > 1 {
			// send hearbeat:
			// randomly choose one node to send message:
			gms.membersListLock.Lock()
			address := gms.chooseRandomKey()
			gms.membersListLock.Unlock()
			fmt.Sprintln("sending a heartbeat message to: %s", address)

			gms.sendList(address)
			// sleep for a cretain amount of time.
		}

	}

}

func (gms *membershipService) chooseRandomKey() string {
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

func (gms *membershipService) processHeartbeat(request *protobuf.MembershipReq) error {
	// TODO:
	// update time stamp of source request

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

func (gms *membershipService) bootstrap(initialMembers []string) {
	fmt.Println("initial members list:", initialMembers)
	for _, address := range initialMembers {
		if address != gms.address {
			joinRequest := &protobuf.MembershipReq{
				SourceAddress:          gms.address,
				Command:                SEND_JOIN_COMMAND,
				MembersList:            gms.GetAllNodes(),
				JoinDestinationAddress: &address,
			}
			// marshalledJoinRequest, err := proto.Marshal(joinRequest)
			// if err == nil {
			// 	fmt.Println("Sending join request to ", address)
			// 	gms.receiveChannel <- marshalledJoinRequest
			// } else {
			// 	fmt.Println(err)
			// }

			marshalledJoinRequest, err := proto.Marshal(joinRequest)
			if err == nil {
				fmt.Println("Sending join request to ", address)
				msgID := gms.generateMessageID()
				msg := transport.Message{
					ClientAddr: address,
					Payload:    marshalledJoinRequest,
					MessageID:  msgID,
				}
				gms.sendChannel <- msg
			} else {
				fmt.Println(err)
			}
		}
	}
}

func (gms *membershipService) listenToReceiveChannel() {
	for {
		select {
		case msgReceived := <-gms.receiveChannel:
			go gms.processMessage(msgReceived)
		}
	}
}

func (gms *membershipService) processMessage(msgReceived []byte) {
	membershipRequest, err := unmarshalMembershipRequest(msgReceived)

	if err != nil {
		fmt.Println(err)
		return
	}
	// fmt.Println("fhdioshnalfskjfnsdklfdsfdfdfdsgfghtfdhnb dgh")
	// fmt.Println(membershipRequest.GetCommand())
	// fmt.Println("fhdioshnalfskjfnsdklfdsfdfdfdsgfghtfdhnb dgh")
	switch membershipRequest.GetCommand() {
	case SEND_JOIN_COMMAND:
		err := gms.processSendJoinRequest(membershipRequest)
		if err != nil {
			fmt.Println(err)
		}

	case HEARTBEAT_GOSSIP_COMMAND:
		// fmt.Println(membershipRequest)
		err := gms.processHeartbeat(membershipRequest)
		if err != nil {
			fmt.Println(err)
		}

	default:
		fmt.Printf("[ERROR] Undefined command received in Group Membership Service")
	}
}
func (gms *membershipService) processSendJoinRequest(request *protobuf.MembershipReq) error {
	// TODO: get source of the request
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
		Command:       HEARTBEAT_GOSSIP_COMMAND,
		MembersList:   gms.GetAllNodes(),
	}

	payload, err := proto.Marshal(MReq)
	if err != nil {
		return err
	}

	// TODO: write a function that automatically sends & gossip failed request if unable to send...
	msgID := gms.generateMessageID()
	msg := transport.Message{
		ClientAddr: destination,
		Payload:    payload,
		MessageID:  msgID,
	}

	gms.sendChannel <- msg
	return nil
}

// sendList(): send this node's membership list to the destination address
func (gms *membershipService) sendList(destination string) {
	// TODO: Here we should probably modify the command to HEARTBEAT_GOSSIP_COMMAND
	isSuccessful, payload := gms.marshalMembershipRequest(HEARTBEAT_GOSSIP_COMMAND, gms.GetAllNodes())
	if !isSuccessful {
		return
	}

	msgID := gms.generateMessageID()
	msg := transport.Message{
		ClientAddr: destination,
		Payload:    payload, // TODO: if we create a new proto, we marshal & attach to payload
		MessageID:  msgID,   // TODO: if we create a new proto, the transport layer will still know which component to route a message to (e.g.: storage vs gms)
	}
	// fmt.Println("Sending message ")

	gms.sendChannel <- msg
}

func (gms *membershipService) GetAllNodes() []string {
	var allNodes []string

	// TODO: put a lock around it
	gms.membersListLock.Lock()

	for key, val := range gms.members {
		if val.isAlive {
			allNodes = append(allNodes, key)
		}
		// TODO: also append ourselves, or the m.members contains ourselves too
	}
	gms.membersListLock.Unlock()

	return allNodes
}

func (m *membershipService) generateMessageID() string {
	id := uuid.New().String()

	return "gossip" + id
}

func (gms *membershipService) marshalMembershipRequest(command uint32, list []string) (bool, []byte) {
	MReq := &protobuf.MembershipReq{
		SourceAddress: gms.address,
		Command:       command,
		MembersList:   list,
	}

	data, err := proto.Marshal(MReq)
	if err != nil {
		fmt.Println("Failed to encode MembershipReq: ", err)
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
