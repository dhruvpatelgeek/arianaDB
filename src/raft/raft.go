package raft

import (
	"dht/google_protocol_buffer/pb/protobuf"
	"dht/src/membership"
	"dht/src/replication"
	"dht/src/transport"
	"encoding/json"
	"fmt"
	"github.com/fatih/color"
	"github.com/golang/protobuf/proto"
	"github.com/looplab/fsm"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

//DEBUG-------------------------
var debug=false;
//------------------------------


type raftnode struct {
	node_ip  string
	timeoutMutex sync.Mutex
	leaderMutex sync.Mutex
	mapMutex sync.Mutex
	pingAck int
	timeout time.Duration
	leader_ip string
	voteRec int
	voteTermMap map[int32]int32
	termNum int32
	fromCoordinator chan protobuf.RaftPayload
	FSM *fsm.FSM

	gmsPtr *membership.MembershipService
	transportPtr *transport.TransportModule
	replication *replication.ReplicationService


	// MAP IP ADDRESS TO STATUS
	netMapMutex sync.Mutex
	/*
	JOINED -> node just joined

	LEFT -> node failed
	-----------------------------
	COMMIT -> node added to network
	 */
	netMap map[string]string
	// on event status change call the migration
	procressingStruct []string
}

// TIMER VARIABLES---------------------
//maximum netowkr down time is 20 seconds
var MIN_TIME = 6
var MAX_TIME = 16
var REFRESH_RATE =250*time.Millisecond
var NETWORK_DELAY=5 // added the network delay veriable
var GAIN=1*time.Second
//-------------------------------------
// COLOR MESSAGES-----------------------
var LDprint = color.New(color.FgBlue).Add(color.ReverseVideo)
var CDprint=color.New(color.FgYellow)
var FWprint=color.New(color.FgMagenta).Add(color.ReverseVideo)
var Xprint=color.New(color.ReverseVideo)
var RXprint=color.New(color.FgRed).Add(color.ReverseVideo)
var ERR=color.New(color.FgYellow).Add(color.ReverseVideo)
var NCPrint=color.New(color.FgHiCyan)
//-------------------------------------

//INIT-----------------------------------------

func New(	gmsPtr *membership.MembershipService,
	transportPtr *transport.TransportModule,
	replication *replication.ReplicationService,
	nodeIp string,
	port int,
	coordinatorToRaft chan protobuf.RaftPayload){

	rand.Seed(time.Now().UnixNano())
	r:=rand.Intn(MAX_TIME - MIN_TIME + 1) + MIN_TIME
	timeout:=time.Duration(r) * time.Second

	R2RPort:=port+2;

	selfAddr:=nodeIp
	selfAddr=selfAddr+":"
	selfAddr+=strconv.Itoa(R2RPort)

	raftStateMachine := RaftStateMachine(coordinatorToRaft,selfAddr,timeout)
	raftStateMachine.gmsPtr=gmsPtr
	raftStateMachine.transportPtr=transportPtr
	raftStateMachine.replication=replication
	raftStateMachine.startUpPromt()
	raftStateMachine.voteRec=0
	raftStateMachine.voteTermMap= make(map[int32]int32)
	raftStateMachine.netMap=make(map[string]string)
	raftStateMachine.termNum=0;
	err := raftStateMachine.FSM.Event("init")
	if err != nil {
		fmt.Println(err)
	}
	go raftStateMachine.incomingMsgReader();
}
//reference from
//https://github.com/looplab/fsm/blob/v0.2.0/fsm.go#L88
func RaftStateMachine(coordinatorToRaft chan protobuf.RaftPayload,node_ip_add string,_timeout time.Duration) *raftnode {
	currNode := &raftnode{
		node_ip: node_ip_add,
		timeout: _timeout,
		leader_ip: "NULL",
		fromCoordinator:coordinatorToRaft,
	}

	currNode.FSM = fsm.NewFSM(
		"initial",
		fsm.Events{
			{Name: "init", Src: []string{"initial"}, Dst: "follower"},
			{Name: "follower_to_candidate", Src: []string{"follower"}, Dst: "candidate"},
			{Name: "candidate_to_leader", Src: []string{"candidate"}, Dst: "leader"},
			{Name: "candidate_to_follower", Src: []string{"candidate"}, Dst: "follower"},
			{Name: "leader_to_follower", Src: []string{"leader"}, Dst: "follower"},
		},
		fsm.Callbacks{
			// NewFSM constructs a FSM from events and callbacks.
			//
			// The events and transitions are specified as a slice of Event structs
			// specified as Events. Each Event is mapped to one or more internal
			// transitions from Event.Src to Event.Dst.
			//
			// Callbacks are added as a map specified as Callbacks where the key is parsed
			// as the callback event as follows, and called in the same order:
			//
			// 1. before_<EVENT> - called before event named <EVENT>
			//
			// 2. before_event - called before all events
			//
			// 3. leave_<OLD_STATE> - called before leaving <OLD_STATE>
			//
			// 4. leave_state - called before leaving all states
			//
			// 5. enter_<NEW_STATE> - called after entering <NEW_STATE>
			//
			// 6. enter_state - called after entering all states
			//
			// 7. after_<EVENT> - called after event named <EVENT>
			//
			// 8. after_event - called after all events

			"before_leader_to_follower":func(e *fsm.Event) {
				CDprint.Println("LEADER STEPS DOWN")
				defer color.Unset()
				currNode.followerFunc(e)
			},
			"before_candidate_to_follower":func(e *fsm.Event) {
				CDprint.Println("FAILED TO WIN ELECTION")
			},
			"enter_follower":func(e *fsm.Event) {
				defer color.Unset()
				currNode.followerFunc(e)
			},
			"enter_candidate":func(e *fsm.Event) {
				defer color.Unset()
				currNode.candidateFunc(e)
			},
			"enter_leader":func(e *fsm.Event) {
				defer color.Unset()
				currNode.leaderFunc(e)
			},
			"leave_state": func(e *fsm.Event) {
				defer color.Unset()
				currNode.enterState(e)
			},

		},
	)

	return currNode
}

//MAIN STATE FUNCTIONS--------------------------------
//startup promt
func (this *raftnode) startUpPromt(){
	defer color.Unset()
	Xprint.Println("[", this.node_ip,"]"," has initilized as a [follower] with timeout [", this.timeout,"]")
}

// called after each state change
func (this *raftnode) enterState(fsm_event *fsm.Event) {
	Xprint.Println("STATE TRANSITION [", this.node_ip,"]",fsm_event.Src,">>",fsm_event.Dst)
}

// called after state has changed to follower
func (this *raftnode) followerFunc(fsm_event *fsm.Event) {
	this.leaderMutex.Lock()
	termNumber:=this.termNum
	this.leaderMutex.Unlock()
	FWprint.Println("TERM >>[",termNumber,"] FOLLOWING [", this.leader_ip,"]")
	go this.timeoutToCandidate(fsm_event)
}

// called after state has changed to candidate
func (this *raftnode) candidateFunc(fsm_event *fsm.Event) {
	CDprint.Println("CANDIDATE FOR LEADERSHIP")
	CDprint.Println("INIT ELECTION")
	go this.askForVotes(fsm_event)
	go this.timeoutToFollower(fsm_event)
}

// called after state has changed to leader
func (this *raftnode) leaderFunc(fsm_event *fsm.Event) {
	LDprint.Println(" TERM",this.termNum,">>",this.FSM.Current())
	netWatcher:=this.seekChangesToNetworkStructure()
	for{
		if(this.FSM.Is("leader")){
			this.pingAllNodes(fsm_event)
			netWatcher()
			ttSleep:=MIN_TIME-NETWORK_DELAY
			sleep_time:=time.Duration(ttSleep)*time.Second
			time.Sleep(sleep_time)
		} else{
			LDprint.Println("[LEADER REVERT]")
			break;
		}
	}
}
//----------------------------------------------------

//HELPER FUNCTIONS------------------------------------
func (this *raftnode) timeoutToCandidate(fsm_event *fsm.Event){
	this.timeoutMutex.Lock()
	print_timeout:=this.timeout
	this.timeoutMutex.Unlock()
	FWprint.Println(this.node_ip,"timeout is",print_timeout);
	for{
		this.timeoutMutex.Lock()
		this.timeout-=REFRESH_RATE
		this.timeoutMutex.Unlock()
		time.Sleep(REFRESH_RATE)
		if(this.timeout<100*time.Millisecond){
			break;
		}
	}
	// STATE TRANSITION INTO THE CANDIDATE STATE
	this.FSM.Event("follower_to_candidate")
}
func (this *raftnode) timeoutToFollower(fsm_event *fsm.Event){
	time.Sleep(10*time.Second)
	if(this.FSM.Is("candidate")){
		CDprint.Println("CANDIDATE TIMED OUT")
		this.reset()
		this.FSM.Event("candidate_to_follower")
	}
}

func (this *raftnode) incomingMsgReader(){
	msg:=protobuf.RaftPayload{}
	for{
		select {
		case msg=<-this.fromCoordinator:

			if(msg.Type=="PING"){
				var newLeader =false
				this.reset()
				if(this.FSM.Is("candidate")){
					CDprint.Println("EXSISTING LEADER DETECTED")
					this.FSM.Event("candidate_to_follower")
				} else if(this.FSM.Is("leader")) {
					this.FSM.Event("leader_to_follower")
				}
				this.netMapMutex.Lock()
				err := json.Unmarshal(msg.Log, &this.netMap)
				if(err!=nil){
					ERR.Println("ERROR MAP UNLOAD")
				}

				if(debug){
					var commit,fail,join,total int
					commit=0
					join=0
					total=0
					fail=0
					for ipAddr, value := range this.netMap {
						total++
						if(value=="COMMIT"){
							commit++
						}
						if(value=="JOINED"){
							join++
							ERR.Println("NODE JOIN ",ipAddr)
						}
						if(value=="FAIL"){
							fail++
							ERR.Println("NODE FAILED ",ipAddr)
						}
					}
					ERR.Println("COMMIT:",commit," JOIN:",join," FAIL:",fail," TOTAL:",total);
				}

				this.netMapMutex.Unlock()
				this.leaderMutex.Lock()
				this.termNum=msg.TermNum;
				if(this.leader_ip=="NULL"){
					this.leader_ip=msg.SenderIP
					FWprint.Println("[INIT] FOLLOWING->",msg.SenderIP)
				} else if(this.leader_ip!=msg.SenderIP){
					this.leader_ip=msg.SenderIP
					newLeader=true;
					FWprint.Println("FOLLOWING->",msg.SenderIP)
				}
				this.leaderMutex.Unlock()
				if(newLeader){
					go this.setLeader(msg.SenderIP)
				}
			}else if(msg.Type=="VOTEASK"){
				//Xprint.Println("VOTE ASKED")
				var response string
				this.mapMutex.Lock()
				_ ,exist := this.voteTermMap[msg.TermNum]
				if exist {
					response="NO"
				} else {
					response="YES"
					this.voteTermMap[msg.TermNum]=1;
				}
				this.mapMutex.Unlock()

				electionRes:=&protobuf.RaftPayload{
					Type:                 "VOTERES",
					VoteRes:              response,
				}
				marshalled_electionRes, err :=proto.Marshal(electionRes)
				if(err!=nil){
					fmt.Println("[CRITIAL] CASTING ERROR")
				}
				this.transportPtr.R2RSend(marshalled_electionRes,msg.SenderIP)
			} else if(msg.Type=="VOTERES"){
				//CDprint.Println("VOTE RECIVED")
				if(msg.VoteRes=="YES"){
					this.voteRec+=1
				}
				if(len(this.gmsPtr.GetAllNodes())==this.voteRec){
					this.FSM.Event("candidate_to_leader")
				}
			} else if(msg.Type=="REVOLT"){
				RXprint.Println("REVOLT DETECTED")
				if(this.FSM.Is("leader")){
					this.FSM.Event("leader_to_follower")
				}
			}
		}
	}
	fmt.Println("CRITICAL ERROR, INBOUND COMMUNICATION HALTED");
}

func (this *raftnode) reset(){
	rand.Seed(time.Now().UnixNano())
	r:=rand.Intn(MAX_TIME - MIN_TIME + 1) + MIN_TIME
	timeout:=time.Duration(r) * time.Second
	this.timeoutMutex.Lock()
	this.timeout=timeout
	this.timeoutMutex.Unlock()
}

func (this *raftnode) askForVotes(fsm_event *fsm.Event){
	this.leaderMutex.Lock()
	this.termNum++;
	this.leaderMutex.Unlock()
	electionPage:=&protobuf.RaftPayload{
		Type:                 "VOTEASK",
		SenderIP:             this.node_ip,
		VoteRes:              "",
		SenderTermsCompleted: 0,
		TermNum:              this.termNum,
		Log:                  nil,
	}

	marshalled_electionPage, err :=proto.Marshal(electionPage)
	if(err!=nil){
		fmt.Println("[CRITIAL] CASTING ERROR")
	}
	nodeLists:=this.gmsPtr.GetAllNodes()



	var currPort string
	this.voteRec=1 // votes for yourself

	if(len(nodeLists)==1){
		this.FSM.Event("candidate_to_leader")
	}
	// then ask eveyone else ot vote for you
	for i := 0; i < len(nodeLists); i++ {
		currPort=parsePort(nodeLists[i],2)
		if(currPort!=this.node_ip){
			//CDprint.Println("ASKING for VOTES FROM ",currPort)
			this.transportPtr.R2RSend(marshalled_electionPage,currPort)
		}
	}
}
func (this *raftnode) pingAllNodes(fsm_event *fsm.Event){
	this.netMapMutex.Lock()
		current_net_string_struct, err :=json.Marshal(this.netMap)
	this.netMapMutex.Unlock()

	electionPage:=&protobuf.RaftPayload{
		Type:                 "PING",
		SenderIP:             this.node_ip,
		VoteRes:              "",
		SenderTermsCompleted: 0,
		TermNum:              this.termNum,
		Log:                  current_net_string_struct,
	}

	marshalled_electionPage, err :=proto.Marshal(electionPage)
	if(err!=nil){
		fmt.Println("[CRITIAL] CASTING ERROR")
	}
	nodeLists:=this.gmsPtr.GetAllNodes()
	var currPort string
	for i := 0; i < len(nodeLists); i++ {
		currPort=parsePort(nodeLists[i],2)
		if(currPort!=this.node_ip){
			this.transportPtr.R2RSend(marshalled_electionPage,currPort)
		}
	}
}

func(this *raftnode) setLeader(senderAddr string){
	time.Sleep(5*time.Second)
	var flag =false;
	this.leaderMutex.Lock()
	if(this.leader_ip!=senderAddr){
		flag=true
	}
	this.leaderMutex.Unlock()

	if(flag){
		RXprint.Println("ATTEMPTING TO ASSASSINATE THE CURRENT LEADER",senderAddr)
		revolution:=&protobuf.RaftPayload{
			Type:                 "REVOLT",
			SenderIP: 			  this.node_ip,
			VoteRes:              "",
			Log:                  nil,
		}

		marshalled_revolution, err :=proto.Marshal(revolution)
		if(err != nil){
			log.Println("[REVOLT ERR]",err)
		}
		this.transportPtr.R2RSend(marshalled_revolution,senderAddr)
	}
}
func parsePort(address string,offset int) string{
	var port_num string
	for i:=0;i< len(address);i++{
		if(address[i]== ':'){
			port_num=address[i+1:];
			casted_port, err :=strconv.Atoi(port_num)
			if(err!=nil){
				fmt.Println("ERROR CASTING")
			}
			casted_port+=offset;
			new_address:=address[:i+1]+strconv.Itoa(casted_port)
			//fmt.Println("NEW ADDRESS->",new_address)
			return new_address
		}
	}
	log.Println("[PARSE PORT ERROR]",address)
	return address
}
// THREE WAY REPLICATION ----------------------------

// sends the message informing the node about the change in the network structure
func (this *raftnode)seekChangesToNetworkStructure() func(){
	previousGmsString:=this.gmsPtr.GetAllNodes()
	if len(this.netMap) == 0 {
		for i:=0;i< len(previousGmsString);i++{
			this.netMapMutex.Lock()
			this.netMap[previousGmsString[i]]="JOINED"
			this.netMapMutex.Unlock()
		}
	}


	for i:=0;i< len(previousGmsString);i++{
		if status, found := this.netMap[previousGmsString[i]]; found {
			if(status=="JOINED"){
				this.procressJoin(previousGmsString[i])
			}
		} else {
			this.netMapMutex.Lock()
			this.netMap[previousGmsString[i]]="JOINED"
			this.netMapMutex.Unlock()
		}
	}

	// is something is int he map but not detected by the master
	// it would mean that that node has failed
	this.netMapMutex.Lock()
	for ipAddr, _ := range this.netMap {
		if(!stringInSlice(ipAddr,previousGmsString)){
			//NCPrint.Println("<>FAILED",ipAddr)
			this.netMap[ipAddr] = "FAIL"
			this.procressFail(ipAddr)
		}
	}
	this.netMapMutex.Unlock()

	return func() {
		currGmsString:=this.gmsPtr.GetAllNodes()
		if(len(previousGmsString)!= len(currGmsString)) {
			networkDifference:=difference(previousGmsString,currGmsString)
			NCPrint.Println("NETCHANGE DETECTED ",networkDifference)
			for i:=0;i< len(networkDifference);i++ {
				if status, found := this.netMap[networkDifference[i]]; found {
					if(status=="FAIL"){
						NCPrint.Println("NODE RECOVERED",networkDifference[i])
						this.netMap[networkDifference[i]] = "JOINED"
						this.procressJoin(networkDifference[i])
					} else if(status=="JOINED") {
						NCPrint.Println("UNJOINED NODE FAILED",networkDifference[i])
						this.netMap[networkDifference[i]] = "FAIL"
						this.procressFail(networkDifference[i])
					} else if(status=="COMMIT") {
						NCPrint.Println("NODE FAILED",networkDifference[i])
						this.netMap[networkDifference[i]] = "FAIL"
						this.procressFail(networkDifference[i])
					}

				} else {
					NCPrint.Println("NEW NODE JOINED",networkDifference[i])
					this.netMap[networkDifference[i]] = "JOINED"
				}
			}
			previousGmsString=currGmsString
		}
	}
}

func (this *raftnode) procressFail(string_ip string){
	//DUMMY
	predecessor_ip:=this.replication.FindPredecessorNode(string_ip)
	jointype:="FAIL"
	FailOption:="PREDECESSOR"

	fail_Payload_predecessor:=&protobuf.InternalMsg{
		MessageID:                   nil,
		Command:                     69,
		JoinType:                    &jointype,
		FailOption:                  &FailOption,
	}
	payload, err :=proto.Marshal(fail_Payload_predecessor)
	if err != nil{
		log.Println("[PAYLOAD ERROR]")
	}

	NCPrint.Println("PREDECESSOR FAIL REQ",string_ip," TO ",predecessor_ip)
	this.transportPtr.ReplicationRequest(payload,parsePort(predecessor_ip,1))// 1 offset for TCP PORT

	// now sending it to the grand predecessor GRAND_PREDECESSOR
	grand_FailOption:="GRAND_PREDECESSOR"
	grand_predecessor_ip:=this.replication.FindPredecessorNode(predecessor_ip)

	fail_Payload_predecessor=&protobuf.InternalMsg{
		MessageID:                   nil,
		Command:                     69,
		JoinType:                    &jointype,
		FailOption:                  &grand_FailOption,
	}
	payload, err =proto.Marshal(fail_Payload_predecessor)
	if err != nil{
		log.Println("[PAYLOAD ERROR]")
	}

	NCPrint.Println("GRAND_PREDECESSOR FAIL REQ",string_ip," TO ",grand_predecessor_ip)
	this.transportPtr.ReplicationRequest(payload,parsePort(grand_predecessor_ip,1))// 1 offset for TCP PORT

}

func (this *raftnode) procressJoin(string_ip string){
	//DUMMY
	destination:=this.replication.FindPredecessorNode(string_ip)
	var jointype *string;
	jointype=new(string)
	*jointype="JOINED"
	FailOption:="NULL"

	joinPayload:=protobuf.InternalMsg{
		MessageID:                   nil,
		Command:                     69,
		JoinType:                    jointype,
		FailOption:                  &FailOption,
	}
	payload, err :=proto.Marshal(&joinPayload)
	if err != nil{
		log.Println("[PAYLOAD ERROR]")
	}

	NCPrint.Println("SENDING ",*joinPayload.JoinType," REQUEST OF",string_ip," TO ",destination)
	this.transportPtr.ReplicationRequest(payload,parsePort(destination,1))// 1 offset for TCP PORT
	this.netMapMutex.Lock()
	this.netMap[string_ip]="COMMIT"
	this.netMapMutex.Unlock()
}
//https://stackoverflow.com/questions/19374219/how-to-find-the-difference-between-two-slices-of-strings
func difference(slice1 []string, slice2 []string) []string {
	var diff []string

	// Loop two times, first to find slice1 strings not in slice2,
	// second loop to find slice2 strings not in slice1
	for i := 0; i < 2; i++ {
		for _, s1 := range slice1 {
			found := false
			for _, s2 := range slice2 {
				if s1 == s2 {
					found = true
					break
				}
			}
			// String not found. We add it to return slice
			if !found {
				diff = append(diff, s1)
			}
		}
		// Swap the slices, only if it was the first loop
		if i == 0 {
			slice1, slice2 = slice2, slice1
		}
	}

	return diff
}

//https://stackoverflow.com/questions/15323767/does-go-have-if-x-in-construct-similar-to-python
func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}