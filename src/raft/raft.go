package raft

import (
	"dht/google_protocol_buffer/pb/protobuf"
	"dht/src/membership"
	"dht/src/transport"
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



type node struct {
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
}

// TIMER VARIABLES---------------------
//maximum netowkr down time is 20 seconds
var MIN_TIME = 2
var MAX_TIME = 10
var REFRESH_RATE =500*time.Millisecond
var NETWORK_DELAY=1 // added the network delay veriable
var GAIN=1*time.Second
//-------------------------------------
// COLOR MESSAGES-----------------------
var LDprint = color.New(color.FgBlue).Add(color.ReverseVideo)
var CDprint=color.New(color.FgYellow)
var FWprint=color.New(color.FgMagenta).Add(color.ReverseVideo)
var Xprint=color.New(color.ReverseVideo)
var RXprint=color.New(color.FgRed).Add(color.ReverseVideo)
var ERR=color.New(color.FgYellow).Add(color.ReverseVideo)
//-------------------------------------

//INIT-----------------------------------------

func New(	gmsPtr *membership.MembershipService,
	transportPtr *transport.TransportModule,
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
	raftStateMachine.startUpPromt()
	raftStateMachine.voteRec=0
	raftStateMachine.voteTermMap= make(map[int32]int32)
	raftStateMachine.termNum=0;
	err := raftStateMachine.FSM.Event("init")
	if err != nil {
		fmt.Println(err)
	}
	go raftStateMachine.incomingMsgReader();
}
//reference from
//https://github.com/looplab/fsm/blob/v0.2.0/fsm.go#L88
func RaftStateMachine(coordinatorToRaft chan protobuf.RaftPayload,node_ip_add string,_timeout time.Duration) *node {
	currNode := &node{
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
				CDprint.Println("EXISTING LEADER DETECTED")
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
func (this *node) startUpPromt(){
	defer color.Unset()
	Xprint.Println("[", this.node_ip,"]"," has initilized as a [follower] with timeout [", this.timeout,"]")
}

// called after each state change
func (this *node) enterState(fsm_event *fsm.Event) {
	Xprint.Println("STATE TRANSITION [", this.node_ip,"]",fsm_event.Src,">>",fsm_event.Dst)
}

// called after state has changed to follower
func (this *node) followerFunc(fsm_event *fsm.Event) {
	this.leaderMutex.Lock()
	termNumber:=this.termNum
	this.leaderMutex.Unlock()
	FWprint.Println("TERM >>[",termNumber,"] FOLLOWING [", this.leader_ip,"]")
	go this.timeoutToCandidate(fsm_event)
}

// called after state has changed to candidate
func (this *node) candidateFunc(fsm_event *fsm.Event) {
	CDprint.Println("CANDIDATE FOR LEADERSHIP")
	CDprint.Println("INIT ELECTION")
	go this.askForVotes(fsm_event)
	go this.timeoutToFollower(fsm_event)
}

// called after state has changed to leader
func (this *node) leaderFunc(fsm_event *fsm.Event) {
	LDprint.Println("[LEADER NODE]")
	LDprint.Println("[OUTBOUND]->PINGING")
	LDprint.Println(" TERM",this.termNum,">>",this.FSM.Current())
	for{
		if(this.FSM.Is("leader")){
			this.pingAllNodes(fsm_event)
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
func (this *node) timeoutToCandidate(fsm_event *fsm.Event){
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
func (this *node) timeoutToFollower(fsm_event *fsm.Event){
	time.Sleep(10*time.Second)
	this.reset()
	// STATE TRANSITION INTO THE CANDIDATE STATE
	if(this.FSM.Is("candidate")){
		this.FSM.Event("candidate_to_follower")
	}
}

func (this *node) incomingMsgReader(){
	Xprint.Println("MSG READER INIT")
	msg:=protobuf.RaftPayload{}
	for{
		select {
		case msg=<-this.fromCoordinator:

			if(msg.Type=="PING"){
				var newLeader =false
				this.reset()
				if(this.FSM.Is("candidate")){
					this.FSM.Event("candidate_to_follower")
				} else if(this.FSM.Is("leader")) {
					this.FSM.Event("leader_to_follower")
				}
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

func (this *node) reset(){
	rand.Seed(time.Now().UnixNano())
	r:=rand.Intn(MAX_TIME - MIN_TIME + 1) + MIN_TIME
	timeout:=time.Duration(r) * time.Second
	this.timeoutMutex.Lock()
	this.timeout=timeout
	this.timeoutMutex.Unlock()
}

func (this *node) askForVotes(fsm_event *fsm.Event){
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
		currPort=parsePort(nodeLists[i])
		if(currPort!=this.node_ip){
			//CDprint.Println("ASKING for VOTES FROM ",currPort)
			this.transportPtr.R2RSend(marshalled_electionPage,currPort)
		}
	}
}
func (this *node) pingAllNodes(fsm_event *fsm.Event){
	electionPage:=&protobuf.RaftPayload{
		Type:                 "PING",
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
	for i := 0; i < len(nodeLists); i++ {
		currPort=parsePort(nodeLists[i])
		if(currPort!=this.node_ip){
			this.transportPtr.R2RSend(marshalled_electionPage,currPort)
		}
	}
}

func(this *node) setLeader(senderAddr string){
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
func parsePort(address string) string{
	var port_num string
	for i:=0;i< len(address);i++{
		if(address[i]== ':'){
			port_num=address[i+1:];
			casted_port, err :=strconv.Atoi(port_num)
			if(err!=nil){
				fmt.Println("ERROR CASTING")
			}
			casted_port+=2;
			new_address:=address[:i+1]+strconv.Itoa(casted_port)
			//fmt.Println("NEW ADDRESS->",new_address)
			return new_address
		}
	}
	return "127.0.0.1:3000" // dummy port
}