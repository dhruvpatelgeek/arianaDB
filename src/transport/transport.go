package transport

import (
	guuid "github.com/google/uuid"
	"dht/google_protocol_buffer/pb/protobuf"
	"dht/src/constants"
	"errors"
	"fmt"
	"hash/crc32"
	"log"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"github.com/pmylund/go-cache"
)

//REFRENCES--------------------------------
//https://stackoverflow.com/questions/28400340/how-support-concurrent-connections-with-a-udp-server-using-go
//https://stackoverflow.com/questions/27625787/measuring-memory-usage-of-executable-run-using-golang
//https://stackoverflow.com/questions/31879817/golang-os-exec-realtime-memory-usage
//https://golangcode.com/print-the-current-memory-usage/

// memory debugger
var numStorageMessages = 0
var numGMSMessages = 0
var numRerouteMessages = 0
var numGossipMessages = 0

//-----------------------------------------
//CONTROL PANEL----------------------------
var debug = false
var MEMORY_LIMIT = 51200
var MULTI_CORE_MODE = true
var MAP_SIZE_MB = 70
var CACHE = 10
var CACHE_LIFESPAN = 5 // how long should the cache persist
var SERVER_TO_SERVER_TIMEOUT = 5 * time.Second

//CONNECTION-------------------------------

// message struct for communicatino with the storage layer
type Message struct {
	ClientAddr string
	Payload    []byte
	MessageID  string
}

// message struct for communicating with the transport module
type TransportModule struct {
	connection                 *net.UDPConn
	storageToStorageConnection *net.TCPListener
	R2Rconnection   		    *net.UDPConn

	heartbeatChanel chan []byte
	coodinatorChan  chan protobuf.InternalMsg
	storageChannel  chan protobuf.InternalMsg
	raftChan chan protobuf.RaftPayload

	hostIP               string
	clientToServerPort   string
	storageToStoragePort string
	hostIPv4             string
}

// initilaizes the transport layer
func New(ip string, clientToServerPort int, gmsChan chan []byte, coordinatorChannel chan protobuf.InternalMsg, storageChannel chan protobuf.InternalMsg,raftChan chan protobuf.RaftPayload) (*TransportModule, error) {
	tm := &TransportModule{}
	if !validPort(clientToServerPort) {
		return tm, nil
	}

	tm.hostIP = ip
	tm.clientToServerPort = strconv.Itoa(clientToServerPort)
	tm.storageToStoragePort = strconv.Itoa(clientToServerPort + 1)
	tm.hostIPv4 = ip + ":" + tm.storageToStoragePort
	R2R_PORT := clientToServerPort + 2
	r2rconn:=createUDPConnection(ip,R2R_PORT)
	tm.connection = createUDPConnection(ip, clientToServerPort)
	tm.storageToStorageConnection = createTCPConnection(ip, tm.storageToStoragePort) // TODO: wtf is this?
	tm.R2Rconnection=r2rconn
	tm.coodinatorChan = coordinatorChannel
	tm.heartbeatChanel = gmsChan
	tm.storageChannel = storageChannel
	tm.raftChan=raftChan
	go tm.bootstrap()

	// TODO: listen to TCP
	go proportionalCollector()

	return tm, nil

}

// send function for sending a payload
func (tm *TransportModule) Send(payload []byte, messageID []byte, destAddr string) {
	message, err := generateShell(payload, messageID)

	if err != nil {
		fmt.Println("payload gen failed")
	}

	msg := &protobuf.Msg{}
	error := proto.Unmarshal(message, msg)

	if error != nil {
		log.Println("Unable to deserialize ", error)
	}

	addr, err := net.ResolveUDPAddr("udp", destAddr)

	if err != nil {
		log.Println("Address error ", err)
	}

	tm.connection.WriteToUDP(message, addr)
}

//------------------------------------------

//optimizations----------------------------
// auto scaling collector that adjusts its collection rate wrt how much memory it has left
// like a P controller in control systems engineering
// the proportionalCollector will increase the number of GC calls when the memory is low
func proportionalCollector() {
	var sleepCtr time.Duration = 1000 * time.Millisecond
	for {
		time.Sleep(sleepCtr)
		sleepCtr = sleepCurve()
		runtime.GC()
	}
}

// helper funciton for proportionalCollector
// sets the GC sleep rate
func sleepCurve() time.Duration {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	currentMem := bToMb((m.Alloc))
	//fmt.Println(currentMem)
	if currentMem < 20 {
		return 5 * time.Second
	} else if currentMem < 30 {
		return 100 * time.Millisecond
	} else if currentMem < 40 {
		return 50 * time.Millisecond
	} else if currentMem < 50 {
		return 10 * time.Millisecond
	} else if currentMem < 80 {
		//fmt.Println("CRITICAL MEMORY")
		return time.Millisecond
	} else {
		//fmt.Println("[SUPER] CRITICAL MEMORY")
		return 100 * time.Microsecond
	}
}

//-----------------------------------------

//CACHE----------------------------

// Create a cache with a default expiration time of 5 seconds, and which
// purges expired items every 1 seconds
var message_cache = cache.New(5*time.Second, 1*time.Nanosecond)

// check if the data is in the cache if it is return true and the cache value
func check_cache(message_id []byte) ([]byte, bool) {
	if debug {
		fmt.Println("checking cache...")
	}
	response, found := message_cache.Get(string(message_id))

	if !found {
		if debug {
			fmt.Println("not found...")
		}
		return nil, false
	} else {
		if debug {
			fmt.Println("found...")
		}
		str := fmt.Sprintf("%v", response)
		var cached_data = []byte(str)

		return cached_data, true
	}
}

// puts the data into the cache
func cache_data(message_id []byte, data []byte) bool {
	if get_mem_usage() > uint64(MEMORY_LIMIT-20) {
		return true
		fmt.Println("\n[MEMORY WARNING]\n")
		message_cache.Flush()
	}
	if get_mem_usage() > uint64(MEMORY_LIMIT/2) {
		fmt.Println("\n[MEMORY WARNING 50%]\n")
		message_cache.DeleteExpired()
	}

	message_cache.Set(string(message_id), string(data), cache.DefaultExpiration)
	return true
}

//-----------------------------------------

//HELPER FUNCTIONS-------------------------

// checks if the port is valid
func validPort(port int) bool {
	return (port < 65535) || (port > 0)
}

// return the IEEE checksums of byte a+b
func calculate_checksum(a []byte, b []byte) uint64 {
	var concat_byte_arr = append(a, b...)
	var check_sum = uint64(crc32.ChecksumIEEE(concat_byte_arr))
	return check_sum
}

//-----------------------------------------

//UDP SERVER FUNC--------------------------

// a udp background function that reads the udp  byte buffer and calls the router
func (tm *TransportModule) UDP_daemon() {

	for {
		buffer := make([]byte, 20100)
		n, remoteAddr, err := tm.connection.ReadFromUDP(buffer)
		for err != nil {
			fmt.Println("listener failed - ", err)
		}

		tm.router(buffer[:n], remoteAddr.String())
	}

}
func createTCPConnection(selfIP string, port string) *net.TCPListener {
	address, err := net.ResolveTCPAddr("tcp", selfIP+":"+port)
	if err != nil {
		fmt.Errorf("Unable to resolve tcp address (ip: %s, port: %d", selfIP, port)
	}
	connection, err := net.ListenTCP("tcp", address)
	if err != nil {
		fmt.Printf("[TCP ERR FTL>S2S_TCPlisten ]%+v", err)
	}
	fmt.Println("S2S ADDRESS IS", address.IP, ":", address.Port)

	return connection
}

// initalizes a UDP listern on the given port
// and returns the conneciton object
func createUDPConnection(selfIP string, port int) *net.UDPConn {
	var err error

	address, err := net.ResolveUDPAddr("udp", selfIP+":"+strconv.Itoa(port))
	if err != nil {
		fmt.Errorf("Unable to resolve udp address (ip: %s, port: %d", selfIP, port)
	}
	connection, err := net.ListenUDP("udp", address)
	if err != nil {
		fmt.Printf("[UDP [fwef]]%+v", err)
	}
	err = connection.SetWriteBuffer(20000)
	if err != nil {
		fmt.Printf("[WRITE SETTING ERROR]%+v", err)
	}

	err = connection.SetReadBuffer(20000)
	if err != nil {
		fmt.Printf("[READ SETTING ERROR]%+v", err)
	}

	fmt.Println("ADDRESS IS", address.IP, ":", address.Port)

	return connection
}

//initialises the server based on port number
func (tm *TransportModule) bootstrap() {
	if MULTI_CORE_MODE {
		fmt.Println("[MULTICORE MODE] [", runtime.NumCPU(), "] SPANNERS AT PORT [", tm.connection.LocalAddr(), "] SYS MEM LIMIT [", MEMORY_LIMIT, "]")
	}

	go tm.UDP_daemon()
	go tm.processStorageToStorageMessages() // TODO: come up with better names for these daemons...
	go tm.R2R_daemon()
}

//for debug to view packet b4 sending
func double_check(arr []byte) {
	cast_whole_req := &protobuf.Msg{
		MessageID: nil,
		Payload:   nil,
		CheckSum:  0,
	}

	error := proto.Unmarshal(arr, cast_whole_req)
	if error != nil {
		fmt.Printf("\nUNPACK ERROR[W2] %+v\n", error)
	}

	if cast_whole_req.CheckSum != calculate_checksum(cast_whole_req.MessageID, cast_whole_req.Payload) {
		fmt.Printf("\n[CHECKSUM WRONG NO RESPONSE SENT] %+v\n", error)
	}

	cast_req := &protobuf.KVResponse{}
	error = proto.Unmarshal(cast_whole_req.Payload, cast_req)
	if error != nil {
		fmt.Printf("\nUNPACK ERROR %+v\n", error)
	}

}

//copied form
//https://golang.org/pkg/runtime/#MemStats

// Description: returns current sys mem usage
func get_mem_usage() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	if debug {
		//fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
		//fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
		//fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	}
	return bToMb(m.Sys)
}

//for memory profiling
// prints the current memory usage
func PrintMemUsage() {
	for {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		// For info on each, see: https://golang.org/pkg/runtime/#MemStats
		fmt.Printf("\r Alloc = %v MiB ", bToMb(m.Alloc))
		fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
		fmt.Printf("\tNumGC = %v\n \r", m.NumGC)

		time.Sleep(500 * time.Millisecond)
	}
}

// converts megabytes to bytes
func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

// warps the payload in the Message protocol buffer
func generateShell(payload []byte, messageId []byte) ([]byte, error) {
	checksum := calculate_checksum([]byte(messageId), payload)
	shell := &protobuf.Msg{
		MessageID: messageId,
		Payload:   payload,
		CheckSum:  checksum,
	}
	marshelledShell, err := proto.Marshal(shell)

	if err != nil {
		fmt.Println("[crtitcal] protobuf marshall error")
	}

	return marshelledShell, err
}

// uses the messageID to route the message to different modules
// if message is prepended with
// gossip-> send to the group membership
// reques-> send to node request
// if the message is nno prepended with anything send to storage layer to be procressed
func (tm *TransportModule) router(serialMsg []byte, clientAddr string) {

	msg := &protobuf.Msg{}
	proto.Unmarshal(serialMsg, msg)

	messageID := msg.GetMessageID()
	payload := msg.GetPayload()

	if len(messageID) > 6 {
		switch string(messageID[0:6]) {
		case "gossip": //gossip pre pend
			tm.gossipPrepend(payload, clientAddr)
		case "reques": // node req prepend
			tm.nodeReq(payload, messageID[6:])
		default: // no pre pend
			tm.clientReq(payload, messageID, clientAddr)
		}
	} else {
		tm.clientReq(payload, messageID, clientAddr)
	}
}

// for handelling  a client request
// checks if the message is cached if it is send the respsonse
// else send it to the sotrage layer to be procressed
func (tm *TransportModule) clientReq(payload []byte, messageID []byte, clientAddr string) {
	cachedResponse, found := check_cache(messageID)
	if found {
		tm.Send(cachedResponse, messageID, clientAddr)
	} else {

		internal_message_obj := protobuf.InternalMsg{
			ClientAddress: &clientAddr,
			KVRequest:     payload,
			MessageID:     messageID,
			Command:       uint32(constants.ProcessClientKVRequest), // TODO: unable
		}

		tm.coodinatorChan <- internal_message_obj

	}
}

// forward the message to the group membership module
func (tm *TransportModule) gossipPrepend(payload []byte, clientAddr string) {
	tm.heartbeatChanel <- payload
}

// this is the funciton call if the recived message is fomr an other node
// in this case  will see if it is cached
func (tm *TransportModule) nodeReq(node2nodePayload []byte, messageID []byte) {
	/* TODO: this needs to change
	1. this assumes every internal message sent will be sent to a client
	2. now, we send internal messages that have nothing to do with the client.
		- maybe the internal message should specify the type: ClientRequest vs ServerRequest
		- check cache & respond if this is a client request
		- forward directly to coordinatorChannel  if this is a server request
	*/

	node2nodeMsg := &protobuf.InternalMsg{}
	proto.Unmarshal(node2nodePayload, node2nodeMsg)
	// TODO: create a field in InternalMsg for ResponseRequired bool?
	clientAddr := node2nodeMsg.GetClientAddress() // TODO: this is now an optional arg since an internal message
	cachedResponse, found := check_cache(messageID)
	if found {
		tm.Send(cachedResponse, messageID, clientAddr)
	} else {
		tm.coodinatorChan <- *node2nodeMsg
	}
}

//https://stackoverflow.com/questions/23558425/how-do-i-get-the-local-ip-address-in-go
//returns the local address of the machine
func GetLocalAddr() string {

	arguments := os.Args
	portInt, err := strconv.Atoi(arguments[1])
	if err != nil {
		fmt.Println(err)
	}
	y := 2 + portInt - 4001
	address := "172.17.0." + strconv.Itoa(y) + ":" + arguments[1]
	// fmt.Println(address)
	// address = "127.0.0.1:"+arguments[1]

	return address
}

// send the messages but caches the response before sending it
func (tm *TransportModule) ResSend(payload []byte, messageID string, destAddr string) {
	cache_data([]byte(messageID), payload)
	tm.Send(payload, []byte(messageID), destAddr)

}

// NEW FUNCTION---------------------------------------------
func (tm *TransportModule) SendReplicationToReplication(payload []byte, destAddr string) {
	outbound_shell := &protobuf.S2S{
		Payload:     payload,
		WhichModule: "replication",
	}
	marshalled_outbound_shell, err := proto.Marshal(outbound_shell)
	if err != nil {
		log.Println("[CRITICAL] PACKEGE GEN ERROR > serverToServer_STORE")
	}
	tm.TCPSend(marshalled_outbound_shell, destAddr)
}

func (tm *TransportModule) TCPSend(payload []byte, destAddr string) {
	if debug {
		fmt.Println("REQ >", destAddr)
	}
	timeoutDuration := SERVER_TO_SERVER_TIMEOUT
	conn, err := net.Dial("tcp4", destAddr)
	defer conn.Close() // close this object after doing this call

	err = conn.SetWriteDeadline(time.Now().Add(timeoutDuration))
	if err != nil {
		log.Println("[NORMAL] TCP SEND ERR > TCPSend")
	}
	_, err = conn.Write(payload)
	if err != nil {
		log.Println("[NORMAL] TCP WRITE ERROR > TCPSend")
	}
}

func (tm *TransportModule) SendHeartbeat(heartbeat *protobuf.MembershipReq, destAddr string) error {
	// TODO: marshal membership request
	serializedHeartbeat, err := proto.Marshal(heartbeat)
	if err != nil {
		fmt.Errorf("[Transport] Failed to marshal hearbeat message")
		return err
	}
	messageID := []byte("gossip" + uuid.New().String())

	message, err := generateShell(serializedHeartbeat, messageID)
	if err != nil {
		fmt.Println("payload gen failed")
		return err
	}

	addr, err := net.ResolveUDPAddr("udp", destAddr)
	if err != nil {
		log.Println("Address error ", err)
	}

	tm.connection.WriteToUDP(message, addr)
	return nil
}

func (tm *TransportModule) SendStorageToStorage(storageMessage *protobuf.InternalMsg, destAddr string) error {
	// TODO: consider using a separate port
	// TODO: consider using a TCP port for higher throughput in milestone 3
	serializedMessage, err := proto.Marshal(storageMessage)
	if err != nil {
		return err
	}

	// obtain the remote's S2S TCP port
	splitDestinationAddress := strings.Split(destAddr, ":")
	if len(splitDestinationAddress) != 2 {
		return errors.New("destination address (" + destAddr + ") did not contain IP:PORT ")
	}
	destinationAddress := splitDestinationAddress[0]
	destinationClientPort, err := strconv.Atoi(splitDestinationAddress[1])
	if err != nil {
		return err
	}
	destinationStorageToStoragePort := strconv.Itoa(destinationClientPort + 1) // TODO: create a function for this to remove duplicated magic constants
	destinationIPv4 := destinationAddress + ":" + destinationStorageToStoragePort

	s2sConnection, err := net.Dial("tcp", destinationIPv4)
	defer s2sConnection.Close()
	if err != nil {
		fmt.Println("FAILED TO DIAL TCP")
		fmt.Println(err)
		return errors.New("Unable to open a S2S TCP connection to " + destinationIPv4)
	}

	// send data
	_, err = s2sConnection.Write(serializedMessage)
	return err
}

func (tm *TransportModule) CachedSendStorageToStorage(payload []byte, messageID string, destAddr string) {
	cache_data([]byte(messageID), payload)
	tm.Send(payload, []byte(messageID), destAddr)

}

func (tm *TransportModule) processStorageToStorageMessages() {
	defer tm.storageToStorageConnection.Close()

	for {
		buffer := make([]byte, 20100)
		conn, err := tm.storageToStorageConnection.Accept()
		if err != nil {
			log.Println(err)
			continue
		}

		n, err := conn.Read(buffer)
		if err != nil {
			log.Println("[Transport] Unable to read the value from the storage-to-storage connection.")
			continue
		}

		payload := buffer[:n]
		internalMessage := &protobuf.InternalMsg{}
		err = proto.Unmarshal(payload, internalMessage)

		if err != nil {
			log.Println("[Transport] Unable to marshal a storage-to-storage request. Ignoring this message")
			continue
		}

		tm.storageChannel <- *internalMessage
	}
}

func (tm *TransportModule) SendCoordinatorToCoordinator(payload []byte, messageID []byte, destAddr string) {
	message, err := generateShell(payload, messageID)
	if err != nil {
		fmt.Println("payload gen failed")
	}

	msg := &protobuf.Msg{}
	error := proto.Unmarshal(message, msg)
	if error != nil {
		log.Println("Unable to deserialize ", error)
	}

	addr, err := net.ResolveUDPAddr("udp", destAddr)
	if err != nil {
		log.Println("Address error ", err)
	}

	tm.connection.WriteToUDP(message, addr)
}

//RAFT FUNCTIONS---------------------------------------------------------

// a udp background function that reads the udp  byte buffer and calls the router
func (tm *TransportModule) R2R_daemon() {

	for {
		buffer := make([]byte, 20100)
		n, _, err := tm.R2Rconnection.ReadFromUDP(buffer)
		for err != nil {
			fmt.Println("listener failed - ", err)
		}
		casted_R2R:=&protobuf.RaftShell{
			Message_ID: nil,
			Checksum:   nil,
			Payload:    nil,
			Type:       "",
		}
		err = proto.Unmarshal(buffer[:n], casted_R2R)
		if(err!=nil){
			fmt.Println("[R2R CASITNG ERROR shell]",err,buffer[:n])
		}
		fmt.Println("[CASTED R2R]",casted_R2R)
		if string(casted_R2R.Checksum)!= strconv.FormatUint(calculate_checksum(casted_R2R.Message_ID, casted_R2R.Payload), 10) {
			fmt.Println("[CHECKSUM ERR R2R]")
		} else {
			raftPayload:=&protobuf.RaftPayload{}
			err = proto.Unmarshal(casted_R2R.Payload, raftPayload)
			if(err!=nil){
				fmt.Println("[R2R CASITNG ERROR payload]")
			}
			tm.raftChan<-*raftPayload
		}
	}

}

func (tm *TransportModule) R2RSend(payload []byte,destAddr string){
	id := guuid.New()
	messageID:=[]byte(id.String())

	send_payload:=&protobuf.RaftShell{
		Message_ID: messageID,
		Checksum:   []byte(strconv.FormatUint(calculate_checksum(messageID, payload), 10)),
		Payload:    payload,
		Type:       "general",
	}
	addr, err := net.ResolveUDPAddr("udp", destAddr)
	fmt.Println("SENDING MESSAGE......")
	fmt.Println(send_payload)

	if err != nil {
		log.Println("Address error ", err)
	}
	marshalled_send_payload,err:=proto.Marshal(send_payload)
	if(err!=nil){
		fmt.Println("[CRITICAL] Casting error R2RSend")
	}
	tm.connection.WriteToUDP(marshalled_send_payload,addr )
}
//--------------------------------------------------------------------------