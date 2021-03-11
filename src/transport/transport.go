package transport

import (
	"dht/google_protocol_buffer/pb/protobuf"
	"fmt"
	"hash/crc32"
	"log"
	"net"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pmylund/go-cache"
)

//REFRENCES--------------------------------
//https://stackoverflow.com/questions/28400340/how-support-concurrent-connections-with-a-udp-server-using-go
//https://stackoverflow.com/questions/27625787/measuring-memory-usage-of-executable-run-using-golang
//https://stackoverflow.com/questions/31879817/golang-os-exec-realtime-memory-usage
//https://golangcode.com/print-the-current-memory-usage/

/*
 * ARCHITECTURE---------------------------------------------
 * ┌────────────────────────────────────────────────────────┐
 * │                                                        │
 * │         Transport layer                                │
 * │                                                        │
 * │                                                        │
 * │     UDP connetion(atmost once)   TCP connection        │
 * │                                                        │
 * │      proto.MSG object            server to server      │
 * │                                                        │
 * │                                  WRAPPER               │
 * │                                                        │
 * │    ┌─────────────────┐         ┌────────────────────┐  │
 * │    │                 │         │                    │  │
 * │    │   Server to     │         │   Server to        │  │
 * │    │                 │         │                    │  │
 * │    │    Client       │         │     Server         │  │
 * │    └───────────────┬─┘         └────────────────┬───┘  │
 * │        PORT 7262   │                 PORT 7272  │      │
 * └──────────────▲─────┼─────────────────────▲──────┼──────┘
 *                │     │                     │      │
 *                │     │                     │      │
 *                      ▼                     		 ▼
 *
 */

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
var LOCAL_PORT string
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
	connection      *net.UDPConn
	S2Sconnection   *net.TCPListener
	heartbeatChanel chan []byte
	coodinatorChan  chan protobuf.InternalMsg
}

// initilaizes the transport layer
func New(ip string, port int, gmsChan chan []byte, StorageChan chan protobuf.InternalMsg) (*TransportModule, error) {

	LOCAL_PORT = strconv.Itoa(port)
	S2S_PORT := port + 1
	tm := &TransportModule{}
	if !validPort(port) {
		return tm, nil
	}

	conn := listen(ip, port)
	s2sconn := S2S_TCPlisten(ip, S2S_PORT)
	tm.coodinatorChan = StorageChan
	tm.connection = conn
	tm.heartbeatChanel = gmsChan
	tm.S2Sconnection = s2sconn
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
func S2S_TCPlisten(selfIP string, port int) *net.TCPListener {
	address, err := net.ResolveTCPAddr("tcp", selfIP+":"+strconv.Itoa(port))
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
func listen(selfIP string, port int) *net.UDPConn {
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
func (tm *TransportModule) Init_server() {
	if MULTI_CORE_MODE {
		fmt.Println("[MULTICORE MODE] [", runtime.NumCPU(), "] SPANNERS AT PORT [", tm.connection.LocalAddr(), "] SYS MEM LIMIT [", MEMORY_LIMIT, "]")
	}

	//go proportionalCollector()
	// tm.daemonSpawner()
	go tm.UDP_daemon()

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
		//if numStorageMessages%500 == 0 {
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
			ClientAddr: clientAddr,
			OriginAddr: "",
			Payload:    payload,
			Message:    messageID,
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
//
func (tm *TransportModule) nodeReq(node2nodePayload []byte, messageID []byte) {
	node2nodeMsg := &protobuf.InternalMsg{
		ClientAddr: "",
		OriginAddr: "",
		Payload:    nil,
		Message:    nil,
	}
	proto.Unmarshal(node2nodePayload, node2nodeMsg)
	clientAddr := node2nodeMsg.GetClientAddr()
	cachedResponse, found := check_cache(messageID)
	if found {
		tm.Send(cachedResponse, messageID, clientAddr)
	} else {
		tm.coodinatorChan <- *node2nodeMsg
	}
}

// this is a function to reposnd to a request form a different node
func (tm *TransportModule) nodeRes(node2nodePayload []byte, messageID []byte) {
	// send the internalPayload to the client
	node2nodeMsg := &protobuf.InternalMsg{}
	proto.Unmarshal(node2nodePayload, node2nodeMsg)

	response := node2nodeMsg.GetPayload()

	cache_data(messageID, response)
	tm.Send(response, messageID, node2nodeMsg.GetClientAddr())
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

func (tm *TransportModule) SendHeartbeat(payload []byte, messageID []byte, destAddr string) {
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

func (tm *TransportModule) SendStorageToStorage(payload []byte, messageID []byte, destAddr string) {
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

func (tm *TransportModule) CachedSendStorageToStorage(payload []byte, messageID string, destAddr string) {
	cache_data([]byte(messageID), payload)
	tm.Send(payload, []byte(messageID), destAddr)

}

func (tm *TransportModule) TCP_Daemon() {
	for {
		buffer := make([]byte, 20100)
		conn, err := tm.S2Sconnection.Accept()
		if err != nil {
			log.Println("[NORMAL] TCP READ ERROR > TCP_Daemon")
		}
		n, err := conn.Read(buffer)
		if err != nil {
			log.Println("[NORMAL] TCP >CONN< READ ERROR > TCP_Daemon")
		}

		tm.S2S_route_to_coodinator(buffer[:n])
	}
}

func (tm *TransportModule) S2S_route_to_coodinator(payload []byte) {
	inbound_payload := &protobuf.S2S{
		Payload:     nil,
		WhichModule: "",
	}
	err := proto.Unmarshal(payload, inbound_payload)
	if err != nil {
		log.Println("[CRITICAL] UNMARSHELLING ERROR S2S_route_to_coodinator")
	}

	node2nodeMsg := &protobuf.InternalMsg{
		ClientAddr: "",
		OriginAddr: "",
		Payload:    nil,
		Message:    nil,
	}
	proto.Unmarshal(inbound_payload.Payload, node2nodeMsg)
	tm.coodinatorChan <- *node2nodeMsg
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

// TODO: sendHeartbeat(destination, payload) UDP
// TODO: sendStorageToClient() // done UDP
// TODO: sendStorageToStorage()// done UDP
//TODO: sendCoordinatorToCoordinator() // done UDP
//TODO: sendReplicationToReplication() // done TCP

//TODO: channels
//GMS (heartbeat message) DONE
//Coordinator ? so whatever comes to storage wire it to coodinator done
