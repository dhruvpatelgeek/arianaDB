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

//CONNECTION-------------------------------

type Message struct {
	ClientAddr string
	Payload    []byte
	MessageID  string
}

type TransportModule struct {
	connection  *net.UDPConn
	GroupSend   chan []byte
	StorageChan chan protobuf.InternalMsg
}

func New(ip string, port int, gmsChan chan []byte, StorageChan chan protobuf.InternalMsg) (*TransportModule, error) {

	LOCAL_PORT = strconv.Itoa(port)

	tm := &TransportModule{}
	if !validPort(port) {
		return tm, nil
	}

	conn := listen(ip, port)

	tm.StorageChan = StorageChan
	tm.connection = conn
	tm.GroupSend = gmsChan

	go proportionalCollector()

	//go tm.daemonSpawner()

	return tm, nil

}

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

	//log.Println("Sending ", destAddr)

	tm.connection.WriteToUDP(message, addr)
}

/*
var connection *net.UDPConn

var GroupSend = make(chan Message)
*/
//------------------------------------------

//optimizations----------------------------

func proportionalCollector() {
	var sleepCtr time.Duration = 1000 * time.Millisecond
	for {
		//log.Println("GC")
		time.Sleep(sleepCtr)
		sleepCtr = sleepCurve()
		runtime.GC()
	}
}

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

/**
 * @Description:  check if the data is in the cache
 * @param message_id
 * @return []byte
 * @return bool
 */
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

/**
 * @Description: put data in the cache
 * @param message_id
 * @param data
 * @return bool
 */
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
/**
 * @Description: checks if port is valid
 * @param port
 * @return bool
 */
func validPort(port int) bool {
	return (port < 65535) || (port > 0)
}

/**
 * @Description:  returns the ieee checksum
 * @param a
 * @param b
 * @return uint64
 */
func calculate_checksum(a []byte, b []byte) uint64 {
	var concat_byte_arr = append(a, b...)
	var check_sum = uint64(crc32.ChecksumIEEE(concat_byte_arr))
	return check_sum
}

//-----------------------------------------

//UDP SERVER FUNC--------------------------
/**
 * @Description:  go routine to serve a client
 * @param connection
 * @param conduit
 * @param thread_num
 */
func (tm *TransportModule) UDP_daemon(connection *net.UDPConn, conduit chan int, thread_num int) {
	if debug {
		fmt.Println("THREAD SPAWNED->", thread_num)
	}
	// count := 0

	for {
		buffer := make([]byte, 20100)
		n, remoteAddr, err := tm.connection.ReadFromUDP(buffer)
		for err != nil {
			fmt.Println("listener failed - ", err)
		}
		// if count%500 == 0 {
		// 	fmt.Println(count)
		// }
		// count++

		tm.router(buffer[:n], remoteAddr.String())
	}

}

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

/**
 * @Description: inialises the server based on port number
 * @param args
 */

func (tm *TransportModule) Init_server() {
	if MULTI_CORE_MODE {
		fmt.Println("[MULTICORE MODE] [", runtime.NumCPU(), "] SPANNERS AT PORT [", tm.connection.LocalAddr(), "] SYS MEM LIMIT [", MEMORY_LIMIT, "]")
	}

	//go proportionalCollector()
	// tm.daemonSpawner()
	go tm.UDP_daemon(tm.connection, nil, 0)

}

/**
 * @Description: for debug to view packet b4 sending
 * @param arr
 */
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
/**
 * @Description: retunrs current sys mem usage
 * @return uint64
 */
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

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

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

// no pre pend
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

		tm.StorageChan <- internal_message_obj

	}
}

func (tm *TransportModule) gossipPrepend(payload []byte, clientAddr string) {
	tm.GroupSend <- payload
}

// node req prepend
func (tm *TransportModule) nodeReq(node2nodePayload []byte, messageID []byte) {
	node2nodeMsg := &protobuf.InternalMsg{}
	proto.Unmarshal(node2nodePayload, node2nodeMsg)
	clientAddr := node2nodeMsg.GetClientAddr()
	cachedResponse, found := check_cache(messageID)
	if found {
		tm.Send(cachedResponse, messageID, clientAddr)
	} else {
		tm.StorageChan <- *node2nodeMsg
	}
}

func (tm *TransportModule) nodeRes(node2nodePayload []byte, messageID []byte) {
	// send the internalPayload to the client
	node2nodeMsg := &protobuf.InternalMsg{}
	proto.Unmarshal(node2nodePayload, node2nodeMsg)

	response := node2nodeMsg.GetPayload()

	cache_data(messageID, response)
	tm.Send(response, messageID, node2nodeMsg.GetClientAddr())
}

// grt local address

//https://stackoverflow.com/questions/23558425/how-do-i-get-the-local-ip-address-in-go
//retunr the local address of the machine
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

func (tm *TransportModule) ResSend(payload []byte, messageID string, destAddr string) {
	cache_data([]byte(messageID), payload)
	tm.Send(payload, []byte(messageID), destAddr)

}
