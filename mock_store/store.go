package main

import (
	"bufio"
	"fmt"
	"hash/crc32"
	"dht/google_protocol_buffer/pb/protobuf"
	"log"
	"net"
	"os"
	"runtime"
	"strconv"
	"time"
	"math/big"
	"crypto/sha256"
	"dht/mock_store/storage"

	"github.com/golang/protobuf/proto"
	"github.com/pmylund/go-cache"
)

//REFRENCES--------------------------------
//https://stackoverflow.com/questions/28400340/how-support-concurrent-connections-with-a-udp-server-using-go
//https://stackoverflow.com/questions/27625787/measuring-memory-usage-of-executable-run-using-golang
//https://stackoverflow.com/questions/31879817/golang-os-exec-realtime-memory-usage
//https://golangcode.com/print-the-current-memory-usage/

//-----------------------------------------
//CONTROL PANEL----------------------------
var debug = true
var MEMORY_LIMIT = 51200
var MULTI_CORE_MODE = true
var MAP_SIZE_MB = 70
var CACHE = 10
var CACHE_LIFESPAN = 5 // how long should the cache persist


const LESS    = -1
const EQUAL   =  0
const GREATER =  1

//-----------------------------------------

//CONNECTION-------------------------------

type Message struct {
	ClientAddr string
	Payload    []byte
	MessageID  string
}

var connection *net.UDPConn

var GroupSend = make(chan Message)
//------------------------------------------

var getAllNodes = func () []string {
	return []string {
		"127.0.0.1:3200",
		"127.0.0.1:3201", 
		/*"124124124", 
		"124124124234", 
		"21638492872", 
		"5bwtry w45yb", 
		"erbtq3vtq 3", 
		"visdvcgwfw7", 
		"fsiufwgfiuba", 
		"34 t13q3vt3qc",
		"31tt3q3vt3qc",
		"31tt3q3vt3qc",
		"31tt3q3vt3qc",
		"31tt3q3vt3qc",
		"31tt3q3vt3qc",
		"31tt3q3vt3qc",
		"31tt3q3vt3qc",
		"31tt3q3vt3qc",
		"31tt3q3vt3qc",
		"31tt3q3vt3qc",
		"31tt3q3vt3qc",
		"31tt3q3vt3qc",
		"31tt3q3vt3qc",
		"31tt3q3vt3qc",
		"31tt3q3vt3qc",*/
	}
}

//optimizations----------------------------

func proportionalCollector() {
	var sleepCtr time.Duration = 1000*time.Millisecond
	for {
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
	if currentMem < 100 {
		return 5*time.Second
	} else if currentMem < 105 {
		return 100*time.Millisecond
	} else if currentMem < 110 {
		return 50*time.Millisecond
	} else if currentMem < 115 {
		return 10*time.Millisecond
	} else if currentMem < 120 {
		//fmt.Println("CRITICAL MEMORY")
		return time.Millisecond
	} else {
		//fmt.Println("[SUPER] CRITICAL MEMORY")
		return 100*time.Microsecond
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
 func check_cache(message_id []byte)([]byte,bool){
	if debug {
		fmt.Println("checking cache...")
	}
	response, found := message_cache.Get(string(message_id))

	if ! found {
		if debug {
			fmt.Println("not found...")
		}
		return nil, false;
	} else {
		if debug {
			fmt.Println("found...")
		}
		str := fmt.Sprintf("%v", response)
		var cached_data=[]byte(str);
		return cached_data,true;
	}
}

/**
 * @Description: put data in the cache
 * @param message_id
 * @param data
 * @return bool
 */
func cache_data(message_id []byte,data []byte)(bool){
	if get_mem_usage() > uint64(MEMORY_LIMIT-20) {
		return true;
		fmt.Println("\n[MEMORY WARNING]\n")
		message_cache.Flush();
	}
	if get_mem_usage() > uint64(MEMORY_LIMIT/2) {
		fmt.Println("\n[MEMORY WARNING 50%]\n")
		message_cache.DeleteExpired();
	}

	message_cache.Set(string(message_id), string(data), cache.DefaultExpiration);
	return true;
}

//-----------------------------------------

//HELPER FUNCTIONS-------------------------
/**
 * @Description: checks if port is valid
 * @param port
 * @return bool
 */
func check_if_port_is_valid(port string) bool {
	casted_port, err := strconv.Atoi(port)
	if err != nil {
		fmt.Printf("CASTING ERROR [port valid]")
	}
	if (casted_port > 65535) || (casted_port < 0) {
		return false
	} else {
		return true
	}
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

/**
 * @Description: converts the input key into a hash
 * @param s
 * @return uint32
 */


//-----------------------------------------
// MESSAGE PARSING FUNC--------------------
/**
 * @Description: peels back the message layer and searches the cache for a reacent message else calls message_handles
 * @param whole_message
 * @return []byte
 * @return bool
 */
func message_broker(whole_message []byte) ([]byte, bool) {
	cast_whole_req := &protobuf.Msg{
		MessageID: nil,
		Payload:   nil,
		CheckSum:  0,
	}

	error := proto.Unmarshal(whole_message, cast_whole_req)
	if error != nil {
		fmt.Printf("\nUNPACK ERROR[W2] %+v\n", error)
	}

	if cast_whole_req.CheckSum != calculate_checksum(cast_whole_req.MessageID, cast_whole_req.Payload) {
		fmt.Printf("\n[CHECKSUM WRONG NO RESPONSE SENT] %+v\n", error)
		return nil, false
	}
	data_cached, found := check_cache(cast_whole_req.MessageID)
	if found {
		res_message := &protobuf.Msg{
			MessageID: cast_whole_req.MessageID,
			Payload:   data_cached,
			CheckSum:  calculate_checksum(cast_whole_req.MessageID, data_cached),
		}
		payload, err := proto.Marshal(res_message)
		if err != nil {
			log.Fatalln("Failed to encode address book:", err)
		} else {
			if debug {
				fmt.Printf("payload generated\n")
			}
		}
		return payload, true

	} else {
		uncached_data,shouldCache := storage.Message_handler(cast_whole_req.Payload)
		if(shouldCache){
			cache_data(cast_whole_req.MessageID, uncached_data)
		}
		res_message := &protobuf.Msg{
			MessageID: cast_whole_req.MessageID,
			Payload:   uncached_data,
			CheckSum:  calculate_checksum(cast_whole_req.MessageID, uncached_data),
		}
		payload, err := proto.Marshal(res_message)
		if err != nil {
			log.Fatalln("Failed to encode address book:", err)
		}
		return payload, true
	}

}



//-----------------------------------------

//UDP SERVER FUNC--------------------------
/**
 * @Description:  go routine to serve a client
 * @param connection
 * @param conduit
 * @param thread_num
 */
func UDP_daemon(connection *net.UDPConn, conduit chan int, thread_num int) {
	if debug {
		fmt.Println("THREAD SPAWNED->", thread_num)
	}
	buffer := make([]byte, 65535)
	n, remoteAddr, err := 0, new(net.UDPAddr), error(nil)
	for err != nil {
		fmt.Println("listener failed - ", err)
		conduit <- 2
	}
	n, remoteAddr, err = connection.ReadFromUDP(buffer)
	conduit <- 1
	go router(buffer[:n], remoteAddr.String())
	/*
	response_val, valid := message_broker(buffer[:n])
	if valid {
		n, err = connection.WriteToUDP(response_val, remoteAddr)
		if err != nil {
			fmt.Println("[FAIL] err serving ->", remoteAddr, "error is ", err)
			fmt.Println("\n[IMP ERROR] message size was ", len(response_val))
		}
		if debug {

			fmt.Println("THREAD EXIT->", thread_num, "\n")
		}
	} else {
		fmt.Println("WRONG CHECKSUM")
	}
*/
}

/**
 * @Description: genrates several go routine depending on the memory
 * @param _port
 * @return func()
 */
func spawn_UDP_daemon(_port string) func() {
	port_num, err := strconv.Atoi(_port)
	if err != nil {
		fmt.Printf("PORT CASTING ERROR [EXIT]")
	}
	address := net.UDPAddr{
		Port: port_num,
		IP:   net.IP{0, 0, 0, 0}, // local ip address
	}
	fmt.Println("ADDRESS IS", address.IP, ":", address.Port)
	return func() {
		var thread_num int = 0
		connection, err = net.ListenUDP("udp", &address)
		err = connection.SetWriteBuffer(20000)
		if err != nil {
			fmt.Printf("[WRITE SETTING ERROR]%+v", err)
		}
		err = connection.SetReadBuffer(20000)
		if err != nil {
			fmt.Printf("[READ SETTING ERROR]%+v", err)
		}
		if err != nil {
			panic(err)
		}
		conduit := make(chan int)
		if MULTI_CORE_MODE {
			for i := 0; i < 1; i++ {
				go UDP_daemon(connection, conduit, thread_num)
				thread_num++
			}
		} else {
			go UDP_daemon(connection, conduit, thread_num)
			thread_num++
		}
		if !debug {
			fmt.Print("\033[s")
		}

		for {
			if get_mem_usage() < uint64(MEMORY_LIMIT) {
				switch <-conduit {
				case 1:
					go UDP_daemon(connection, conduit, thread_num)
					thread_num++
				case 2:
					if debug {
						fmt.Println("THREAD NUMBER ", thread_num, "EXITED ON ERROR")
					}
					thread_num++
				}

			} else {
				fmt.Printf("\n[HALT] MEMORY FULL calling garbage collector-> %+v\n", get_mem_usage())
				time.Sleep(1 * time.Second)
				message_cache.Flush()
				message_cache.DeleteExpired()
				runtime.GC()
			}
		}

	}
}

/**
 * @Description: inialises the server based on port number
 * @param args
 */
func init_server(args string) {
	spawner := spawn_UDP_daemon(args)
	if MULTI_CORE_MODE {
		fmt.Println("[MULTICORE MODE] [", runtime.NumCPU(), "] SPANNERS AT PORT [", args, "] SYS MEM LIMIT [", MEMORY_LIMIT, "]")
	}
	go proportionalCollector()
	spawner()

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

	cast_req := &protobuf.KVResponse{
		ErrCode: nil,
		Value:   nil,
		Pid:     nil,
	}
	error = proto.Unmarshal(cast_whole_req.Payload, cast_req)
	if error != nil {
		fmt.Printf("\nUNPACK ERROR %+v\n", error)
	} else {
		//fmt.Println("\n XXXXX values areXXXXX \n");
		//fmt.Println(cast_req.ErrCode);
		//fmt.Println(cast_req.Value);
		//fmt.Println(cast_req.Pid);
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



//-----------------------------------------
//MAIN FUNCTION
//-----------------------------------------
func main() {
	fmt.Println("MAIN 122")
	argsWithProg := os.Args
	if len(argsWithProg) != 2 {
		fmt.Printf("INVALID NUMBER OF ARGUMENTS, EXITTING....\n")
	} else {
		if !check_if_port_is_valid(argsWithProg[1]) {
			fmt.Printf("PORT NOT VALID,EXITTING...\n")
		} else {
			fmt.Printf("------------------\n")
			init_server(argsWithProg[1]) // initaliaze;
		}
	}

}



//-----------------------------------------
//MAIN FUNCTION
//-----------------------------------------

func hashDifference(key *big.Int, node *big.Int) *big.Int {
	diff := big.NewInt(0) 
	max := big.NewInt(0)
	maxSlice := make([]byte, 256)
	// Initialize with largest byte value
	for el := range maxSlice {
		maxSlice[el] = 15
	}

	max.SetBytes(maxSlice)

	keyCmp := key.Cmp(node)
	
	if keyCmp == LESS {
		return diff.Sub(node, key)
	} else if keyCmp == EQUAL {
		return diff.SetInt64(0)
	} else {
		return diff.Add(diff.Sub(max, key), node)
	}
}

func hashInt(key string) *big.Int {
	keyHash := hash(key)
	keyHashInt := big.NewInt(0)
	return keyHashInt.SetBytes(keyHash)
}

func hash(str string) []byte {
	digest := sha256.Sum256([]byte(str))
	return digest[:]
}

func keyRoute(key []byte)string{
	keyHashInt := hashInt(string(key))

	nodeList := getAllNodes()
	//logger.Println(nodeList)

	responsibleNode := nodeList[0]
	diff := hashDifference(keyHashInt, hashInt(responsibleNode))

	// Find node responsible for given key
	for _, currNode := range nodeList {
		currDiff := hashDifference(keyHashInt, hashInt(currNode))
		if currDiff.Cmp(diff) == LESS {
			diff = currDiff
			responsibleNode = currNode
		} 
	}

	return responsibleNode
}

func send(payload []byte,messageID []byte,destAddr string){
	message,err:=generateShell(payload,messageID)
	if(err!=nil){
		fmt.Println("payload gen failed");
	}

	msg := &protobuf.Msg{}
	error:=proto.Unmarshal(message,msg);
	if error != nil {
		log.Println("Unable to deserialize ", error)
	}
	/*
	conn, err := net.Dial("udp", address)
	if err != nil {
		fmt.Printf("Some error %v", err)
		return
	}*/

	addr, err := net.ResolveUDPAddr("udp", destAddr)
	if err != nil {
		log.Println("Address error ", err)
	}

	connection.WriteToUDP(message, addr)
}

func uDPSendAsClient(payload []byte,address string,itr int,timeout int64,message_id string){
	if(itr>3) {
		return;
	}
	fmt.Printf("RETRYING REQUEST [%d]--------------------------\n",itr);
	conn, err := net.Dial("udp", address)
	if err != nil {
		fmt.Printf("Some error %v", err)
		return
	}
	//writing message->
	fmt.Fprintf(conn,string(payload));
	fmt.Printf("packet-written");

	//reading RESPONSE-<
	response_payload :=  make([]byte, 10000)
	var byte_ctr int;
	timeoutDuration := time.Duration(timeout) * time.Millisecond;
	err = conn.SetReadDeadline(time.Now().Add(timeoutDuration))
	byte_ctr, err = bufio.NewReader(conn).Read(response_payload)
	if e,ok := err.(net.Error); ok && e.Timeout() {
		fmt.Printf("\nTIMEOUT after %v\n",timeoutDuration)
		conn.Close();
		if(itr<3){
			uDPSendAsClient(payload,address,itr+1,timeout+100,message_id);
		}
		return
	}else if err != nil {
		fmt.Println("\n[ERROR] reading\n",err);
		conn.Close();
		return
	} else {
		fmt.Printf("packet-received:\n bytes=%d \nfrom=%s\n",byte_ctr,address);
		fmt.Printf("\n>>>>>>>REPLY FORM SERVER[SUCCESS]\n")
		cast_response:=&protobuf.Msg{
			MessageID: nil,
			Payload:   nil,
			CheckSum:  0,
		}
		error:=proto.Unmarshal(response_payload[:byte_ctr],cast_response);
		if(error!=nil) {
			fmt.Printf("\n[e2e3]UNPACK ERROR %+v\n",error)
		}
		local_checksum:=calculate_checksum(cast_response.GetMessageID(),cast_response.GetPayload());
		var flag=false;
		if(local_checksum!=cast_response.GetCheckSum()){
			fmt.Printf("\n[CHECKSUM WRONG]\n")
			flag=true;
		}
		if(flag){
			if(itr<3) {
				fmt.Printf("\n[RETRYING]\n")
				uDPSendAsClient(payload,address,itr+1,timeout+100,message_id);
			}
		} else{
			server_response:=cast_response.GetPayload();
			res_struct:=&protobuf.KVResponse{
				ErrCode: nil,
				Value:   nil,
				Pid:     nil,
			}
			error=proto.Unmarshal(server_response,res_struct);
			if(res_struct.GetErrCode()==0){
				fmt.Printf("\nVALLUE WRITTEN SUCESSFULLY\n----------");
				fmt.Printf("value written is \"%+v\"",string(res_struct.GetValue()));
			} else {
				fmt.Println("\n[SERVER ERROR]satellite internal server error ",res_struct.GetErrCode());
				if(itr<3) {
					fmt.Printf("\n[RETRYING]\n")
					uDPSendAsClient(payload,address,itr+1,timeout+100,message_id);
				}
			}
		}
		conn.Close();
		return
	}
	return
}
func generateShell(payload []byte,messageId []byte) ([]byte,error){
	checksum:=calculate_checksum([]byte(messageId),payload)
	shell:=&protobuf.Msg{
		MessageID: messageId,
		Payload:   payload,
		CheckSum:  checksum,
	}
	marshelledShell,err:=proto.Marshal(shell)

	if (err!=nil){
		fmt.Println("[crtitcal] protobuf marshall error")
	}
	return marshelledShell,err;
}

func router(payload []byte,clientAddr string){

	unmarshelled_payload:=&protobuf.Msg{
		MessageID: nil,
		Payload:   nil,
		CheckSum:  0,
	}
	proto.Unmarshal(payload,unmarshelled_payload)

	if(len(unmarshelled_payload.MessageID)>6) {
		switch string(unmarshelled_payload.MessageID[0:6]) {
		case "gossip": //gossip pre pend
			gossipPrepend(unmarshelled_payload.Payload,clientAddr)
			log.Println("gossip")
		case "reques": // node req prepend
			nodePrePend(unmarshelled_payload.Payload,unmarshelled_payload.MessageID[6:])
			log.Println("reques")
		case "respos": //resposnse prepend
			resPrepend(unmarshelled_payload.Payload,unmarshelled_payload.MessageID[6:])
			log.Println("respos")
		default: // no pre pend
			//log.Println("default-> no prepend")
			noPrePend(unmarshelled_payload.Payload,unmarshelled_payload.MessageID,clientAddr)
		}
	} else {
		log.Println("default-> no prepend")
		noPrePend(unmarshelled_payload.Payload,unmarshelled_payload.MessageID,clientAddr)
	}
}

const LOCAL_PORT = "3201"

// no pre pend
func noPrePend(payload []byte,messageID []byte,clientAddr string) {
	//	func keyRoute(key []byte)string
	//	func send(paylod []byte,messageID string,dest_addr string)
	where_to_send:=keyRoute(payload)
	//argsWithProg := os.Args
	if(where_to_send=="127.0.0.1:" + LOCAL_PORT){
		response, found := message_cache.Get(string(messageID))
		if found{
			str := fmt.Sprintf("%v", response)
			var cached_data=[]byte(str);
			send(cached_data,messageID,clientAddr)

		} else {
			response_to_send,shouldCache:=storage.Message_handler(payload);
			if shouldCache{
				message_cache.Add(string(messageID),response_to_send,5*time.Second)
			}
			send(response_to_send,messageID,clientAddr)
		}
	} else {
		internalPayload:=&protobuf.InternalMsg{
			ClientAddr: clientAddr,
			OriginAddr: "127.0.0.1:" + LOCAL_PORT,
			Payload:    payload,
		}
		marshalled_internalPayload,err:=proto.Marshal(internalPayload)
		if(err!=nil){
			log.Println("[ERTICAL CASTINF ERROR][234]")
		} else {
			send(marshalled_internalPayload, append([]byte("reques"), messageID...), where_to_send)
		}

	}
}

func gossipPrepend(payload []byte,clientAddr string){
	unmarshelled_payload:=&protobuf.Msg{}
	proto.Unmarshal(payload,unmarshelled_payload)

	struct_to_push:=Message{
		ClientAddr: clientAddr,
		Payload:    unmarshelled_payload.Payload,
		MessageID:  string(unmarshelled_payload.MessageID),
	}

	GroupSend <- struct_to_push;
}

// node req prepend
func nodePrePend(node_to_node_payload []byte,messageID []byte){
	unmarshelled_node_to_node_payload := &protobuf.InternalMsg{}

	proto.Unmarshal(node_to_node_payload, unmarshelled_node_to_node_payload)
	payload_gen, _ := storage.Message_handler(unmarshelled_node_to_node_payload.Payload)

	internalPayload:=&protobuf.InternalMsg{
		ClientAddr: unmarshelled_node_to_node_payload.GetClientAddr(),
		Payload:    payload_gen,
	}
	marshalled_internalPayload, err:=proto.Marshal(internalPayload)
	if(err!=nil){
		log.Println("[ERTICAL CASTINF ERROR][234]")
	}

	send(marshalled_internalPayload, append([]byte("respos"), messageID...), unmarshelled_node_to_node_payload.OriginAddr)
	// call the storage.Message_handler
	// then send to the origin node

}

func resPrepend(internalPayload []byte,messageID []byte){
	// send the internalPayload to the client
	unmarshelled_node_to_node_payload:=&protobuf.InternalMsg{}
	proto.Unmarshal(internalPayload,unmarshelled_node_to_node_payload)

	send(unmarshelled_node_to_node_payload.Payload, messageID, unmarshelled_node_to_node_payload.ClientAddr)
}


//-----------------------------------------