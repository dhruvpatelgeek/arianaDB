package storage

import (
	"crypto/sha256"
	"dht/google_protocol_buffer/pb/protobuf"
	"encoding/hex"
	"fmt"
	"log"
	"math/big"
	"os"
	"runtime"
	"sync"
	"syscall"
	//"encoding/hex"
	"dht/mock_store/transport"
	"github.com/golang/protobuf/proto"
)

//REFRENCES--------------------------------
//https://stackoverflow.com/questions/28400340/how-support-concurrent-connections-with-a-udp-server-using-go
//https://stackoverflow.com/questions/27625787/measuring-memory-usage-of-executable-run-using-golang
//https://stackoverflow.com/questions/31879817/golang-os-exec-realtime-memory-usage
//https://golangcode.com/print-the-current-memory-usage/

//CONTROL PANEL----------------------------
var debug = false
var MAP_SIZE_MB = 128 // memory HARD LIMIT

// Command numbers
const PUT      = 1
const GET      = 2
const REMOVE   = 3
const SHUTDOWN = 4
const WIPEOUT  = 5
const IS_ALIVE = 6
const GET_PID  = 7
const GET_MC   = 8

// Error codes
const OK        = 0
const NO_KEY    = 1
const NO_SPACE  = 2
const SYS_OVRLD = 3
const UNKWN_CMD = 5
const INV_KEY   = 6
const INV_VAL   = 7

//for the refactor
const LESS    = -1
const EQUAL   =  0
const GREATER =  1


//MAP_AND_CACHE----------------------------
var storage = make(map[string][]byte)
var mutex sync.Mutex

//-----------------------------------------
var getAllNodes = func () []string {
	return []string {
		//transport.GetLocalAddr(),
		"128.189.222.109:3200",
		"128.189.222.109:3201",
		"128.189.222.109:3202",
		"128.189.222.109:3203",
		"128.189.222.109:3204",
		"128.189.222.109:3205",
		"128.189.222.109:3206",
		"128.189.222.109:3207",
		"128.189.222.109:3208",
		"128.189.222.109:3209",
		"128.189.222.109:3210",
		"128.189.222.109:3211",
		"128.189.222.109:3212",
		"128.189.222.109:3213",
		"128.189.222.109:3214",
		"128.189.222.109:3215",
		"128.189.222.109:3216",
		"128.189.222.109:3217",
		"128.189.222.109:3218",
		"128.189.222.109:3219",
		"128.189.222.109:3220",
		"128.189.222.109:3221",
		"128.189.222.109:3222",
		"128.189.222.109:3223",
		//"127.0.0.1:3202",
		//"127.0.0.1:3203",
		//"127.0.0.1:3204",
		//"127.0.0.1:3205",
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

func keyRoute(key []byte) string {
	if len(key) == 0 {
		return transport.GetLocalAddr()
	}

	keyHashInt := hashInt(hex.EncodeToString(key))
	//log.Println(hex.EncodeToString(key), " Key hash", keyHashInt.String())

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


func StorageModule(ts *transport.TransportModule,reqFrom_ts chan protobuf.InternalMsg){
	for{
		req:=<-reqFrom_ts
		if(req.OriginAddr==""){
			unmarshalled_payload:=&protobuf.KVRequest{}
			proto.Unmarshal(req.Payload,unmarshalled_payload)
			destAddr := keyRoute(unmarshalled_payload.Key)
			//argsWithProg := os.Args
			if (destAddr == transport.GetLocalAddr()){ // is it should be handles
				response, _ := Message_handler(req.Payload)
				ts.ResSend(response,string(req.Message ), req.ClientAddr)
			} else {
				internalPayload_new :=req
				internalPayload_new.OriginAddr=transport.GetLocalAddr()
				marshalled_internalPayload, err := proto.Marshal(&internalPayload_new)
				if err != nil {
					log.Println("[ERTICAL CASTINF ERROR][234]")
				} else {
					ts.Send(marshalled_internalPayload,[]byte( "reques"+string(req.Message)), destAddr)
				}
			}
		} else {
			response, _ := Message_handler(req.Payload)
			ts.ResSend(response,string(req.Message),req.ClientAddr);
		}
	}
}


/**
 * @Description: peals the seconday message layer and performs server functions returns the genarated payload
 * @param message
 * @return []byte
 */
 func Message_handler(message []byte) ([]byte, bool) {
	cast_req := &protobuf.KVRequest{}
	error := proto.Unmarshal(message, cast_req)
	if error != nil {
		fmt.Printf("\nUNPACK ERROR %+v\n", error)
	}

	switch cast_req.GetCommand() {
	case PUT:
		return put(cast_req.GetKey(), cast_req.GetValue(), cast_req.GetVersion()), false
	case GET:
		return get(cast_req.Key), false
	case REMOVE:
		return remove(cast_req.Key), true
	case SHUTDOWN:
		shutdown()
	case WIPEOUT:
		return wipeout(), true
	case IS_ALIVE:
		return is_alive(), true
	case GET_PID:
		return getpid(), true
	case GET_MC:
		return getmemcount(), true
	default:
		return message, true
	}

	return is_alive(), true
}


//-----------------------------------------
//DATABASE FUNCTIONS-----------------------

/**
 * @Description:Puts some value (and corresponding version)
 *into the store. The value (and version) can be later retrieved using the key.
 * @param key
 * @param value
 * @param version
 * @return []byte
 */
func put(key []byte, value []byte, version int32) []byte {
	var errCode uint32 = OK
	if int(len(value)) > 10000 {
		errCode = INV_VAL

		payload := &protobuf.KVResponse{ErrCode: &errCode}
		out, err := proto.Marshal(payload)
		if err != nil {
			log.Fatalln("Failed to encode address book:", err)
		}

		return out
	}
	mutex.Lock() //<<<<<<<<<<<<<<<MAP LOCK

	if getCurrMem() < uint64(MAP_SIZE_MB) {

	//log.Println("PUT ", hex.EncodeToString(value), " at ", hex.EncodeToString(key))
		storage[string(key)] = value // adding the value
		if debug {
			fmt.Println("PUT", string(key), ",<->", string(value))
		}
	} else {
		if debug {
			fmt.Println("ERROR PUTTING")
		}
		errCode = NO_SPACE
	}
	mutex.Unlock() //<<<<<<<<<<<<<<<MAP UNLOCK

	payload := &protobuf.KVResponse{
		ErrCode: &errCode,
		Value:   value,
	}
	out, err := proto.Marshal(payload)
	if err != nil {
		log.Fatalln("Failed to encode address book:", err)
	}

	return out
}

/**
 * @Description:Returns the value and version that is associated with the key. If there is no such key in your store, the store should return error (not found).
 * @param key
 * @return []byte
 */
func get(key []byte) []byte {
	mutex.Lock()
	value, found := storage[string(key)]
	//log.Println("GET ", hex.EncodeToString(value), " from ", hex.EncodeToString(key))
	mutex.Unlock()
	var errCode uint32 = OK
	if !found {
		errCode = NO_KEY
	}

	payload := &protobuf.KVResponse{
		ErrCode: &errCode,
		Value:   value,
	}
	out, err := proto.Marshal(payload)
	if err != nil {
		log.Fatalln("Failed to encode address book:", err)
	}

	return out
}

/**
 * @Description:Removes the value that is associated with the key.
 * @param key
 * @return []byte
 */
func remove(key []byte) []byte {
	var errCode uint32 = OK

	mutex.Lock()
	value, found := storage[string(key)]

	if found {
		delete(storage, string(key))
		mutex.Unlock()

		payload := &protobuf.KVResponse{
			ErrCode: &errCode,
			Value:   value,
		}

		out, err := proto.Marshal(payload)
		if err != nil {
			log.Fatalln("Failed to encode address book:", err)
		}

		return out

	} else {
		errCode = NO_KEY

		payload := &protobuf.KVResponse{
			ErrCode: &errCode,
			Value:   value,
		}
		out, err := proto.Marshal(payload)
		if err != nil {
			log.Fatalln("Failed to encode address book:", err)
		}

		return out
	}
}

/**
 * @Description: calls os.shutdown
 */
func shutdown() {
	os.Exit(555)
}

/**
 * @Description: clears the database
 * @return []byte
 */
func wipeout() []byte {
	mutex.Lock() //<<<<<<<<<<<<<<<MAP LOCK
	/*
	for k := range storage {
		delete(storage, k)
	}*/
	storage = make(map[string][]byte)
	mutex.Unlock() //<<<<<<<<<<<<<<<MAP UNLOCK
	var errCode uint32 = OK

	payload := &protobuf.KVResponse{ErrCode: &errCode}
	out, err := proto.Marshal(payload)
	if err != nil {
		log.Fatalln("Failed to encode address book:", err)
	}

	log.Println("Wipeout ", payload)

	return out
}

/**
 * @Description: response indicating server is alive
 * @return []byte
 */
func is_alive() []byte {
	fmt.Println("CLIENT ASKED IF SERVER ALIVE")

	var errCode uint32 = OK

	payload := &protobuf.KVResponse{ErrCode: &errCode}
	out, err := proto.Marshal(payload)
	if err != nil {
		log.Fatalln("Failed to encode address book:", err)
	}

	return out
}

/**
 * @Description: gets the current procressID
 * @return []byte
 */
func getpid() []byte {
	var errCode uint32 = OK

	pid := int32(syscall.Getpid())
	payload := &protobuf.KVResponse{
		ErrCode: &errCode,
		Pid:     &pid,
	}

	out, err := proto.Marshal(payload)
	if err != nil {
		log.Fatalln("Failed to encode address book:", err)
	}

	return out
}

/**
 * @Description: returns number of members
 * @return []byte
 */
func getmemcount() []byte {
	var errCode uint32 = OK

	payload := &protobuf.KVResponse{ErrCode: &errCode}

	out, err := proto.Marshal(payload)
	if err != nil {
		log.Fatalln("Failed to encode address book:", err)
	}

	return out
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}


func getCurrMem() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return bToMb((m.Alloc))
}
