package storage

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"dht/google_protocol_buffer/pb/protobuf"
	"dht/src/membership"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"log"
	"math/big"
	"os"
	"runtime"
	"sync"
	"syscall"

	//"encoding/hex"
	"dht/src/transport"

	"github.com/golang/protobuf/proto"
)

//REFRENCES--------------------------------
//https://stackoverflow.com/questions/28400340/how-support-concurrent-connections-with-a-udp-server-using-go
//https://stackoverflow.com/questions/27625787/measuring-memory-usage-of-executable-run-using-golang
//https://stackoverflow.com/questions/31879817/golang-os-exec-realtime-memory-usage
//https://golangcode.com/print-the-current-memory-usage/

//CONTROL PANEL----------------------------
var debug = false
var MAP_SIZE_MB = 128000000000000000 // memory HARD LIMIT

// Command numbers
const PUT = 1
const GET = 2
const REMOVE = 3
const SHUTDOWN = 4
const WIPEOUT = 5
const IS_ALIVE = 6
const GET_PID = 7
const GET_MC = 8

// Error codes
const OK = 0
const NO_KEY = 1
const NO_SPACE = 2
const SYS_OVRLD = 3
const UNKWN_CMD = 5
const INV_KEY = 6
const INV_VAL = 7

const LESS = -1
const EQUAL = 0
const GREATER = 1

//MAP_AND_CACHE----------------------------

type StorageModule struct {
	kvStore map[string][]byte
	kvsLock *sync.Mutex
}

//-----------------------------------------
// var getAllNodes = gms.GetAllNodes()
// func () []string {
// 	return []string {
// 		//transport.GetLocalAddr(),
// 		// "127.0.0.1:3200",
// 		// "127.0.0.1:3201",
// 		// "127.0.0.1:3202",
// 		// "127.0.0.1:3203",
// 		// "127.0.0.1:3204",
// 		// "127.0.0.1:3205",
// 		"127.0.0.1:3206",
// 		"127.0.0.1:3207",
// 		"127.0.0.1:3208",
// 		"127.0.0.1:3209",
// 		"127.0.0.1:3210",
// 		"127.0.0.1:3211",
// 		// "127.0.0.1:3212",
// 		// "127.0.0.1:3213",
// 		// "127.0.0.1:3214",
// 		// "127.0.0.1:3215",
// 		// "127.0.0.1:3216",
// 		// "127.0.0.1:3217",
// 		// "127.0.0.1:3218",
// 		// "127.0.0.1:3219",
// 		// "127.0.0.1:3220",
// 		// "127.0.0.1:3221",
// 		// "127.0.0.1:3222",
// 		// "127.0.0.1:3223",
// 		//"127.0.0.1:3202",
// 		//"127.0.0.1:3203",
// 		//"127.0.0.1:3204",
// 		//"127.0.0.1:3205",
// 	}
// }

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

func keyRoute(key []byte, gms *membership.MembershipService) string {
	if len(key) == 0 {
		return transport.GetLocalAddr()
	}

	keyHashInt := hashInt(hex.EncodeToString(key))
	//log.Println(hex.EncodeToString(key), " Key hash", keyHashInt.String())

	nodeList := gms.GetAllNodes()
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

func New(
	tm *transport.TransportModule,
	reqFrom_tm chan protobuf.InternalMsg,
	gms *membership.MembershipService) *StorageModule {

	sm := StorageModule{
		kvStore: make(map[string][]byte),
		kvsLock: &sync.Mutex{},
	}

	go sm.runModule(tm, reqFrom_tm, gms)

	return &sm
}

func gzipString(src []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)

	_, err := zw.Write(src)
	if err != nil {
		return nil, err
	}

	if err := zw.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func unzipgzipString(src []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)

	gr, err := gzip.NewReader(bytes.NewBuffer(src))
	if err != nil {
		return nil, err
	}
	src, err = ioutil.ReadAll(gr)
	if err := zw.Close(); err != nil {
		return nil, err
	}

	return src, err
}

func (sm *StorageModule) runModule(tm *transport.TransportModule, reqFrom_tm chan protobuf.InternalMsg, gms *membership.MembershipService) {
	for {
		req := <-reqFrom_tm
		if req.OriginAddr == "" {
			unmarshalled_payload := &protobuf.KVRequest{}
			proto.Unmarshal(req.Payload, unmarshalled_payload)
			destAddr := keyRoute(unmarshalled_payload.Key, gms)

			//argsWithProg := os.Args
			if destAddr == transport.GetLocalAddr() { // is it should be handles
				response := sm.message_handler(req.Payload)
				tm.ResSend(response, string(req.Message), req.ClientAddr)
			} else {
				internalPayload_new := req
				internalPayload_new.OriginAddr = transport.GetLocalAddr()
				marshalled_internalPayload, err := proto.Marshal(&internalPayload_new)
				if err != nil {
					log.Println("[ERTICAL CASTINF ERROR][234]")
				} else {
					tm.Send(marshalled_internalPayload, []byte("reques"+string(req.Message)), destAddr)
				}
			}
		} else {
			response := sm.message_handler(req.Payload)
			tm.ResSend(response, string(req.Message), req.ClientAddr)
		}
	}
}

/**
 * @Description: peals the seconday message layer and performs server functions returns the genarated payload
 * @param message
 * @return []byte
 */
func (sm *StorageModule) message_handler(message []byte) []byte {
	cast_req := &protobuf.KVRequest{}
	error := proto.Unmarshal(message, cast_req)
	if error != nil {
		fmt.Printf("\nUNPACK ERROR %+v\n", error)
	}

	var errCode uint32
	var value []byte
	var pid int32

	switch cast_req.GetCommand() {
	case PUT:
		errCode = sm.put(cast_req.GetKey(), cast_req.GetValue(), cast_req.GetVersion())
	case GET:
		value, errCode = sm.get(cast_req.Key)
	case REMOVE:
		errCode = sm.remove(cast_req.Key)
	case SHUTDOWN:
		shutdown()
	case WIPEOUT:
		errCode = sm.wipeout()
	case IS_ALIVE:
		errCode = is_alive()
	case GET_PID:
		pid, errCode = getpid()
	case GET_MC:
		_ = getmemcount()
	default:
		errCode = UNKWN_CMD
	}

	kvres := &protobuf.KVResponse{
		ErrCode: &errCode,
		Value:   value,
		Pid:     &pid,
	}

	serialResponse, err := proto.Marshal(kvres)
	if err != nil {
		log.Println("Marshalling error ", err)
	}

	return serialResponse
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
func (sm *StorageModule) put(key []byte, value []byte, version int32) uint32 {
	if int(len(value)) > 10000 {
		return INV_VAL
	}

	if getCurrMem() < uint64(MAP_SIZE_MB) {
		// zipValue,err:=gzipString(value)
		// if(err!=nil){
		// 	fmt.Println(err)
		// }
		sm.kvsLock.Lock()
		sm.kvStore[string(key)] = value
		sm.kvsLock.Unlock()
	} else {
		return NO_SPACE
	}

	return OK
}

/**
 * @Description:Returns the value and version that is associated with the key. If there is no such key in your store, the store should return error (not found).
 * @param key
 * @return []byte
 */
func (sm *StorageModule) get(key []byte) ([]byte, uint32) {
	sm.kvsLock.Lock()
	value, found := sm.kvStore[string(key)]
	sm.kvsLock.Unlock()

	if !found {
		return nil, NO_KEY
	}
	// unziped,err:=unzipgzipString(value)
	// if err!= nil {
	// 	fmt.Println(err)
	// }
	// return unziped, OK
	return value, OK
}

/**
 * @Description:Removes the value that is associated with the key.
 * @param key
 * @return []byte
 */
func (sm *StorageModule) remove(key []byte) uint32 {

	sm.kvsLock.Lock()
	_, found := sm.kvStore[string(key)]
	sm.kvsLock.Unlock()

	if found {
		sm.kvsLock.Lock()
		delete(sm.kvStore, string(key))
		sm.kvsLock.Unlock()

		return OK

	}

	return NO_KEY

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
func (sm *StorageModule) wipeout() uint32 {
	sm.kvsLock.Lock() //<<<<<<<<<<<<<<<MAP LOCK
	sm.kvStore = make(map[string][]byte)
	sm.kvsLock.Unlock() //<<<<<<<<<<<<<<<MAP UNLOCK

	return OK
}

/**
 * @Description: response indicating server is alive
 * @return []byte
 */
func is_alive() uint32 {
	fmt.Println("CLIENT ASKED IF SERVER ALIVE")

	return OK
}

/**
 * @Description: gets the current procressID
 * @return []byte
 */
func getpid() (int32, uint32) {
	pid := int32(syscall.Getpid())

	return pid, OK
}

/**
 * @Description: returns number of members
 * @return []byte
 */
func getmemcount() int32 {
	return 1 //TODO RETURNING 1 for now
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func getCurrMem() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return bToMb((m.Alloc))
}
