package storage

import (
	"dht/google_protocol_buffer/pb/protobuf"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"syscall"

	"dht/src/constants"
	"dht/src/transport"

	"github.com/golang/protobuf/proto"
)

/** TODO: refactor list
- remove the routing and process the incoming requests directly & send response
*/

//REFRENCES--------------------------------
//https://stackoverflow.com/questions/28400340/how-support-concurrent-connections-with-a-udp-server-using-go
//https://stackoverflow.com/questions/27625787/measuring-memory-usage-of-executable-run-using-golang
//https://stackoverflow.com/questions/31879817/golang-os-exec-realtime-memory-usage
//https://golangcode.com/print-the-current-memory-usage/

//CONTROL PANEL----------------------------
var debug = false

const STORE_SIZE_MB = 100 // memory HARD LIMIT
const MEM_LIM_MB = 120    // memory HARD LIMIT

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

type StorageModule struct {
	kvStore map[string][]byte
	kvsLock *sync.Mutex
	tm      *transport.TransportModule
}

// @Description: Initializes a new storage module
// @param tm - transport module that sends requests to the storage module
// @param reqFrom_tm - channel that transport module send requests on
// @param gms - group membership service that gives the nodes in the network
// @return *StorageModule
func New(tm *transport.TransportModule, coordinatorMessage chan protobuf.InternalMsg) *StorageModule {

	sm := StorageModule{
		kvStore: make(map[string][]byte),
		kvsLock: &sync.Mutex{},
		tm:      tm,
	}
	go sm.runModule(coordinatorMessage)

	return &sm
}

// @Description: Starts the storage module loop
// @param tm - transport module that sends requests to the storage module
// @param reqFrom_tm - channel that transport module send requests on
// @param gms - group membership service that gives the nodes in the network
func (sm *StorageModule) runModule(coordinatorMessage chan protobuf.InternalMsg) {
	for {
		request := <-coordinatorMessage
		command := request.GetCommand()
		switch constants.InternalMessageCommands(command) {
		case constants.ProcessKVRequest:
			err := sm.processKVRequest(request)
			if err != nil {
				fmt.Errorf("", err)
			}
		case constants.ProcessKeyMigrationRequest:
			fmt.Println("Received ProcessKeyMigrationRequest command in storage")
			err := sm.processKeyMigrationRequest(request)
			if err != nil {
				fmt.Errorf("", err)
			}
		case constants.ProcessTableMigrationRequest:
			fmt.Println("Received ProcessTableMigrationRequest command in storage")
		default:
			fmt.Printf("Storage: Received unrecognized command (%d) from Coordinator", command)
		}
	}
}

// @Description: peals the seconday message layer and performs server functions returns the genarated payload
// @param message
// @return []byte
func (sm *StorageModule) processKVRequest(request protobuf.InternalMsg) error {
	cast_req := &protobuf.KVRequest{}
	err := proto.Unmarshal(request.KVRequest, cast_req)
	if err != nil {
		return err
	}

	var errCode uint32
	var value []byte
	var pid int32

	if getCurrMem() < MEM_LIM_MB {
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
	} else {
		errCode = SYS_OVRLD
	}

	kvres := &protobuf.KVResponse{
		ErrCode: &errCode,
		Value:   value,
		Pid:     &pid,
	}

	kvResponse, err := proto.Marshal(kvres)
	if err != nil {
		log.Println("Marshalling error ", err)
	}

	messageID := request.GetMessageID()
	clientAddress := request.GetClientAddress()
	sm.tm.ResSend(kvResponse, string(messageID), clientAddress)
	return nil
}

// @Description:Puts some value (and corresponding version)
// into the store. The value (and version) can be later retrieved using the key.
// @param key
// @param value
// @param version
// @return []byte
func (sm *StorageModule) put(key []byte, value []byte, version int32) uint32 {
	if int(len(value)) > 10000 {
		return INV_VAL
	}

	if getCurrMem() < uint64(STORE_SIZE_MB) {
		sm.kvsLock.Lock()
		sm.kvStore[string(key)] = value
		sm.kvsLock.Unlock()
	} else {
		return NO_SPACE
	}

	return OK
}

// @Description:Returns the value and version that is associated with the key. If there is no such key in your store, the store should return error (not found).
// @param key
// @return []byte
func (sm *StorageModule) get(key []byte) ([]byte, uint32) {
	sm.kvsLock.Lock()
	value, found := sm.kvStore[string(key)]
	sm.kvsLock.Unlock()

	if !found {
		return nil, NO_KEY
	}

	return value, OK
}

// @Description:Removes the value that is associated with the key.
// @param key
// @return []byte
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

// @Description: calls os.shutdown
func shutdown() {
	os.Exit(555)
}

// @Description: clears the database
// @return []byte
func (sm *StorageModule) wipeout() uint32 {
	sm.kvsLock.Lock()
	sm.kvStore = make(map[string][]byte)
	sm.kvsLock.Unlock()

	return OK
}

func (sm *StorageModule) processKeyMigrationRequest(request protobuf.InternalMsg) error {
	// TODO: process key migration request
	// extract lowerbound, upperbound, destination address & convert to appropriate
	//destination := request.GetMigrationDestinationAddress()
	//lowerbound := request.GetMigrationRangeLowerbound()
	//upperbound := request.GetMigrationRangeUpperbound()

	// check if wrap around
	return nil
}

// @Description: response indicating server is alive
// @return []byte
func is_alive() uint32 {
	fmt.Println("CLIENT ASKED IF SERVER ALIVE")

	return OK
}

// @Description: gets the current procressID
// @return []byte
func getpid() (int32, uint32) {
	pid := int32(syscall.Getpid())

	return pid, OK
}

// @Description: returns number of members
// @return []byte
func getmemcount() int32 {
	return 1
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func getCurrMem() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return bToMb((m.Alloc + m.StackInuse + m.MSpanInuse + m.MCacheInuse))
}
