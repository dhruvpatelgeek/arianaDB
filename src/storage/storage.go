package storage

import (
	"dht/google_protocol_buffer/pb/protobuf"
	"errors"
	"fmt"
	"log"
	"math/big"
	"os"
	"runtime"
	"sync"
	"syscall"
	"time"

	"dht/src/constants"
	"dht/src/structure"
	"dht/src/transport"

	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
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

	// TODO: consider making this a channel of pointers to prevent copying protobufs with inherent mutex
	// when popping from channel and passing as arguments to functions
	coordinatorToStorageChannel chan protobuf.InternalMsg
	transportToStorageChannel   chan protobuf.InternalMsg
}

// @Description: Initializes a new storage module
// @param tm - transport module that sends requests to the storage module
// @param reqFrom_tm - channel that transport module send requests on
// @param gms - group membership service that gives the nodes in the network
// @return *StorageModule
func New(tm *transport.TransportModule,
	coordinatorToStorageChannel chan protobuf.InternalMsg,
	transportToStorageChannel chan protobuf.InternalMsg) *StorageModule {

	sm := StorageModule{
		kvStore:                     make(map[string][]byte),
		kvsLock:                     &sync.Mutex{},
		tm:                          tm,
		coordinatorToStorageChannel: coordinatorToStorageChannel,
		transportToStorageChannel:   transportToStorageChannel,
	}
	go sm.processCoordinatorMessages()
	go sm.processStorageToStorageMessages()
	// go sm.monitorKVStoreSize()

	return &sm
}

// @Description: Starts the storage module loop
// @param tm - transport module that sends requests to the storage module
// @param reqFrom_tm - channel that transport module send requests on
// @param gms - group membership service that gives the nodes in the network
func (sm *StorageModule) processCoordinatorMessages() {
	for {
		request := <-sm.coordinatorToStorageChannel
		command := request.GetCommand()
		switch constants.InternalMessageCommands(command) {
		case constants.ProcessKVRequest:
			err := sm.processKVRequest(&request)
			if err != nil {
				fmt.Errorf("", err)
			}
		case constants.ProcessKeyMigrationRequest:
			err := sm.processKeyMigrationRequest(&request)
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
func (sm *StorageModule) processKVRequest(request *protobuf.InternalMsg) error {
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

func (sm *StorageModule) processKeyMigrationRequest(request *protobuf.InternalMsg) error {
	// TODO: process key migration request
	// extract lowerbound, upperbound, destination address & convert to appropriate
	destination := request.GetMigrationDestinationAddress()

	//lowerbound := request.GetMigrationRangeLowerbound()
	//upperbound := request.GetMigrationRangeUpperbound()
	lowerbound := new(big.Int)
	_, success := lowerbound.SetString(request.GetMigrationRangeLowerbound(), 10)
	if !success {
		return errors.New("Storage: Failed to convert the lowerbound from string to big.int while processing migrating request. Ignoring.")
	}

	upperbound := new(big.Int)
	_, success = upperbound.SetString(request.GetMigrationRangeUpperbound(), 10)
	if !success {
		return errors.New("Storage: Failed to convert the upperbound from string to big.int while processing migrating request. Ignoring.")
	}

	// the new node's range is wrapped around if upperbound < lowerbound
	// note x.Cmp(y) returns -1 if x < y
	isWrapAround := upperbound.Cmp(lowerbound) == -1

	sm.kvsLock.Lock()
	for key, value := range sm.kvStore {
		hashedKey := structure.HashKey(key) // TODO: replace this function with the shared functions
		switch isWrapAround {
		case false:
			// when not wrapped around, the key is in new node's range if the hashedKey is inside [lowerbound:upperbound]
			hashedKeyIsBounded := lowerbound.Cmp(hashedKey) == -1 && hashedKey.Cmp(upperbound) == -1
			if hashedKeyIsBounded {
				// migrate keys
				err := sm.migrateKey(key, value, destination)
				if err != nil {
					fmt.Println("[Storage] Failed to migrate key.", err.Error())
				}
			}
		case true:
			// when wrapped around, the key is in new node's range if outside the range [upperbound:lowerbound] where upperbound < lowerbound
			hashedKeyIsBounded := !(upperbound.Cmp(hashedKey) == -1 && hashedKey.Cmp(lowerbound) == -1)
			if hashedKeyIsBounded {
				// migrate keys
				err := sm.migrateKey(key, value, destination)
				if err != nil {
					fmt.Println("[Storage] Failed to migrate key.", err.Error())
				}
			}
		}
	}
	sm.kvsLock.Unlock()

	// check if wrap around
	return nil
}

// ASSUMES callee already holds the lock to sm.kvStore
func (sm *StorageModule) migrateKey(key string, value []byte, destination string) error {
	kvRequest := &protobuf.KVRequest{
		Command: PUT,
		Key:     []byte(key),
		Value:   value,
	}
	serializedKVRequest, err := proto.Marshal(kvRequest)
	if err != nil {
		return errors.New("[Storage] failed to marshal KVRequest during migration. Ignoring this key")
	}

	internalMessage := &protobuf.InternalMsg{
		MessageID: []byte(uuid.New().String()),
		Command:   uint32(constants.InsertMigratedKey),
		KVRequest: serializedKVRequest,
	}

	err = sm.tm.SendStorageToStorage(internalMessage, destination)
	if err != nil {
		return err
	}

	delete(sm.kvStore, key)
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

func (sm *StorageModule) processStorageToStorageMessages() {
	for {
		message := <-sm.transportToStorageChannel
		messageID := string(message.GetMessageID())

		// TODO: message.GetCommand() returns the incorrect message
		switch internalMsgCommand := constants.InternalMessageCommands(message.GetCommand()); internalMsgCommand {
		case constants.InsertMigratedKey:
			kvRequest := &protobuf.KVRequest{}
			err := proto.Unmarshal(message.GetKVRequest(), kvRequest)
			if err != nil {
				fmt.Printf("[Storage] Failed to unmarshal KVRrequest (id: %s) attached to InternalMsg from storage-to-storage channel.\n", messageID)
				fmt.Printf("[Storage] Ignoring InternalMsg (id: %s) from storage-to-storage channel.\n", messageID)
				continue
			}

			switch kvCommand := kvRequest.GetCommand(); kvCommand {
			case PUT:
				_ = sm.put(kvRequest.GetKey(), kvRequest.GetValue(), kvRequest.GetVersion())
			default:
				fmt.Printf("[Storage] Storage-to-Storage channel received an unsupported KVRequest command (%d). Only supports PUT (%d) commands.\n", kvCommand, PUT)
			}
		default:
			fmt.Printf("[Storage] S2S received an unsupported InternalMsg command (%d). Only supports (%d) command.\n", internalMsgCommand, constants.InsertMigratedKey)
		}
	}
}

func (sm *StorageModule) monitorKVStoreSize() {
	for {
		time.Sleep(5 * time.Second)
		fmt.Printf("[Storage] Number of keys in KVStore: %d\n", len(sm.kvStore))
	}
}
