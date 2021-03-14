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
)

//REFRENCES--------------------------------
//https://stackoverflow.com/questions/28400340/how-support-concurrent-connections-with-a-udp-server-using-go
//https://stackoverflow.com/questions/27625787/measuring-memory-usage-of-executable-run-using-golang
//https://stackoverflow.com/questions/31879817/golang-os-exec-realtime-memory-usage
//https://golangcode.com/print-the-current-memory-usage/

//CONTROL PANEL----------------------------
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
	// replicated kv stores
	headKVStore     map[string][]byte
	headKVStoreLock *sync.Mutex

	middleKVStore     map[string][]byte
	middleKVStoreLock *sync.Mutex

	tailKVStore     map[string][]byte
	tailKVStoreLock *sync.Mutex

	tm *transport.TransportModule

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
		headKVStore:     make(map[string][]byte),
		headKVStoreLock: &sync.Mutex{},

		middleKVStore:     make(map[string][]byte),
		middleKVStoreLock: &sync.Mutex{},

		tailKVStore:     make(map[string][]byte),
		tailKVStoreLock: &sync.Mutex{},

		tm: tm,

		coordinatorToStorageChannel: coordinatorToStorageChannel,
		transportToStorageChannel:   transportToStorageChannel,
	}
	go sm.processStorageToStorageMessages()
	go sm.processCoordinatorMessages()
	//go sm.monitorKVStoreSize()

	return &sm
}

func (sm *StorageModule) processStorageToStorageMessages() {
	for {
		message := <-sm.transportToStorageChannel
		messageID := string(message.GetMessageID())

		switch internalMsgCommand := constants.InternalMessageCommands(message.GetCommand()); internalMsgCommand {
		case constants.ProcessStorageToStorageKVRequest:
			// TODO: use the generalized put & get request
			kvRequest := &protobuf.KVRequest{}
			err := proto.Unmarshal(message.GetKVRequest(), kvRequest)
			if err != nil {
				fmt.Printf("[Storage] Failed to unmarshal KVRrequest (id: %s) attached to InternalMsg from storage-to-storage channel.\n", messageID)
				fmt.Printf("[Storage] Ignoring InternalMsg (id: %s) from storage-to-storage channel.\n", messageID)
				continue
			}

			// get destination table
			tableSelection := constants.TableSelection(message.GetDestinationNodeTable())
			kvStore, kvStoreLock, err := sm.getKVStore(tableSelection)
			if err != nil {
				fmt.Printf("[Storage] ProcessStorageToStorageKVRequest request selected a non-existing table. \n\t%s\n", err.Error())
				continue
			}

			// process restricted kv request
			// TODO: why isn't this using the processKVRequest() function?
			// TODO: Should follow the same pattern using processClientKVRequest & processPropagatedKVRequest
			switch kvCommand := kvRequest.GetCommand(); kvCommand {
			case PUT:
				_ = sm.put(kvStore, kvStoreLock, kvRequest.GetKey(), kvRequest.GetValue(), kvRequest.GetVersion())
			case WIPEOUT:
				_ = sm.wipeout(tableSelection)
			default:
				fmt.Printf("[Storage] Storage-to-Storage channel received an unsupported KVRequest command (%d).\n", kvCommand)
			}
		default:
			fmt.Printf("[Storage] Storage-to-Storage received an unsupported InternalMsg command (%d). Only supports (%d) command.\n", internalMsgCommand, constants.ProcessStorageToStorageKVRequest)
		}
	}
}

func (sm *StorageModule) processCoordinatorMessages() {
	for {
		request := <-sm.coordinatorToStorageChannel
		command := request.GetCommand()
		switch constants.InternalMessageCommands(command) {
		case constants.ProcessKeyMigrationRequest:
			err := sm.processKeyMigrationRequest(&request)
			if err != nil {
				fmt.Printf("[Storage] Failed to process KeyMigrationRequest (%s)\n", err)
			}

		case constants.ProcessTableMigrationRequest:
			err := sm.processTableMigrationRequest(&request)
			if err != nil {
				fmt.Printf("[Storage] Failed to process TableMigrationRequest (%s)\n", err)
			}

		case constants.ProcessPropagatedKVRequest:
			err := sm.processPropagatedKVRequest(&request)
			if err != nil {
				fmt.Printf("[Storage] Failed to process PropagatedKVRequest (%s)\n", err)
			}

		case constants.ProcessClientKVRequest:
			err := sm.processClientKVRequest(&request)
			if err != nil {
				fmt.Printf("[Storage] Failed to process ClientKVRequest (%s)\n", err)
			}
		default:
			fmt.Printf("[Storage] Received unrecognized command (%d) from Coordinator", command)
		}
	}
}

func (sm *StorageModule) processKeyMigrationRequest(request *protobuf.InternalMsg) error {
	// extract lowerbound, upperbound, destination address & convert to appropriate
	destination := request.GetMigrationDestinationAddress()

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
	isWrapAround := upperbound.Cmp(lowerbound) == -1 // note: x.Cmp(y) returns -1 if x < y

	// get request's table selections
	originatingTable := constants.TableSelection(request.GetOriginatingNodeTable())
	destinationTable := constants.TableSelection(request.GetDestinationNodeTable())

	// get selected kvStore & its lock
	kvStore, kvStoreLock, err := sm.getKVStore(originatingTable)
	if err != nil {
		return nil
	}

	kvStoreLock.Lock()
	for key, value := range kvStore {
		hashedKey := structure.HashKey(key)
		switch isWrapAround {
		case false:
			// when not wrapped around, the key is in new node's range if the hashedKey is inside [lowerbound:upperbound]
			hashedKeyIsBounded := lowerbound.Cmp(hashedKey) == -1 && hashedKey.Cmp(upperbound) == -1
			if hashedKeyIsBounded {
				// migrate keys
				err := sm.migrateKey(kvStore, key, value, destination, destinationTable)
				if err != nil {
					fmt.Println("[Storage] Failed to migrate key.", err.Error())
				}
			}
		case true:
			// when wrapped around, the key is in new node's range if outside the range [upperbound:lowerbound] where upperbound < lowerbound
			hashedKeyIsBounded := !(upperbound.Cmp(hashedKey) == -1 && hashedKey.Cmp(lowerbound) == -1)
			if hashedKeyIsBounded {
				// migrate keys
				err := sm.migrateKey(kvStore, key, value, destination, destinationTable)
				if err != nil {
					fmt.Println("[Storage] Failed to migrate key.", err.Error())
				}
			}
		}
	}
	kvStoreLock.Unlock()

	// check if wrap around
	return nil
}

func (sm *StorageModule) getKVStore(tableSelection constants.TableSelection) (map[string][]byte, *sync.Mutex, error) {
	switch tableSelection {
	case constants.Head:
		return sm.headKVStore, sm.headKVStoreLock, nil
	case constants.Middle:
		return sm.middleKVStore, sm.middleKVStoreLock, nil
	case constants.Tail:
		return sm.tailKVStore, sm.tailKVStoreLock, nil
	default:
		return nil, nil, errors.New("[Storage] Failed to get KVStore and corresponding lock because tableSelection is unrecognized.")
	}
}

// ASSUMES callee already holds the lock to sm.kvStore
func (sm *StorageModule) migrateKey(
	kvStore map[string][]byte,
	key string, value []byte,
	destination string, destinationTable constants.TableSelection) error {

	internalMessage, err := createInsertMigratedKeyRequest(key, value, destinationTable)
	if err != nil {
		return err
	}

	err = sm.tm.SendStorageToStorage(internalMessage, destination)
	if err != nil {
		return err
	}

	delete(kvStore, key)
	return nil
}

func (sm *StorageModule) processTableMigrationRequest(request *protobuf.InternalMsg) error {
	// extract destination address
	destination := request.GetMigrationDestinationAddress()

	// get request's table selections
	originatingTable := constants.TableSelection(request.GetOriginatingNodeTable())
	destinationTable := constants.TableSelection(request.GetDestinationNodeTable())

	// get selected kvStore & its lock
	kvStore, kvStoreLock, err := sm.getKVStore(originatingTable)
	if err != nil {
		return nil
	}

	// wipeout destination table
	err = sm.wipeoutDestinationTable(destination, destinationTable)
	if err != nil {
		return errors.New("[Storage] Failed to wipeout destination table. Aborting table migration \n" + err.Error())
	}

	// migrate originating table to destination table
	kvStoreLock.Lock()
	for key, value := range kvStore {
		err := sm.migrateKey(kvStore, key, value, destination, destinationTable)
		if err != nil {
			fmt.Println("[Storage] Failed to migrate key while processing TableMigrationRequest.", err.Error())
		}
	}
	kvStoreLock.Unlock()

	return nil
}

func (sm *StorageModule) wipeoutDestinationTable(destination string, destinationTable constants.TableSelection) error {
	internalMessage, err := createWipeoutDestinationTableKVRequest(destination, destinationTable)
	if err != nil {
		return err
	}

	err = sm.tm.SendStorageToStorage(internalMessage, destination)
	if err != nil {
		return err
	}
	return nil
}

func (sm *StorageModule) processClientKVRequest(request *protobuf.InternalMsg) error {
	kvStore, kvStoreLock := sm.headKVStore, sm.headKVStoreLock // note: hard-coded in case the request didn't set the table
	err := sm.processKVRequest(request, true, kvStore, kvStoreLock)
	return err
}

func (sm *StorageModule) processPropagatedKVRequest(request *protobuf.InternalMsg) error {
	tableSelection := constants.TableSelection(request.GetDestinationNodeTable())
	kvStore, kvStoreLock, err := sm.getKVStore(tableSelection)
	if err != nil {
		return err
	}
	err = sm.processKVRequest(request, false, kvStore, kvStoreLock)
	return err
}

/*	Client KVRequests are inserted into the Head KVStore.

	TODO: confirm with the coordinator that when a client request arrives at the head of the correct node,
	it will forward the exact InternalMsg forwarded by the transport layer.
*/
func (sm *StorageModule) processKVRequest(request *protobuf.InternalMsg, responseToClientRequired bool, kvStore map[string][]byte, kvStoreLock *sync.Mutex) error {
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
			errCode = sm.put(kvStore, kvStoreLock, cast_req.GetKey(), cast_req.GetValue(), cast_req.GetVersion())
		case GET:
			value, errCode = sm.get(kvStore, kvStoreLock, cast_req.Key)
		case REMOVE:
			errCode = sm.remove(kvStore, kvStoreLock, cast_req.Key)
		case SHUTDOWN:
			shutdown()
		case WIPEOUT:
			errCode = sm.wipeout(constants.Head)
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
	if responseToClientRequired {
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
	}
	return nil
}

func (sm *StorageModule) put(kvStore map[string][]byte, kvStoreLock *sync.Mutex, key []byte, value []byte, version int32) uint32 {
	if int(len(value)) > 10000 {
		return INV_VAL
	}

	memSize := getCurrMem()
	if memSize < uint64(STORE_SIZE_MB) {
		kvStoreLock.Lock()
		kvStore[string(key)] = value
		kvStoreLock.Unlock()
	} else {
		fmt.Println("[Storage] Fatal: not enough memory. ", memSize)
		return NO_SPACE
	}

	return OK
}

// @Description:Returns the value and version that is associated with the key. If there is no such key in your store, the store should return error (not found).
// @param key
// @return []byte
func (sm *StorageModule) get(kvStore map[string][]byte, kvStoreLock *sync.Mutex, key []byte) ([]byte, uint32) {
	kvStoreLock.Lock()
	value, found := kvStore[string(key)]
	kvStoreLock.Unlock()

	if !found {
		return nil, NO_KEY
	}

	return value, OK
}

func (sm *StorageModule) remove(kvStore map[string][]byte, kvStoreLock *sync.Mutex, key []byte) uint32 {
	kvStoreLock.Lock()
	_, found := kvStore[string(key)]
	kvStoreLock.Unlock()

	if found {
		kvStoreLock.Lock()
		delete(kvStore, string(key))
		kvStoreLock.Unlock()
		return OK
	}

	return NO_KEY

}

// @Description: calls os.shutdown
func shutdown() {
	os.Exit(555)
}

// @Description: clears the database
func (sm *StorageModule) wipeout(tableSelection constants.TableSelection) uint32 {
	switch tableSelection {
	case constants.Head:
		sm.headKVStoreLock.Lock()
		sm.headKVStore = make(map[string][]byte)
		sm.headKVStoreLock.Unlock()
	case constants.Middle:
		sm.middleKVStoreLock.Lock()
		sm.middleKVStore = make(map[string][]byte)
		sm.middleKVStoreLock.Unlock()
	case constants.Tail:
		sm.tailKVStoreLock.Lock()
		sm.tailKVStore = make(map[string][]byte)
		sm.tailKVStoreLock.Unlock()
	default:
		fmt.Printf("[Storage] Received a wipeout command for a non-existing table (%d)\n.", tableSelection)
	}

	return OK
}

// @Description: response indicating server is alive
func is_alive() uint32 {
	fmt.Println("CLIENT ASKED IF SERVER ALIVE")

	return OK
}

// @Description: gets the current procressID
func getpid() (int32, uint32) {
	pid := int32(syscall.Getpid())

	return pid, OK
}

// @Description: returns number of members
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

func (sm *StorageModule) monitorKVStoreSize() {
	for {
		time.Sleep(5 * time.Second)
		fmt.Printf("[Storage] Number of keys in KVStores (Head: %d, Middle: %d, Tail: %d)\n",
			len(sm.headKVStore), len(sm.middleKVStore), len(sm.tailKVStore))
	}
}
