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
	"dht/src/membership"
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

type StorageService struct {
	// replicated kv stores
	headKVStore     map[string][]byte
	headKVStoreLock *sync.Mutex

	middleKVStore     map[string][]byte
	middleKVStoreLock *sync.Mutex

	tailKVStore     map[string][]byte
	tailKVStoreLock *sync.Mutex

	tm  *transport.TransportModule
	gms *membership.MembershipService

	// TODO: consider making this a channel of pointers to prevent copying protobufs with inherent mutex
	// when popping from channel and passing as arguments to functions
	coordinatorToStorageChannel chan protobuf.InternalMsg
	transportToStorageChannel   chan protobuf.InternalMsg
}

func New(tm *transport.TransportModule,
	gms *membership.MembershipService,

	coordinatorToStorageChannel chan protobuf.InternalMsg,
	transportToStorageChannel chan protobuf.InternalMsg) *StorageService {

	sm := StorageService{
		headKVStore:     make(map[string][]byte),
		headKVStoreLock: &sync.Mutex{},

		middleKVStore:     make(map[string][]byte),
		middleKVStoreLock: &sync.Mutex{},

		tailKVStore:     make(map[string][]byte),
		tailKVStoreLock: &sync.Mutex{},

		tm:  tm,
		gms: gms,

		coordinatorToStorageChannel: coordinatorToStorageChannel,
		transportToStorageChannel:   transportToStorageChannel,
	}
	go sm.processStorageToStorageMessages()
	go sm.processCoordinatorMessages()
	// go sm.monitorKVStoreSize()

	return &sm
}

func (sm *StorageService) processStorageToStorageMessages() {
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

func (sm *StorageService) processCoordinatorMessages() {
	for {
		request := <-sm.coordinatorToStorageChannel
		command := request.GetCommand()
		switch constants.InternalMessageCommands(command) {
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

/**
MigratePartialTable is a thread-safe function which blocks the caller until the subset of the keys are migrated to the destination.

MigrateTable assumes the destination is valid and the destinationTable is valid.

The subset of keys to be migrated in the originating table is defined by the migrationRangeLowerbound and migrationRangeUpperbound.
1. If migrationRangeLowerbound < migrationRangeUpperbound, we will migrate keys within [migrationRangeLowerbound, migrationRangeUpperbound].
2. If migrationRangeLowerbound > migrationRangeUpperbound (i.e.: wrapped around), we will migrate keys not in [migrationRangeLowerbound, migrationRangeUpperbound].

If a key fails to transfer to the destination table during table migration, it will be deleted from the local
copy and continue migrating the remaining keys. This failure may result in loss of keys.
*/
func (sm *StorageService) MigratePartialTable(destination string,
	originatingTable constants.TableSelection, destinationTable constants.TableSelection,
	migrationRangeLowerbound string, migrationRangeUpperbound string) error {

	lowerbound := new(big.Int)
	_, success := lowerbound.SetString(migrationRangeLowerbound, 10)
	if !success {
		return errors.New("Storage: Failed to convert the lowerbound from string to big.int while processing migrating request. Ignoring.")
	}

	upperbound := new(big.Int)
	_, success = upperbound.SetString(migrationRangeUpperbound, 10)
	if !success {
		return errors.New("Storage: Failed to convert the upperbound from string to big.int while processing migrating request. Ignoring.")
	}

	// the new node's range is wrapped around if upperbound < lowerbound
	isWrapAround := upperbound.Cmp(lowerbound) == -1 // note: x.Cmp(y) returns -1 if x < y

	// get selected kvStore & its lock
	kvStore, kvStoreLock, err := sm.getKVStore(originatingTable)
	if err != nil {
		return err
	}

	kvStoreLock.Lock() // TODO: consider grabbing the lock sooner to prevent table modification
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
					fmt.Println("[Storage] Failed to migrate key during partial table migration.", err.Error())
				}
			}
		case true:
			// when wrapped around, the key is in new node's range if outside the range [upperbound:lowerbound] where upperbound < lowerbound
			hashedKeyIsBounded := !(upperbound.Cmp(hashedKey) == -1 && hashedKey.Cmp(lowerbound) == -1)
			if hashedKeyIsBounded {
				// migrate keys
				err := sm.migrateKey(kvStore, key, value, destination, destinationTable)
				if err != nil {
					fmt.Println("[Storage] Failed to migrate key during partial table migration.", err.Error())
				}
			}
		}
	}
	kvStoreLock.Unlock()

	// check if wrap around
	return nil
}

func (sm *StorageService) getKVStore(tableSelection constants.TableSelection) (map[string][]byte, *sync.Mutex, error) {
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
func (sm *StorageService) migrateKey(
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

/**
MigrateTable is a thread-safe function which blocks the caller until the destination table is wiped
and the originating table is migrated to the destination table.

MigrateTable assumes the destination is valid and the destinationTable is valid.

MigrateTable will abort the table migration if:
1. an invaliad originating table is chosen.
1. it is unable to wipe out the destination table.

If a key fails to transfer to the destination table during table migration, it will be deleted from the local
copy and continue migrating the remaining keys. This failure may result in loss of keys.
*/
func (sm *StorageService) MigrateTable(destination string,
	originatingTable constants.TableSelection,
	destinationTable constants.TableSelection) error {

	// get selected kvStore & its lock
	kvStore, kvStoreLock, err := sm.getKVStore(originatingTable)
	if err != nil {
		return err
	}

	// wipeout destination table
	kvStoreLock.Lock()
	err = sm.wipeoutDestinationTable(destination, destinationTable)
	if err != nil {
		kvStoreLock.Unlock()
		return errors.New("[Storage] Failed to wipeout destination table. Aborting table migration \n" + err.Error())
	}

	// migrate originating table to destination table
	for key, value := range kvStore {
		err := sm.migrateKey(kvStore, key, value, destination, destinationTable)
		if err != nil {
			fmt.Println("[Storage] Failed to migrate key during table migration. ", err.Error())
		}
	}
	kvStoreLock.Unlock()

	return nil
}

func (sm *StorageService) wipeoutDestinationTable(destination string, destinationTable constants.TableSelection) error {
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

func (sm *StorageService) processClientKVRequest(request *protobuf.InternalMsg) error {
	kvStore, kvStoreLock := sm.headKVStore, sm.headKVStoreLock // note: hard-coded in case the request didn't set the table
	err := sm.processKVRequest(request, true, kvStore, kvStoreLock)
	return err
}

func (sm *StorageService) processPropagatedKVRequest(request *protobuf.InternalMsg) error {
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
func (sm *StorageService) processKVRequest(request *protobuf.InternalMsg, responseToClientRequired bool, kvStore map[string][]byte, kvStoreLock *sync.Mutex) error {
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
			_ = sm.getmemcount()
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

func (sm *StorageService) put(kvStore map[string][]byte, kvStoreLock *sync.Mutex, key []byte, value []byte, version int32) uint32 {
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

func (sm *StorageService) get(kvStore map[string][]byte, kvStoreLock *sync.Mutex, key []byte) ([]byte, uint32) {
	kvStoreLock.Lock()
	value, found := kvStore[string(key)]
	kvStoreLock.Unlock()

	if !found {
		return nil, NO_KEY
	}

	return value, OK
}

func (sm *StorageService) remove(kvStore map[string][]byte, kvStoreLock *sync.Mutex, key []byte) uint32 {
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

func shutdown() {
	os.Exit(555)
}

func (sm *StorageService) wipeout(tableSelection constants.TableSelection) uint32 {
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

func is_alive() uint32 {
	fmt.Println("CLIENT ASKED IF SERVER ALIVE")

	return OK
}

func getpid() (int32, uint32) {
	pid := int32(syscall.Getpid())

	return pid, OK
}

// @Description: returns number of members
func (sm *StorageService) getmemcount() int32 {
	return int32(len(sm.gms.GetAllNodes()))
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func getCurrMem() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return bToMb((m.Alloc + m.StackInuse + m.MSpanInuse + m.MCacheInuse))
}

func (sm *StorageService) monitorKVStoreSize() {
	for {
		time.Sleep(5 * time.Second)
		fmt.Printf("[Storage] Number of keys in KVStores (Head: %d, Middle: %d, Tail: %d)\n",
			len(sm.headKVStore), len(sm.middleKVStore), len(sm.tailKVStore))
	}
}
