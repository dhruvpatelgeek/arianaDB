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

type StorageService struct {
	hostIpv4 string
	// replicated kv stores
	headKVStore   *sync.Map
	middleKVStore *sync.Map
	tailKVStore   *sync.Map

	tm  *transport.TransportModule
	gms *membership.MembershipService

	// TODO: consider making this a channel of pointers to prevent copying protobufs with inherent mutex
	// when popping from channel and passing as arguments to functions
	coordinatorToStorageChannel chan protobuf.InternalMsg
	transportToStorageChannel   chan protobuf.InternalMsg
}

func New(
	hostname string,
	hostport string,
	tm *transport.TransportModule,
	gms *membership.MembershipService,

	coordinatorToStorageChannel chan protobuf.InternalMsg,
	transportToStorageChannel chan protobuf.InternalMsg) *StorageService {

	sm := StorageService{
		hostIpv4: hostname + ":" + hostport,

		headKVStore:   new(sync.Map),
		middleKVStore: new(sync.Map),
		tailKVStore:   new(sync.Map),

		tm:  tm,
		gms: gms,

		coordinatorToStorageChannel: coordinatorToStorageChannel,
		transportToStorageChannel:   transportToStorageChannel,
	}
	go sm.processStorageToStorageMessages()

	for i := 0; i < NUM_KVREQUEST_WORKERS; i++ {
		go sm.processCoordinatorMessages()
	}

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
			_, err = sm.getKVStore(tableSelection)
			if err != nil {
				fmt.Printf("[Storage] ProcessStorageToStorageKVRequest request selected a non-existing table. \n\t%s\n", err.Error())
				continue
			}

			// process restricted kv request
			// TODO: Should follow the same pattern using processClientKVRequest & processPropagatedKVRequest
			switch kvCommand := kvRequest.GetCommand(); kvCommand {
			case PUT:
				_ = sm.put(tableSelection, kvRequest.GetKey(), kvRequest.GetValue())
			case WIPEOUT:
				fmt.Printf("[Storage] Received wipeout command from (TODO) to wipe my table :%d\n", tableSelection)
				_, err = sm.wipeout(tableSelection)
				if err != nil {
					fmt.Println(err)
				}
			default:
				fmt.Printf("[Storage] Storage-to-Storage channel received an unsupported KVRequest command (%d).\n", kvCommand)
			}
		default:
			fmt.Printf("[Storage] Storage-to-Storage received an unsupported InternalMsg command (%d). Only supports (%d) command.\n", internalMsgCommand, constants.ProcessStorageToStorageKVRequest)
			fmt.Printf("\t Command 'ProcessClientKVRequest': (%d)\n", constants.ProcessClientKVRequest)
			fmt.Printf("\t Command 'ProcessPropagatedKVRequest': (%d)\n", constants.ProcessPropagatedKVRequest)
			fmt.Printf("\t Command 'ProcessStorageToStorageKVRequest': (%d)\n", constants.ProcessStorageToStorageKVRequest)
			fmt.Printf("\t Command 'SplitTableRequest': (%d)\n", constants.SplitTableRequest)
			fmt.Printf("\t Command 'ProcessHeadTableMigrationRequest': (%d)\n", constants.ProcessHeadTableMigrationRequest)
			fmt.Printf("\t Command 'ProcessMigratingHeadTableRequest': (%d)\n", constants.ProcessMigratingHeadTableRequest)
		}
	}
}

func (sm *StorageService) processCoordinatorMessages() {
	for {
		request := <-sm.coordinatorToStorageChannel
		// fmt.Printf("[Storage] [Info] client kv request backlog size: %d\n", len(sm.coordinatorToStorageChannel))
		err := sm.processKVRequest(&request)
		if err != nil {
			fmt.Printf("[Storage] Failed to process PropagatedKVRequest (%s)\n", err)
		}
	}
}

/*
MigratePartialTable is a thread-safe function which blocks the caller until the subset of the keys are migrated to the destination. The migrated
subset of keys will be removed from the originating table.

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
	kvStore, err := sm.getKVStore(originatingTable)
	if err != nil {
		return err
	}

	kvStore.Range(func(k, v interface{}) bool {
		key, _ := k.(string)
		value, _ := v.([]byte)
		hashedKey := structure.HashKey(key) // TODO: maybe we should store the hashed keys in the first place?

		switch isWrapAround {
		case false:
			// when not wrapped around, the key is in new node's range if the hashedKey is inside [lowerbound:upperbound]
			hashedKeyIsBounded := lowerbound.Cmp(hashedKey) == -1 && hashedKey.Cmp(upperbound) == -1
			if hashedKeyIsBounded {
				// migrate keys
				err := sm.migrateKey(originatingTable, key, value, destination, destinationTable)
				if err != nil {
					fmt.Println("[Storage] Failed to migrate key during partial table migration.", err.Error())
				}
				kvStore.Delete(k)
			}
		case true:
			// when wrapped around, the key is in new node's range if outside the range [upperbound:lowerbound] where upperbound < lowerbound
			hashedKeyIsBounded := !(upperbound.Cmp(hashedKey) == -1 && hashedKey.Cmp(lowerbound) == -1)
			if hashedKeyIsBounded {
				// migrate keys
				err := sm.migrateKey(originatingTable, key, value, destination, destinationTable)
				if err != nil {
					fmt.Println("[Storage] Failed to migrate key during partial table migration.", err.Error())
				}
				kvStore.Delete(k)
			}
		}

		return true
	})

	return nil
}

func (sm *StorageService) getKVStore(tableSelection constants.TableSelection) (*sync.Map, error) {
	switch tableSelection {
	case constants.Head:
		return sm.headKVStore, nil
	case constants.Middle:
		return sm.middleKVStore, nil
	case constants.Tail:
		return sm.tailKVStore, nil
	default:
		return nil, errors.New("[Storage] Failed to get KVStore and corresponding lock because tableSelection is unrecognized.")
	}
}

/** migrateKey() migrates a key-value pair from the local key-value store to
the rdestination's table. The local key is not deleted after the migration. migrateKey()
throws an error if it was unable to migrate the key.

- assumes callee already holds the lock to sm.kvStore
*/
func (sm *StorageService) migrateKey(
	tableSelection constants.TableSelection,
	key string, value []byte,
	destination string, destinationTable constants.TableSelection) error {

	internalMessage, err := createInsertMigratedKeyRequest(key, value, destinationTable)
	if err != nil {
		return err
	}

	if destination == sm.hostIpv4 {
		sm.put(tableSelection, []byte(key), value)
	} else {
		err = sm.tm.SendStorageToStorage(internalMessage, destination)
		if err != nil {
			fmt.Println("[Storage] Failed to migrate key remotely due to ", err.Error())
			return err
		}
	}
	return nil
}

/*
MigrateTable is a thread-safe function which blocks the caller until the destination table is wiped
and the originating table is migrated to the destination table. After migration, the originating table will be preserved.

MigrateTable assumes the destination is valid and the destinationTable is valid.

MigrateTable will abort the table migration if:
1. an invaliad originating table is chosen.
1. it is unable to wipe out the destination table.

If a key fails to transfer to the destination table during table migration, it will be deleted from the local
copy and continue migrating the remaining keys. This failure may result in loss of keys.
*/
func (sm *StorageService) MigrateEntireTable(destination string,
	originatingTable constants.TableSelection,
	destinationTable constants.TableSelection) error {

	// get selected kvStore & its lock
	kvStore, err := sm.getKVStore(originatingTable)
	if err != nil {
		return err
	}

	// wipeout destination table
	err = sm.wipeoutDestinationTable(destination, destinationTable)
	if err != nil {
		return fmt.Errorf("[Storage] Failed to wipeout destination (%s) table (%d). \n Caused by: %s. \nAbsorting table migration.\n", destination, destinationTable, err.Error())
	}
	// migrate originating table to destination table
	kvStore.Range(func(k, v interface{}) bool {
		key, _ := k.(string)
		value, _ := v.([]byte)

		err := sm.migrateKey(originatingTable, key, value, destination, destinationTable)
		if err != nil {
			fmt.Println("[Storage] Failed to migrate key during table migration. ", err.Error())
		}
		return true
	})

	return nil
}

/**	MergeTables locally transfer the contents of source table into the destination table without
first removing the contents of the destination table.

This operation will wipeout the source table.
*/
func (sm *StorageService) MergeTables(
	destinationTable constants.TableSelection,
	sourceTable constants.TableSelection) error {

	// get selected kvStore & its lock
	sourceKVStore, err := sm.getKVStore(sourceTable)
	if err != nil {
		return err
	}

	destinationKVStore, err := sm.getKVStore(destinationTable)
	if err != nil {
		return err
	}

	// TODO: block all other operations on the source and destination tables
	fmt.Printf("[Storage] Merging table %d into %d.\n", sourceTable, destinationTable)
	// migrate originating table to destination table
	sourceKVStore.Range(func(k, v interface{}) bool {
		destinationKVStore.Store(k, v)
		return true
	})
	// clear source table for ease of debugging
	sourceKVStore.Range(func(key interface{}, value interface{}) bool {
		sourceKVStore.Delete(key)
		return true
	})

	return nil
}

/** wipeoutDestinationTable() attempts to wipeout the destination table by making a wipeout request. Returns
nil if the wipeout request was successfully sent. Nil return value doesn't imply that the destination table
is wiped, only that the request has been sent.

- throws an error if the wipeout request failed to send.
*/
func (sm *StorageService) wipeoutDestinationTable(destination string, destinationTable constants.TableSelection) error {
	internalMessage, err := createWipeoutDestinationTableKVRequest(destination, destinationTable)
	if err != nil {
		return err
	}

	if destination == sm.hostIpv4 {
		sm.wipeout(destinationTable)
	} else {
		err = sm.tm.SendStorageToStorage(internalMessage, destination)
		if err != nil {
			fmt.Println("[Storage] Failed to wipeout destination table due to ", err.Error())
			return err
		}
	}
	return nil
}

/*	Client KVRequests are inserted into the Head KVStore.

	TODO: confirm with the coordinator that when a client request arrives at the head of the correct node,
	it will forward the exact InternalMsg forwarded by the transport layer.
*/
func (sm *StorageService) processKVRequest(request *protobuf.InternalMsg) error {
	tableSelection := constants.TableSelection(request.GetDestinationNodeTable())

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
			errCode = sm.put(tableSelection, cast_req.GetKey(), cast_req.GetValue())
		case GET:
			value, errCode = sm.get(tableSelection, cast_req.Key)
		case REMOVE:
			errCode = sm.remove(tableSelection, cast_req.Key)
		case SHUTDOWN:
			shutdown()
		case WIPEOUT:
			errCode, _ = sm.wipeout(tableSelection)
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

	responseRequired := request.GetRespondToClient()
	if responseRequired == false && cast_req.GetCommand() == GET {
		fmt.Println("[Storage] [Error] Received a client Get request for which responseRequired is false.")
	}

	if responseRequired {
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

func (sm *StorageService) put(tableSelection constants.TableSelection, key []byte, value []byte) uint32 {
	if int(len(value)) > 10000 {
		return INV_VAL
	}

	memSize := getCurrMem()
	if memSize < uint64(STORE_SIZE_MB) {
		kvStore, _ := sm.getKVStore(tableSelection)
		kvStore.Store(string(key), value)
	} else {
		fmt.Println("[Storage] Fatal: not enough memory. ", memSize)
		return NO_SPACE
	}

	return OK
}

func (sm *StorageService) get(tableSelection constants.TableSelection, key []byte) ([]byte, uint32) {
	kvStore, _ := sm.getKVStore(tableSelection)
	v, found := kvStore.Load(string(key))

	if !found {
		log.Printf("[Storage] [Warning] Could not find key.\n")
		return nil, NO_KEY
	}

	value, isCastSuccessful := v.([]byte)
	if !isCastSuccessful {
		log.Printf("[Storage] [Error] Failed to cast value to []byte.")
		return nil, NO_KEY
	}

	return value, OK
}

func (sm *StorageService) remove(tableSelection constants.TableSelection, key []byte) uint32 {
	kvStore, _ := sm.getKVStore(tableSelection)
	_, found := kvStore.Load(string(key))

	if found {
		kvStore.Delete(string(key))
		return OK
	} else {
		return NO_KEY
	}
}

func shutdown() {
	fmt.Println("[Storage] [Info] Received command to shutdown.")
	os.Exit(555)
}

func (sm *StorageService) wipeout(tableSelection constants.TableSelection) (uint32, error) {
	kvstore, err := sm.getKVStore(tableSelection)
	if err != nil {
		err = fmt.Errorf("[Storage] Received a wipeout command for a non-existing table (%d)\n.", tableSelection)
		return 0, err
	}

	kvstore.Range(func(k, val interface{}) bool {
		kvstore.Delete(k)
		return true
	})

	return OK, nil
}

func is_alive() uint32 {
	fmt.Println("[Storage] [Info] Client asked if I am alive.")
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
