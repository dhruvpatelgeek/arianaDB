package storage

import (
	"fmt"
	"dht/google_protocol_buffer/pb/protobuf"
	"log"
	"os"
	"sync"
	"syscall"
	"unsafe"

	"github.com/golang/protobuf/proto"
)

//REFRENCES--------------------------------
//https://stackoverflow.com/questions/28400340/how-support-concurrent-connections-with-a-udp-server-using-go
//https://stackoverflow.com/questions/27625787/measuring-memory-usage-of-executable-run-using-golang
//https://stackoverflow.com/questions/31879817/golang-os-exec-realtime-memory-usage
//https://golangcode.com/print-the-current-memory-usage/

//CONTROL PANEL----------------------------
var debug = false
var MAP_SIZE_MB = 70

//MAP_AND_CACHE----------------------------
var storage = make(map[string][]byte)
var mutex sync.Mutex


/**
 * @Description: peals the seconday message layer and performs server functions returns the genarated payload
 * @param message
 * @return []byte
 */
 func Message_handler(message []byte) ([]byte,bool) {
	cast_req := &protobuf.KVRequest{}
	error := proto.Unmarshal(message, cast_req)
	if error != nil {
		fmt.Printf("\nUNPACK ERROR %+v\n", error)
	}

	//* 0x01 - Put: This is a put operation.
	//* 0x02 - Get: This is a get operation.
	//* 0x03 - Remove: This is a remove operation.
	//* 0x04 - Shutdown: shuts-down the node (used for testing and management).
	//* 0x05 - Wipeout: deletes all keys stored in the node (used for testing).
	//* 0x06 - IsAlive: does nothing but replies with success if the node is alive.
	//* 0x07 - GetPID: the node is expected to reply with the processID of the Go process
	//* 0x08 - GetMembershipCount:(This will be used later, for now you are expected to return 1.)

	switch cast_req.GetCommand() {
	case 1:
		{
			if debug {
				fmt.Println("PUT")
			}
			return put(cast_req.GetKey(), cast_req.GetValue(), cast_req.GetVersion()), false
		}
	case 2:
		{
			if debug {
				fmt.Println("GET")
			}
			return get(cast_req.Key), false
		}
	case 3:
		{
			if debug {
				fmt.Println("REMOVE")
			}
			return remove(cast_req.Key), true
		}
	case 4:
		{
			if debug {
				fmt.Println("SHUTDOWN")
			}
			fmt.Println("[TERMINATING]....")
			shutdown()
		}
	case 5:
		{
			if debug {
				fmt.Println("WIPEOUT")
			}
			return wipeout(), true
		}
	case 6:
		{
			if debug {
				fmt.Println("IsALIVE")
			}
			return is_alive(), true
		}
	case 7:
		{
			if debug {
				fmt.Println("GetPID")
			}
			return getpid(), true
		}
	case 8:
		{
			if debug {
				fmt.Println("GetMembershipCount")
			}
			return getmemcount(), true
		}
	default:
		{
			if debug {
				fmt.Println("INVALID COMMAND")
			}
			return message, true
		}
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
	var error_code uint32 = 0
	error_code = 0
	if int(len(value)) > 10000 {
		error_code = 7
		var err_code *uint32
		err_code = new(uint32)
		err_code = &error_code

		var _pid *int32
		_pid = new(int32)
		procress_id := int32(syscall.Getpid())
		_pid = &procress_id
		payload := &protobuf.KVResponse{
			ErrCode: err_code,
			Value:   nil, // edited
			Pid:     _pid,
		}
		out, err := proto.Marshal(payload)
		if err != nil {
			log.Fatalln("Failed to encode address book:", err)
		}

		return out
	}
	mutex.Lock() //<<<<<<<<<<<<<<<MAP LOCK

	if bToMb(uint64(unsafe.Sizeof(storage))) < uint64(MAP_SIZE_MB) {

		storage[string(key)] = value // adding the value
		error_code = 0
		if debug {
			fmt.Println("PUT", string(key), ",<->", string(value))
		}
	} else {
		if debug {
			fmt.Println("ERROR PUTTING")
		}
		error_code = 2
	}
	mutex.Unlock() //<<<<<<<<<<<<<<<MAP UNLOCK

	var err_code *uint32
	err_code = new(uint32)
	err_code = &error_code

	var _pid *int32
	_pid = new(int32)
	procress_id := int32(syscall.Getpid())
	_pid = &procress_id
	payload := &protobuf.KVResponse{
		ErrCode: err_code,
		Value:   value,
		Pid:     _pid,
	}
	out, err := proto.Marshal(payload)
	if err != nil {
		log.Fatalln("Failed to encode address book:", err)
	} else {
		if debug {
			//fmt.Printf("NEW BUFFER-> %+v",out);
		}
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
	mutex.Unlock()
	var error_code uint32
	if found {
		error_code = 0
	} else {
		error_code = 1
	}

	var err_code *uint32
	err_code = new(uint32)
	err_code = &error_code

	var _pid *int32
	_pid = new(int32)
	procress_id := int32(syscall.Getpid())
	_pid = &procress_id
	payload := &protobuf.KVResponse{
		ErrCode: err_code,
		Value:   value,
		Pid:     _pid,
	}
	out, err := proto.Marshal(payload)
	if err != nil {
		log.Fatalln("Failed to encode address book:", err)
	} else {
		if debug {
			//fmt.Printf("NEW BUFFER-> %+v",out);
		}
	}

	return out
}

/**
 * @Description:Removes the value that is associated with the key.
 * @param key
 * @return []byte
 */
func remove(key []byte) []byte {
	mutex.Lock()
	value, found := storage[string(key)]
	if found {
		delete(storage, string(key))
	}
	mutex.Unlock()
	if found {
		var error_code uint32
		error_code = 0
		var err_code *uint32
		err_code = new(uint32)
		err_code = &error_code

		var _pid *int32
		_pid = new(int32)
		procress_id := int32(syscall.Getpid())
		_pid = &procress_id
		payload := &protobuf.KVResponse{
			ErrCode: err_code,
			Value:   value,
			Pid:     _pid,
		}
		out, err := proto.Marshal(payload)
		if err != nil {
			log.Fatalln("Failed to encode address book:", err)
		} else {
			if debug {
				//fmt.Printf("NEW BUFFER-> %+v",out);
			}
		}

		return out

	} else {
		var error_code uint32
		error_code = 1
		var err_code *uint32
		err_code = new(uint32)
		err_code = &error_code

		var _pid *int32
		_pid = new(int32)
		procress_id := int32(syscall.Getpid())
		_pid = &procress_id
		payload := &protobuf.KVResponse{
			ErrCode: err_code,
			Value:   value,
			Pid:     _pid,
		}
		out, err := proto.Marshal(payload)
		if err != nil {
			log.Fatalln("Failed to encode address book:", err)
		} else {
			if debug {
				//fmt.Printf("NEW BUFFER-> %+v",out);
			}
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
	for k := range storage {
		delete(storage, k)
	}
	mutex.Unlock() //<<<<<<<<<<<<<<<MAP UNLOCK
	var error_code uint32
	var err_code *uint32
	err_code = new(uint32)
	err_code = &error_code

	var _pid *int32
	_pid = new(int32)
	procress_id := int32(syscall.Getpid())
	_pid = &procress_id
	payload := &protobuf.KVResponse{
		ErrCode: err_code,
		Pid:     _pid,
	}
	out, err := proto.Marshal(payload)
	if err != nil {
		log.Fatalln("Failed to encode address book:", err)
	} else {
		if debug {
			//fmt.Printf("NEW BUFFER-> %+v",out);
		}
	}

	return out
}

/**
 * @Description: response indicating server is alive
 * @return []byte
 */
func is_alive() []byte {
	fmt.Println("CLIENT ASKED IF SERVER ALIVE")

	var error_code uint32
	error_code = 0

	var err_code *uint32
	err_code = new(uint32)
	err_code = &error_code

	var _pid *int32
	_pid = new(int32)
	procress_id := int32(syscall.Getpid())
	_pid = &procress_id
	payload := &protobuf.KVResponse{
		ErrCode: err_code,
		Pid:     _pid,
	}
	out, err := proto.Marshal(payload)
	if err != nil {
		log.Fatalln("Failed to encode address book:", err)
	} else {
		if debug {
			//fmt.Printf("NEW BUFFER-> %+v",out);
		}
	}
	return out
}

/**
 * @Description: gets the current procressID
 * @return []byte
 */
func getpid() []byte {
	var error_code uint32
	error_code = 0

	var err_code *uint32
	err_code = new(uint32)
	err_code = &error_code

	var _pid *int32
	_pid = new(int32)
	procress_id := int32(syscall.Getpid())
	_pid = &procress_id
	payload := &protobuf.KVResponse{
		ErrCode: err_code,
		Pid:     _pid,
	}
	out, err := proto.Marshal(payload)
	if err != nil {
		log.Fatalln("Failed to encode address book:", err)
	} else {
		if debug {
			//fmt.Printf("NEW BUFFER-> %+v",out);
		}
	}
	return out
}

/**
 * @Description: returns number of members
 * @return []byte
 */
func getmemcount() []byte {
	var error_code uint32
	error_code = 0

	var err_code *uint32
	err_code = new(uint32)
	err_code = &error_code

	var _pid *int32
	_pid = new(int32)
	procress_id := int32(syscall.Getpid())
	_pid = &procress_id
	payload := &protobuf.KVResponse{
		ErrCode: err_code,
		Pid:     _pid,
	}
	out, err := proto.Marshal(payload)
	if err != nil {
		log.Fatalln("Failed to encode address book:", err)
	} else {
		if debug {
			//fmt.Printf("NEW BUFFER-> %+v",out);
		}
	}
	return out
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}