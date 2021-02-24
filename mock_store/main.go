package main

import (
	"dht/google_protocol_buffer/pb/protobuf"
	"dht/mock_store/transport"
	"log"
	"os"
	"strconv"
	"dht/mock_store/storage"
)


func main() {

	var GroupSend = make(chan transport.Message)
	var StorageSend = make(chan protobuf.InternalMsg)

	log.Println("MAIN 123456")
	argsWithProg := os.Args
	if len(argsWithProg) != 2 {
		log.Printf("INVALID NUMBER OF ARGUMENTS, EXITTING....\n")
	} else {
		port, _ := strconv.Atoi(argsWithProg[1])
		tm, _ := transport.New(port, GroupSend,StorageSend)
		go storage.StorageModule(tm,StorageSend)
		tm.Init_server()
	}

}

