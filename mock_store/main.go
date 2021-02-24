package main

import (
	"dht/google_protocol_buffer/pb/protobuf"
	"dht/mock_store/membership"
	"dht/mock_store/storage"
	transport2 "dht/mock_store/transport"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

const MAX_RECEIVE_CHANNEL = 2

func main() {
	// TODO: parse command line args
	arguments := os.Args[1:]
	port, initialMembers, err := validateArgs(arguments)
	if err != nil {
		log.Fatal(err)
		log.Fatal("Error occurred while validating arguments")
	}

	// TODO: initialize channels
	storageChannel := make(chan protobuf.InternalMsg, 2)
	gmsChannel := make(chan []byte, 2)

	// TODO: initialize
	// transport, err := transport.New()
	portInt, err := strconv.Atoi(port)
	transport, err := transport2.New(portInt, gmsChannel, storageChannel)
	go transport.Init_server()

	

	x, _ := membership.New(initialMembers, transport,
		 gmsChannel, "127.0.0.1", port)

	go storage.StorageModule(transport, storageChannel, x)

	for {
		time.Sleep(1 * time.Second) // just so the app doesn't close after finishing execution.
		allMembers := x.GetAllNodes()
		fmt.Println(allMembers)
	}
	
}

func validateArgs(args []string) (port string, initialMembers []string, err error) {
	// if len(args) != 2 {
	// 	return "", nil, errors.New("Must provide exactly 2 arguments")
	// }

	port = args[0]

	parsedPort, err := strconv.Atoi(port)
	if err != nil {
		return "", nil, errors.New("Failed to parse port")
	}
	if parsedPort < 0 || parsedPort > 65535 {
		return "", nil, errors.New("Invalid port number")
	}
	
	initialMembers = []string{"127.0.0.1:4001", "127.0.0.1:4002", "127.0.0.1:4003", "127.0.0.1:4004", "127.0.0.1:4005", "127.0.0.1:4006","127.0.0.1:4007", "127.0.0.1:4008", "127.0.0.1:4009","127.0.0.1:4010"} 

	// peersFilePath := args[1]
	// file, err := os.Open(peersFilePath)
	// if err != nil {
	// 	return "", nil, errors.New("Unable to open peers file")
	// }
	// defer file.Close()

	// scanner := bufio.NewScanner(file)
	// for scanner.Scan() {
	// 	text := scanner.Text()
	// 	// TODO: validate ip addresses
	// 	initialMembers = append(initialMembers, text)
	// }

	// if err := scanner.Err(); err != nil {
	// 	return "", nil, errors.New("Failed to read peers file using scanner")
	// }

	return port, initialMembers, nil
}

