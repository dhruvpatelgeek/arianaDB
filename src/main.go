package main

import (
	"bufio"
	"dht/google_protocol_buffer/pb/protobuf"
	"dht/src/coordinator"
	"dht/src/membership"
	"dht/src/storage"
	"dht/src/structure"
	"dht/src/transport"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

func main() {
	// parse input arguments
	arguments := os.Args[1:]
	port, initialMembers, err := validateArgs(arguments)
	if err != nil {
		log.Fatal("Error occurred while validating arguments.", err)
		panic(err)
	}

	// construct channels to communicate between transport layer and application layer
	gmsChannel := make(chan []byte, 2)

	// construct channels for intra-node service communication
	gmsToCoordinatorChannel := make(chan structure.GMSEventMessage)
	transportToCoordinatorChannel := make(chan protobuf.InternalMsg, 2)
	coordinatorToStorageChannel := make(chan protobuf.InternalMsg, 2)
	coordinatorToReplicationChannel := make(chan int)

	// get info about self
	containerHostname := getOutboundIP().String()

	// initialize transport layer
	portInt, _ := strconv.Atoi(port)
	transport, err := transport.New(containerHostname, portInt, gmsChannel, transportToCoordinatorChannel)
	if err != nil {
		log.Fatal("Failed to initialize transport layer.", err)
		panic("Error when creating transport layer. Abort creating server.")
	}
	go transport.Init_server()

	// initialize group membership service
	gms, err := membership.New(initialMembers, transport, gmsChannel, containerHostname, port, gmsToCoordinatorChannel)
	if err != nil {
		log.Fatal("Failed to initialize GMS.", err)
		panic("Error when creating gms. Abort creating server.")
	}

	// initialize storage service
	storage.New(transport, coordinatorToStorageChannel, gms)

	// initialize coordinator service
	_, err = coordinator.New(gmsToCoordinatorChannel, transportToCoordinatorChannel, coordinatorToStorageChannel, coordinatorToReplicationChannel, transport)
	if err != nil {
		log.Fatal("Failed to initialize CoordinatorService", err)
		panic("Error when creating coordinator. Abort node initialization")
	}
	// go func() {
	// 	for {
	// 		time.Sleep(5000 * time.Millisecond)
	// 		fmt.Println(gms.GetAllNodes())
	// 	}
	// }()

	// go func() {
	// 	for {
	// 		notification := <-GMSToCordinator
	// 		fmt.Println(notification)
	// 	}
	// }()

	// block main thread from ending and closing application
	doneChan := make(chan error, 1)
	_ = <-doneChan
}

// validateArgs() extracts the "port" and "initial peers list" from the constructor args.
// - returns error if:
//	- incorrect number of arguments. Expects 2 arguments to be passed: $port $peersfile
//	- $port is not an integer
//	- $port is out-of-bounds (valid if port is in [0,65535])
//	- unable to open file at $peersfile
//	- faied to read $peersfile.
//
// - "initial peers list" ignores invalid entries where an entry is invalid if:
//	- a line is not of the form "ip:port"
//	- "ip" is not a valid IPv4 address
//	- "port" is out-of-bounds.
func validateArgs(args []string) (port string, initialMembers []string, err error) {
	if len(args) != 2 {
		return "", nil, errors.New("Must provide exactly 2 arguments")
	}

	port = args[0]

	// validate port argument
	parsedPort, err := strconv.Atoi(port)
	if err != nil {
		return "", nil, errors.New("Failed to parse port")
	}
	if parsedPort < 0 || parsedPort > 65535 {
		return "", nil, errors.New("Invalid port number")
	}

	peersFilePath := args[1]
	file, err := os.Open(peersFilePath)
	if err != nil {
		return "", nil, errors.New("Unable to open peers file")
	}
	defer file.Close()

	// validate peers file and append to initialMembers list if peer is valid
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		text := scanner.Text()
		splitText := strings.Split(text, ":")
		if len(splitText) != 2 {
			fmt.Errorf("invalid entry in peer file %s. Ignoring this entry as a peer", text)
			continue
		}
		ip, err := validateIP(splitText[0])
		if err != nil {
			fmt.Errorf("Invalid ip in peers file. Peer: %s. Ignoring this entry as a peer", text)
			continue

		}

		port, err := validatePort(splitText[1])
		if err != nil {
			fmt.Errorf("Invalid port in peers file. Peer: %s. Ignoring this entry as a peer", text)
			continue

		}
		initialMembers = append(initialMembers, ip+":"+port)
	}

	if err := scanner.Err(); err != nil {
		return "", nil, errors.New("Failed to read peers file using scanner")
	}

	return port, initialMembers, nil
}

func validateIP(ip string) (string, error) {
	if ip := net.ParseIP(ip); ip == nil {
		return "", errors.New("invalid self-ip address to create new membership service")
	}

	return ip, nil
}

func validatePort(port string) (string, error) {
	parsedPort, err := strconv.Atoi(port)
	if err != nil {
		return "", errors.New("Failed to parse port")
	}
	if parsedPort < 0 || parsedPort > 65535 {
		return "", errors.New("Invalid port number")
	}

	return port, nil
}

// source: https://stackoverflow.com/questions/23558425/how-do-i-get-the-local-ip-address-in-go
func getOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP
}
