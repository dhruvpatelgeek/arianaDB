package main

import (
	"dht/mock_store/transport"
	"log"
	"os"
)


func main() {
	log.Println("MAIN 122")
	argsWithProg := os.Args
	if len(argsWithProg) != 2 {
		log.Printf("INVALID NUMBER OF ARGUMENTS, EXITTING....\n")
	} else {
		transport.Init_server(argsWithProg[1])
	}

}

