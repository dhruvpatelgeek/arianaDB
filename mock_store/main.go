package main

import ( 
	"log"
	"os"
	"dht/mock_store/transport"
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