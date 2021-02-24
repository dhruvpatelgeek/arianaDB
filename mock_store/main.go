package main

import ( 
	"log"
	"os"
	"strconv"
	"dht/mock_store/transport"
)


func main() {

	var GroupSend = make(chan transport.Message)

	log.Println("MAIN 122")
	argsWithProg := os.Args
	if len(argsWithProg) != 2 {
		log.Printf("INVALID NUMBER OF ARGUMENTS, EXITTING....\n")
	} else {
		port, _ := strconv.Atoi(argsWithProg[1])
		tm, _ := transport.New(port, GroupSend)

		tm.Init_server()
	}

}