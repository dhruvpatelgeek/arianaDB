package main

import (
	"bufio"
	"dht/google_protocol_buffer/pb/protobuf"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/golang/protobuf/proto"
	guuid "github.com/google/uuid"
)

//SOFTWARE VERSION-------------------------------------------

//NAME:Dhruv Patel
//Student number: 43586999
// main.go

//----------------------------------------------------------

//DOCUMENTATION-------------------------------------------
//RUN USING
//$ go run src/client/main.go <ip address> <port number> <student number>
// prints the secret key+logs

//----------------------------------------------------

//REFRENCES-------------------------------------------
//https://blog.kowalczyk.info/article/JyRZ/generating-good-unique-ids-in-go.html
//https://ops.tips/blog/udp-client-and-server-in-go/
//https://developers.google.com/protocol-buffers
//https://developers.google.com/protocol-buffers/docs/gotutorial
//stackoverflow
//https://golang.org/doc/

//----------------------------------------------------
/*
	INPUT byte array a,b
	return crc32IEEE
*/
func calculate_checksum(a []byte, b []byte) uint64 {
	var concat_byte_arr = append(a, b...)
	var check_sum = uint64(crc32.ChecksumIEEE(concat_byte_arr))
	return check_sum
}

/*
	return UUID string generated using
                guuid "github.com/google/uuid"
*/
func genUUID() []byte {
	id := guuid.New()
	//fmt.Printf("UUID GENERATED-> %s\n", id.String())
	return []byte(id.String())
}

/*
	INPUT student number
	returns marshalled message payload in from of []bytes
*/
func genrate_request(student_number uint32, key_val string) ([]byte, string) {

	precursor_payload := &protobuf.KVRequest{
		Command: student_number,
		Key:     []byte(key_val),
		Value:   []byte(key_val),
		Version: nil,
	}

	payload, err := proto.Marshal(precursor_payload)
	if err != nil {
		log.Fatalln("Failed to encode address book:", err)
	} else {
		fmt.Printf("payload generated\n")
	}

	messageID := []byte(key_val)
	var concat_byte_arr = append(messageID, payload...)
	var check_sum = uint64(crc32.ChecksumIEEE(concat_byte_arr))
	precursor_message := &protobuf.Msg{
		MessageID: messageID,
		Payload:   payload,
		CheckSum:  check_sum,
	}
	out, err := proto.Marshal(precursor_message)
	if err != nil {
		log.Fatalln("Failed to encode address book:", err)
	} else {
		if debug {
			fmt.Printf("NEW BUFFER-> %+v", out)
		}
	}

	return out, hex.EncodeToString(messageID)
}

/*
	INPUT address,student number

	validates the adresses and issues 1 request +3 retries to the provided address
*/
func client_call(address string, student_number int, key_value string) {
	//var debug_message="\x91\x8b\x01\x00\x00\x01\x00\x00\x00\x00\x00\x00\x07example\x03com\x00\x00\x01\x00\x01"
	//var payload=[]byte(debug_message);

	payload, checksum := genrate_request(uint32(student_number), key_value)
	retry(payload, address, 0, 100, checksum)
	return
}

/*
	INPUT payload, address, iteration counter, timout, message id

	creates a connection to the address and sends the payload

	waits for resposnse
	if timeout -> retry
	if corruption-> retry

	on success
	prints the SECRET_KEY to the console
*/

func is_ip_valid(addr string) bool {
	if net.ParseIP(addr) == nil {
		return false
	} else {
		return true
	}
}

func check_if_port_is_valid(port string) bool {
	casted_port, err := strconv.Atoi(port)
	if err != nil {
		fmt.Printf("CASTING ERROR [port valid]")
	}
	if (casted_port > 65535) || (casted_port < 0) {
		return false
	} else {
		return true
	}
}

func retry(payload []byte, address string, itr int, timeout int64, checksum string) {
	var _payload = payload
	fmt.Printf("RETRYING REQUEST [%d]--------------------------\n", itr)
	conn, err := net.Dial("udp", address)
	if err != nil {
		fmt.Printf("Some error %v", err)
		return
	}

	//writing message->
	fmt.Fprintf(conn, string(_payload))
	fmt.Printf("packet-written")
	//reading message-<
	p := make([]byte, 1024)
	var byte_ctr int
	timeoutDuration := time.Duration(timeout) * time.Millisecond
	err = conn.SetReadDeadline(time.Now().Add(timeoutDuration))
	byte_ctr, err = bufio.NewReader(conn).Read(p)
	if e, ok := err.(net.Error); ok && e.Timeout() {
		fmt.Printf("\nTIMEOUT after %v\n", timeoutDuration)
		conn.Close()

		if itr < 3 {
			retry(payload, address, itr+1, timeout+100, checksum)
		}
		return
	} else if err != nil {
		fmt.Printf("\n[ERROR] %v\n", err)
		conn.Close()
		return
	} else {
		if debug {
			fmt.Printf("packet-received:\n bytes=%d \nfrom=%s\n", byte_ctr, address)
		}
		fmt.Printf("\n>>>>>>>REPLY FORM SERVER[SUCCESS]\n")

		var flag = false
		if flag {
			if itr < 3 {
				fmt.Printf("\nRETRYING\n")
				retry(payload, address, itr+1, timeout+100, checksum)
			}
		} else {
			fmt.Printf("\n*******************************************\n")
			cast_res := &protobuf.KVResponse{
				ErrCode: nil,
				Value:   nil,
				Pid:     nil,
			}

			message_response := &protobuf.Msg{
				MessageID: nil,
				Payload:   nil,
				CheckSum:  0,
			}
			error := proto.Unmarshal(p[:byte_ctr], message_response)
			if error != nil {
				fmt.Printf("\nUNPACK ERROR %+v\n", error)
			}

			error = proto.Unmarshal(message_response.Payload, cast_res)
			if error != nil {
				fmt.Printf("\nUNPACK ERROR %+v\n", error)
			}
			fmt.Printf("\nErrCode %+v\n", cast_res.ErrCode)
			fmt.Printf("value %+v\n", string(cast_res.Value))
			fmt.Printf("PID %+v\n", cast_res.Pid)
			fmt.Printf("\n*******************************************\n")
			fmt.Printf("TERMINATING PROCRESS[SUCCESS]\n")
		}
		conn.Close()
		return
	}
	return
}

//DEBUG FLAG--------------------------------------
var debug = false

//------------------------------------------------
func main() {
	if !debug {
		argsWithProg := os.Args
		if len(argsWithProg) != 5 {
			fmt.Printf("INVALID NUMBER OF ARGUMENTS, EXITTING....\n")
		} else {

			fmt.Printf("address is ->%s\n", argsWithProg[1])
			fmt.Printf("port is ->%s\n", argsWithProg[2])
			fmt.Printf("message number is ->%s\n", argsWithProg[3])
			fmt.Printf("KEY VAL is ->%s\n", argsWithProg[4])
			prod_address := argsWithProg[1] + ":" + argsWithProg[2]
			std_numb, err := strconv.Atoi(argsWithProg[3])
			if err != nil {
				fmt.Printf("STUDENT NUMBER CONVERSION ERROR,EXITTING...\n")
			} else if !is_ip_valid(argsWithProg[1]) {
				fmt.Printf("IP ADDRESS NOT VALID,EXITTING...\n")
			} else if !check_if_port_is_valid(argsWithProg[2]) {
				fmt.Printf("PORT NOT VALID,EXITTING...\n")
			} else {
				fmt.Printf("------------------\n")
				client_call(prod_address, std_numb, argsWithProg[4])
			}
		}
	} else { //debug sequence
		// if DEBUG IS TRUE;
		address := "127.0.0.1:3000"
		client_call(address, 1, "hello")
	}
	fmt.Printf("CLIENT-CLOSED\n")
}
