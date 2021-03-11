package storage

import (
	"testing"
	"reflect"

	"dht/google_protocol_buffer/pb/protobuf"
	"dht/src/transport"
	"github.com/golang/protobuf/proto"
)

func correct(kvr protobuf.KVResponse, errCode uint32, value []byte, pid int32) (bool) {

	return (errCode == kvr.GetErrCode() &&
			reflect.DeepEqual(value, kvr.GetValue()) &&
			pid == kvr.GetPid()) 
}																			 

func TestReqRes(t *testing.T) {
	var tests = []struct {
		name string

		cmd uint32 
		key []byte
		value_in []byte 
		version_in int32

		errCode uint32 
		value_out []byte
		pid int32 
	} {
		{name: "Put 2 -> [1, 3]",	
		cmd: 1, key: []byte{2}, value_in: []byte{1, 3}, version_in: 2,
		errCode: 0},

		{name: "Put [2 4 6 8] -> [1, 5, 7]",	
		cmd: 1, key: []byte{2, 4, 6, 8}, value_in: []byte{1, 5, 7}, version_in: 0,
		errCode: 0},

		{name: "Put nil value",	
		cmd: 1, key: []byte{2, 4}, version_in: 0,
		errCode: 0},

		{name: "Put nil key",	
		cmd: 1, value_in: []byte{1, 3}, version_in: 2,
		errCode: 6},

		{name: "Get nil key",	
		cmd: 2,
		errCode: 6},

		{name: "Get [2]",	
		cmd: 2, key: []byte{2},
		errCode: 0, value_out: []byte{1, 3}},

		{name: "Put 2 -> [6, 7, 9]",	
		cmd: 1, key: []byte{2}, value_in: []byte{6, 7, 9}, version_in: 3,
		errCode: 0},

		{name: "Get [2]",	
		cmd: 2, key: []byte{2},
		errCode: 0, value_out: []byte{6, 7, 9}},

		{name: "Remove [2]",	
		cmd: 3, key: []byte{2},
		errCode: 0},

		{name: "Get [2] - Invalid",	
		cmd: 2, key: []byte{2},
		errCode: 1},

		{name: "Get [2, 4, 6, 8]",	
		cmd: 2, key: []byte{2, 4, 6, 8},
		errCode: 0, value_out: []byte{1, 5, 7}},

		{name: "Remove [2, 4, 6, 8]",	
		cmd: 3, key: []byte{2, 4, 6, 8},
		errCode: 0},

		{name: "Remove [2, 4, 6, 8] invalid",	
		cmd: 3, key: []byte{2, 4, 6, 8},
		errCode: 1},

		{name: "wipeout empty",	
		cmd: 5,
		errCode: 0},

		{name: "Put [5 7] -> [1, 3]",	
		cmd: 1, key: []byte{5, 7}, value_in: []byte{1, 3}, version_in: 2,
		errCode: 0},

		{name: "Put [4 9 8] -> [1, 3, 7, 8]",	
		cmd: 1, key: []byte{4, 9, 8}, value_in: []byte{1, 3, 7, 8}, version_in: 2,
		errCode: 0},

		{name: "wipeout",	
		cmd: 5,
		errCode: 0},

		{name: "Get [5 7] - Invalid",	
		cmd: 2, key: []byte{5, 7},
		errCode: 1},

		{name: "Get [4 9 8] - Invalid",	
		cmd: 2, key: []byte{4, 9, 8},
		errCode: 1},
	}

	cm := make(chan protobuf.InternalMsg)
	tm := &transport.TransportModule{}
	sm := New(tm, cm)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			payloadStruct := &protobuf.KVRequest{Command: test.cmd, Key: test.key, Value: test.value_in, Version: &test.version_in}
			
			req, _ := proto.Marshal(payloadStruct)
			serialResponse := sm.message_handler(req)
			response := &protobuf.KVResponse{}
			_ = proto.Unmarshal(serialResponse, response)
			if !correct(*response, test.errCode, test.value_out, test.pid) {
				t.Errorf("Expected %d, %X, %d\nGot %d, %X, %d",
						test.errCode, test.value_out, test.pid,
						response.GetErrCode(), response.GetValue(), response.GetPid())
			}
		})
	}
}