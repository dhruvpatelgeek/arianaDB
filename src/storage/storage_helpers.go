package storage

import (
	"dht/google_protocol_buffer/pb/protobuf"
	"dht/src/constants"
	"errors"

	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
)

// TODO: maybe put all the common non-class functions in here?
func createInsertMigratedKeyRequest(key string, value []byte, destinationTable constants.TableSelection) (*protobuf.InternalMsg, error) {
	kvRequest := &protobuf.KVRequest{
		Command: PUT,
		Key:     []byte(key),
		Value:   value,
	}
	serializedKVRequest, err := proto.Marshal(kvRequest)
	if err != nil {
		return nil, errors.New("[Storage] failed to marshal KVRequest during migration. Ignoring this key")
	}
	optionalDestinationTable := uint32(destinationTable) // note: optional protobuf args require pointers to uint32

	internalMessage := &protobuf.InternalMsg{
		MessageID:            []byte(uuid.New().String()),
		Command:              uint32(constants.ProcessStorageToStorageKVRequest),
		KVRequest:            serializedKVRequest,
		DestinationNodeTable: &optionalDestinationTable,
	}
	return internalMessage, nil
}

func createWipeoutDestinationTableKVRequest(destination string, destinationTable constants.TableSelection) (*protobuf.InternalMsg, error) {
	kvRequest := &protobuf.KVRequest{
		Command: WIPEOUT,
	}
	serializedKVRequest, err := proto.Marshal(kvRequest)
	if err != nil {
		return nil, errors.New("[Storage] Failed to serialize wipeout KVRequest.")
	}
	optionalDestinationTable := uint32(destinationTable) // note: optional protobuf args require pointers to uint32

	internalMessage := &protobuf.InternalMsg{
		MessageID:            []byte(uuid.New().String()),
		Command:              uint32(constants.ProcessStorageToStorageKVRequest),
		KVRequest:            serializedKVRequest,
		DestinationNodeTable: &optionalDestinationTable,
	}
	return internalMessage, nil
}
