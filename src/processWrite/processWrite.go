package processWrite

import (
	"dht/google_protocol_buffer/pb/protobuf"
	"dht/src/constants"
	"errors"
	"log"
	"sync"
)

type ProcessWrite struct {
	headKVStoreLog       sync.Map
	middleKVStoreLog     sync.Map
	tailKVStoreLog       sync.Map
	headKVStorePayload   sync.Map
	middleKVStorePayload sync.Map
	tailKVStorePayload   sync.Map
}

func New() *ProcessWrite {
	processWriteModule := &ProcessWrite{}
	return processWriteModule
}

func (this *ProcessWrite) Log(internalMessage protobuf.InternalMsg, numberOfcommitCalls int32) {
	messageID := internalMessage.MessageID
	payload := internalMessage
	switch whichTable := internalMessage.DestinationNodeTable; *whichTable {
	case uint32(constants.Head):
		//log.Println(" recived logging request at  ", *whichTable, "with value ", numberOfcommitCalls)
		createLog(&this.headKVStoreLog, messageID, numberOfcommitCalls)
		createCommit(&this.headKVStorePayload, messageID, payload)
	case uint32(constants.Middle):
		//log.Println(" recived logging request at  ", *whichTable, "with value ", numberOfcommitCalls)
		createLog(&this.middleKVStoreLog, messageID, numberOfcommitCalls)
		createCommit(&this.middleKVStorePayload, messageID, payload)
	case uint32(constants.Tail):
		//log.Println(" recived logging request at  ", *whichTable, "with value ", numberOfcommitCalls)
		createLog(&this.tailKVStoreLog, messageID, numberOfcommitCalls)
		createCommit(&this.tailKVStorePayload, messageID, payload)
	default:
		log.Println("[LOGIC ERROR] incorrect routing r89h34h >>", *whichTable, "with value ", numberOfcommitCalls)
	}

}

func (this *ProcessWrite) Commit(internalMessage protobuf.InternalMsg) (protobuf.InternalMsg, bool) {
	messageID := internalMessage.MessageID
	switch whichTable := internalMessage.DestinationNodeTable; *whichTable {
	case uint32(constants.Head):
		//log.Println(" recived commit request at  ", *whichTable)
		shouldCommit, err := signalLogTableSemaphore(&this.headKVStoreLog, messageID)
		if err == nil {
			if shouldCommit {
				return_proto_buf, commit_error := this.commitTransaction(&this.headKVStorePayload, messageID)
				if commit_error == nil {
					return return_proto_buf, true
				} else {
					return return_proto_buf, false
				}
			}
		} else {
			log.Println("[COMMIT ERROR] r4h398 >>", err)
		}

	case uint32(constants.Middle):
		//log.Println(" recived commit request at  ", *whichTable)
		shouldCommit, err := signalLogTableSemaphore(&this.middleKVStoreLog, messageID)
		if err == nil {
			if shouldCommit {
				return_proto_buf, commit_error := this.commitTransaction(&this.middleKVStorePayload, messageID)
				if commit_error == nil {
					return return_proto_buf, true
				} else {
					log.Println("[WRITER ERROR] r49h9 >>>>", commit_error)
					return return_proto_buf, false
				}
			}
		} else {
			log.Println("[COMMIT ERROR] 9483hrh >>", err)
		}
	case uint32(constants.Tail):
		//log.Println(" recived commit request at  ", *whichTable)
		shouldCommit, err := signalLogTableSemaphore(&this.tailKVStoreLog, messageID)
		if err == nil {
			if shouldCommit {
				return_proto_buf, commit_error := this.commitTransaction(&this.tailKVStorePayload, messageID)
				if commit_error == nil {
					return return_proto_buf, true
				} else {
					return return_proto_buf, false
				}
			}
		} else {
			log.Println("[COMMIT ERROR] 8rh349 >>", err)
		}
	default:
		log.Println("[LOGIC ERROR] 894j incorrect routing >>", *whichTable, "with value ")

		return internalMessage, false
	}
	return internalMessage, false
}

func createLog(table *sync.Map, messageID []byte, numberOfcommitCallsTillCommit int32) {
	table.Store(string(messageID), numberOfcommitCallsTillCommit)
}

func createCommit(table *sync.Map, messageID []byte, payload protobuf.InternalMsg) {
	table.Store(string(messageID), payload)
}

func signalLogTableSemaphore(table *sync.Map, messageID []byte) (bool, error) {
	value, found := table.Load(string(messageID))
	if found {
		//fmt.Println(">>>>>>>>>>>>>>>>>FOUND>>>>>> 12")
		semaphore := value.(int32)
		if semaphore == 2 {
			semaphore = semaphore - 1
			table.Store(string(messageID), semaphore)
			return false, nil
		} else if semaphore == 1 {
			table.Delete(string(messageID))
			return true, nil
		} else {
			log.Println("LOG VALUE IS NOT CORRECT,", semaphore)
			err1 := errors.New("[WRONG LOGIC] 8hr43 the log table value is not correct")
			return false, err1
		}
	} else {
		//fmt.Println("<<<<<<<<<<<<<<<<<<NOT FOUND>>>>>> 12")
		err1 := errors.New("[WRONG LOGIC] r43hr89 the log table value does not exist")
		return false, err1
	}
}

func (this *ProcessWrite) commitTransaction(table *sync.Map, messageID []byte) (protobuf.InternalMsg, error) {
	value, found := table.LoadAndDelete(string(messageID))
	if found {
		payload := value.(protobuf.InternalMsg)
		//log.Println("[COMMIT SUCCESS] 87hf")
		return payload, nil
	} else {
		error_not_found := errors.New("[WRONG LOGIC] r4398 no commit found for key")
		nil_proto := protobuf.InternalMsg{}
		return nil_proto, error_not_found
	}

}
