package coordinator

import (
	"dht/google_protocol_buffer/pb/protobuf"
	"dht/src/storage"
)
import "dht/src/transport"

func coodinator(sm *storage.StorageModule){ //(storage, replication,gms){

	//router
	var storage_input_chan chan protobuf.InternalMsg
	for{
		select {

		// trans-> cood->replication

		// gms -> cood -> replication
			//sends events
		// gms <- cood <- transport
			// for the hearbeat

		// storage ->| COODINAROTR
		// storage <-|

		// replication func ()= get next node{ called by the
		//									 }
		//
		//... all the chanels hers

		//

		}
	}
}