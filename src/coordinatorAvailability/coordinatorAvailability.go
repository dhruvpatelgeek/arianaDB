package coordinatorAvailability

import (
	"sync"
)


type CoordinatorAvailability struct {
	availible bool
	mu sync.Mutex
}



func New()(*CoordinatorAvailability, error){
	coordinatorAvailability := new(CoordinatorAvailability)
	coordinatorAvailability.availible = true

	return coordinatorAvailability, nil
}

func (coordinatorAvailability *CoordinatorAvailability) CanStartMigrating() bool{
	coordinatorAvailability.mu.Lock()
	if(coordinatorAvailability.availible){
		coordinatorAvailability.availible = false
		coordinatorAvailability.mu.Unlock()
		return true
	}else{
		return false
	}
}

func (coordinatorAvailability *CoordinatorAvailability) FinishedDistributeKeys(){
	coordinatorAvailability.mu.Lock()
	coordinatorAvailability.availible = true
	coordinatorAvailability.mu.Unlock()
}


