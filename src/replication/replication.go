package replication

import (
	"crypto/sha256"
	"encoding/hex"
	//"log"
	"math/big"
	"sync"

	"dht/src/transport"
)

const LESS = -1
const EQUAL = 0
const GREATER = 1

// ******************
// JUST FOR DEBUGGING
type coord2rep struct {
	status int
	node string
}
// ******************


type ReplicationService struct {
	chains map[string]int
	chainsLock *sync.Mutex
}

func New(gmsEvents <-chan coord2rep) (*ReplicationService) {
	rs := &ReplicationService{chains: make(map[string]int), chainsLock: &sync.Mutex{}}
	go rs.runService(gmsEvents)

	return rs
}

func (rs *ReplicationService) runService(gmsEvents <-chan coord2rep) {
	for {
		event := <-gmsEvents
		if event.status == 0 {
			rs.addHead(event.node)
		} else {
			rs.removeHead(event.node)
		}
	}
}

func (rs *ReplicationService) addHead(node string) {
	rs.chainsLock.Lock()
	_, alreadyExists := rs.chains[node]
	if !alreadyExists {
		rs.chains[node] = 1
	}
	rs.chainsLock.Unlock()
}

func (rs *ReplicationService) removeHead(node string) {
	rs.chainsLock.Lock()
	delete(rs.chains, node)
	rs.chainsLock.Unlock()
}

func (rs *ReplicationService) getAllChains() []string {
	allChains := []string{}

	rs.chainsLock.Lock()
	for key, _ := range rs.chains {
		allChains = append(allChains, key)
	}
	rs.chainsLock.Unlock()

	return allChains
}

// @Description: Determines which node is responsible for the given key
// @param key
// @return string - Location of node responsible for the given key
func (rs *ReplicationService) GetNextNode(key []byte) string {
	if len(key) == 0 {
		return transport.GetLocalAddr()
	}

	keyHashInt := hashInt(hex.EncodeToString(key))

	nodeList := rs.getAllChains()

	responsibleNode := nodeList[0]
	diff := hashDifference(keyHashInt, hashInt(responsibleNode))

	// Find node responsible for given key
	for _, currNode := range nodeList {
		currDiff := hashDifference(keyHashInt, hashInt(currNode))
		if currDiff.Cmp(diff) == LESS {
			diff = currDiff
			responsibleNode = currNode
		}
	}

	return responsibleNode
}

// @Description: Computes the numerical difference between
// the key's hash and the node's hash
// @param key 
// @param node
// @return *big.Int
func hashDifference(key *big.Int, node *big.Int) *big.Int {
	diff := big.NewInt(0)
	max := big.NewInt(0)
	maxSlice := make([]byte, 256)
	// Initialize with largest byte value
	for el := range maxSlice {
		maxSlice[el] = 15
	}

	max.SetBytes(maxSlice)

	keyCmp := key.Cmp(node)

	if keyCmp == LESS {
		return diff.Sub(node, key)
	} else if keyCmp == EQUAL {
		return diff.SetInt64(0)
	} else {
		return diff.Add(diff.Sub(max, key), node)
	}
}

// @Description: returns the key's hash value as a big int
// @param key 
// @return *big.Int - Hash digest
func hashInt(key string) *big.Int {
	keyHash := hash(key)
	keyHashInt := big.NewInt(0)
	return keyHashInt.SetBytes(keyHash)
}

// @Description: returns the input string's sha256 digest
// @param str 
// @return []byte - SHA256 digest
func hash(str string) []byte {
	digest := sha256.Sum256([]byte(str))
	return digest[:]
}