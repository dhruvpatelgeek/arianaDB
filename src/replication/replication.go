package replication

import (

	//"log"
	"dht/src/membership"
	"dht/src/structure"
	"math/big"
)

const LESS = -1
const EQUAL = 0
const GREATER = 1

type ReplicationService struct {
	hostIP   string
	hostPort string
	hostIPv4 string

	gms *membership.MembershipService
}

func New(hostIP string, hostPort string, gms *membership.MembershipService) *ReplicationService {
	rs := &ReplicationService{}

	rs.hostIP = hostIP
	rs.hostPort = hostPort
	rs.hostIPv4 = hostIP + ":" + hostPort

	rs.gms = gms

	return rs
}

func (rs *ReplicationService) GetMigrationRange(ipv4 string) (string, string) {
	nodeHashInt := structure.HashKey(ipv4)

	predecessor := rs.findPredecessorFromHash(nodeHashInt)

	predecessorHashInt := structure.HashKey(predecessor)
	migrationStart := big.NewInt(1)
	migrationStart.Add(migrationStart, predecessorHashInt)

	return migrationStart.String(), nodeHashInt.String()
}

func (rs *ReplicationService) IsPredecessor(ipv4 string) bool {

	predecessor := rs.findPredecessorFromHash(structure.HashKey(rs.hostIPv4))
	return ipv4 == predecessor
}

func (rs *ReplicationService) FindPredecessorNode(ipv4 string) string {
	return rs.findPredecessorFromHash(structure.HashKey(ipv4))
}

func (rs *ReplicationService) findPredecessorFromHash(hash *big.Int) string {
	nodeList := rs.gms.GetAllNodes()
	//fmt.Println("[NODE LINST>>]",nodeList)
	//fmt.Println("[HASH]",hash.String())
	var responsibleNode string
	// diff := hashDifference(structure.HashKey(responsibleNode), hash)

	diff := big.NewInt(0)
	maxSlice := make([]byte, 256)
	// Initialize with largest byte value
	for el := range maxSlice {
		maxSlice[el] = 15
	}

	diff.SetBytes(maxSlice)

	// Find node responsible for given key
	for _, currNode := range nodeList {
		currDiff := hashDifference(structure.HashKey(currNode), hash)
		if currDiff.Cmp(diff) == LESS  && currDiff.Cmp(big.NewInt(0)) != EQUAL {
			diff = currDiff
			responsibleNode = currNode
		}
	}

	return responsibleNode
}

// @Description: Determines which node is responsible for the given key
// @param key
// @return string - Location of node responsible for the given key
func (rs *ReplicationService) GetNextNode(key []byte) string {
	// TODO: for the same key, this is giving different results
	if len(key) == 0 {
		return rs.hostIPv4
	}

	keyHashInt := structure.HashKey(string(key))

	return rs.findSuccessorNode(keyHashInt)
}

func (rs *ReplicationService) findSuccessorNode(hash *big.Int) string {
	nodeList := rs.gms.GetAllNodes()

	responsibleNode := nodeList[0]
	diff := hashDifference(hash, structure.HashKey(responsibleNode))

	// Find node responsible for given key
	for _, currNode := range nodeList {
		currDiff := hashDifference(hash, structure.HashKey(currNode))
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


