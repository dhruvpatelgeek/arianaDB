package structure

import (
	"crypto/sha256"
	"math/big"

	"github.com/google/uuid"
)

// generate messageID
func GenerateMessageID() []byte {
	id := uuid.New().String()

	return []byte("gossip" + id)
}


// @Description: returns the key's hash value as a big int
// @param key
// @return *big.Int - Hash digest
func HashKey(key string) *big.Int {
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