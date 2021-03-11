package replication

import (
	"testing"
	"time"
	"strconv"
	"encoding/hex"
)

func correct(actual []string, expected []string) bool {
	if len(actual) != len(expected) {
		return false
	} 

	if len(actual) == 0 {
		return true
	}

	for i := 0; i < len(expected); i++ {
		found := false
		for j := 0; j < len(actual); j++ {
			if actual[j] == expected[i] {
				found = true
			}
		}

		if found {
			return true
		}
	}

	return false
}

func TestMemberList(t *testing.T) {
	events := []struct {
		status int
		node string
		expected []string
	}{
		{status: 0, node:"0.0.0.0:0", expected: []string{"0.0.0.0:0"}},
		{status: 0, node:"0.0.0.0:0", expected: []string{"0.0.0.0:0"}},
		{status: 1, node:"0.0.0.0:0", expected: []string{}},
		{status: 0, node:"1.1.1.1:0", expected: []string{"1.1.1.1:0"}},
		{status: 0, node:"7.7.7.7:7", expected: []string{"1.1.1.1:0", "7.7.7.7:7"}},
		{status: 0, node:"1.1.1.1:1", expected: []string{"1.1.1.1:0", "7.7.7.7:7", "1.1.1.1:1"}},
		{status: 1, node:"1.1.1.1:0", expected: []string{"1.1.1.1:1", "7.7.7.7:7"}},
		{status: 1, node:"1.1.1.1:1", expected: []string{"7.7.7.7:7"}},
	}

	gmsEvents := make(chan coord2rep)
	replicationService := New(gmsEvents)
	for _, test := range events {
		name := "Status " + strconv.Itoa(test.status) + ", node: " + test.node
		t.Run(name, func(t *testing.T) {
			gmsEvents <- coord2rep{status: test.status, node: test.node}
			time.Sleep(time.Millisecond)
			if !correct(replicationService.getAllChains(), test.expected) {
				t.Error("Expected: ", test.expected, ", but got: ", replicationService.getAllChains())
			}
		})
	}
}

func TestRouting(t *testing.T) {
	events := []struct {
		status int
		node string
	}{
		{status: 0, node:"0.0.0.0:0"},
		{status: 0, node:"1.1.1.1:9"},
		{status: 0, node:"7.7.7.7:7"},
		{status: 0, node:"1.1.1.1:1"},
	}

	gmsEvents := make(chan coord2rep)
	replicationService := New(gmsEvents)

	for _, event := range events {
		gmsEvents <- coord2rep{status: event.status, node: event.node}
		time.Sleep(time.Millisecond)
	}


	keys := []struct {
		key []byte
		expected string
	} {
		{key: []byte{0}, expected: "0.0.0.0:0"},
		{key: []byte{1}, expected: "7.7.7.7:7"},
		{key: []byte{2}, expected: "7.7.7.7:7"},
		{key: []byte{3}, expected: "1.1.1.1:1"},
		{key: []byte{1, 2, 4}, expected: "0.0.0.0:0"},
		{key: []byte{9, 8, 7, 12, 6}, expected: "1.1.1.1:9"},
		{key: []byte{15}, expected: "0.0.0.0:0"},
		{key: []byte{14}, expected: "1.1.1.1:9"},
		{key: []byte{13, 1, 5}, expected: "1.1.1.1:1"},
	}

	for _, key := range keys {
		name := "ROUTE " + hex.EncodeToString(key.key)
		t.Run(name, func(t *testing.T) {
			expected := []string{"0.0.0.0:0", "1.1.1.1:9", "7.7.7.7:7", "1.1.1.1:1"}
			if !correct(replicationService.getAllChains(), expected) {
				t.Error("[Incorrect initialization] - Expected: ", expected, ", but got: ", replicationService.getAllChains())
			}

			nextNode := replicationService.GetNextNode(key.key)
			if key.expected != nextNode {
				t.Error("Expected: ", key.expected, ", but got: ", nextNode)
			}
		})
	}
}