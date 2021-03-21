package membership

// Number of nodes to gossip during every heartbeat Period
const FANOUT uint32 = 18

// SendJoinCommand is a command sent by new joins node
const SendJoinCommand uint32 = 12

// HeartbeatGossipCommand is a command for heartbeat protocol
const HeartbeatGossipCommand uint32 = 13

// TimeHeartbeat is time period for Heartbeat
const HeartbeatPeriod = 150

// TimeFail is time period for failCheck Check
const FailCheckPeriod = 19 * 1000

// TimeCleanup is time period for Clean up failChecked node
const CleanupPeriod = 20 * 1000

type GMSEventType uint32

const (
	Joined GMSEventType = iota + 1 // if this is a client request sent directly
	Failed
)

type GMSEventMessage struct {
	EventType GMSEventType
	Nodes     []string
}
