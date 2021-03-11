package structure

// isJoined is true if new node joined/ false => failed
type GMSEventMessage struct {
	IsJoined bool
	Node     string
}

// TODO: come up with other message types too
