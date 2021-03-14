package constants

type InternalMessageCommands uint32

const (
	ProcessClientKVRequest InternalMessageCommands = iota + 1 // if this is a client request sent directly
	ProcessPropagatedKVRequest
	ProcessKeyMigrationRequest
	ProcessTableMigrationRequest
	InsertMigratedKey
)

type TableSelection uint32

const (
	Head TableSelection = iota + 1 // if this is a client request sent directly
	Middle
	Tail
)
