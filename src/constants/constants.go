package constants

type InternalMessageCommands uint32

const (
	ProcessKVRequest InternalMessageCommands = iota + 1
	ProcessKeyMigrationRequest
	ProcessTableMigrationRequest
	InsertMigratedKey
)
