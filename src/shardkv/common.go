package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrOutdatedConfig = "ErrOutdatedConfig"
)

type Err string

type ClerkHeader struct {
	ClerkId int64
	SerialNo int64
	ConfigNo int
}

// Put or Append
type PutAppendArgsBody struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendArgs struct {
	Header    ClerkHeader
	Body      PutAppendArgsBody
}

type PutAppendReply struct {
	Err Err
}

type GetArgsBody struct {
	Key string
	// You'll have to add definitions here.
}

type GetArgs struct {
	Header   ClerkHeader
	Body     GetArgsBody
}

type GetReply struct {
	Err   Err
	Value string
}
