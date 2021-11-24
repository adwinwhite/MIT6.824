package shardctrler

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (c *Config) Deepcopy() Config {
	newConfig := Config{}
	newConfig.Num = c.Num
	newConfig.Shards = c.Shards
	groups := make(map[int][]string)
	for k, v := range c.Groups {
		groups[k] = append([]string(nil), v...)
	}
	newConfig.Groups = groups
	return newConfig
}

const (
	OK = "OK"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

type ClerkHeader struct {
	ClerkId int64
	SerialNo int64
}

type JoinArgsBody struct {
	Servers map[int][]string // new GID -> servers mappings
}

type JoinArgs struct {
	Header  ClerkHeader
	Body    JoinArgsBody
}

type JoinReply struct {
	// WrongLeader bool
	Err         Err
}

type LeaveArgsBody struct {
	GIDs []int
}

type LeaveArgs struct {
	Header  ClerkHeader
	Body    LeaveArgsBody
}

type LeaveReply struct {
	// WrongLeader bool
	Err         Err
}

type MoveArgsBody struct {
	Shard int
	GID   int
}

type MoveArgs struct {
	Header  ClerkHeader
	Body    MoveArgsBody
}

type MoveReply struct {
	// WrongLeader bool
	Err         Err
}

type QueryArgsBody struct {
	Num int // desired config number
}

type QueryArgs struct {
	Header  ClerkHeader
	Body    QueryArgsBody
}

type QueryReply struct {
	// WrongLeader bool
	Err         Err
	Config      Config
}
