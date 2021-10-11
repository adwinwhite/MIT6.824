package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type TaskStatus int

const (
	Pending TaskStatus = iota
	Assigned 
	Finished
)

type TaskType int

const (
	Undefined TaskType = iota
	Map
	Reduce
)




type DispatchArgs struct {}

type DispatchReply struct {
	 Type TaskType
}

type GetMapTaskArgs struct {
	WorkerID string
}

type GetMapTaskReply struct {
	Err string
	MapID int
	InputFile string
}

type GetReduceTaskArgs struct {
	WorkerID string
}

type GetReduceTaskReply struct {
	Err string
	ReduceID int
	InputFile string
}

type SummitMapResultArgs struct {
	WorkerID string
	MapID int
}

type SummitMapResultReply struct {
	Err string
	Success bool
}

type ConfirmMapResultArgs struct {
	MapID int
}

type ConfirmMapResultReply struct {}

// Add your RPC definitions here.
type SummitReduceResultArgs struct {
	WorkerID string
	ReduceID int
}

type SummitReduceResultReply struct {
	Err string
	Success bool
}

type ConfirmReduceResultArgs struct {
	ReduceID int
}

type ConfirmReduceResultReply struct {}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
