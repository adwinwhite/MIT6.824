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

// type TaskStatus int

// const (
	// Pending TaskStatus = iota
	// Assigned
	// Finished
// )

type TaskType int

const (
	Undefined TaskType = iota
	Map
	Reduce
)

type RequestTaskArgs struct {
	WorkerID string
}

type RequestTaskReply struct {
	Type TaskType
	InputFile string
	TaskID string
	Err string
}

type SubmitTaskArgs struct {
	WorkerID string
	Type TaskType
	TaskID string
}

type SubmitTaskReply struct {
	Err string
}

type ConfirmTaskArgs struct {
	Type TaskType
	TaskID string
}

type ConfirmTaskReply struct {}




// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
