package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"
import "strconv"

type TaskStage int

const (
	Mapping TaskStage = iota
	Sorting
	Reducing
	Completed
)

type TaskInfo struct {
	workerID  string
	taskType  TaskType
	taskID    string
	inputFile string
}

// I need a database. Considering no performance here
type Coordinator struct {
	stage          TaskStage
	pendingTasks   []TaskInfo
	assignedTasks  []TaskInfo
	finishedTasks  []TaskInfo
	taskStatusLock sync.Mutex
	inputFiles     []string
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) {
	switch c.stage {
	case Mapping:
		c.getMapTask(args, reply)
	case Sorting:
		reply.Err = "Sorting"
	case Reducing:
		c.getReduceTask(args, reply)
	case Completed:
		reply.Err = "Completed"
	}
}

func (c *Coordinator) SubmitTask(args *SubmitTaskArgs, reply *SubmitTaskReply) {
	switch c.stage {
	case Mapping:
		c.submitMapTask(args, reply)
	case Sorting:
		reply.Err = "Sorting"
	case Reducing:
		c.submitReduceTask(args, reply)
	case Completed:
		reply.Err = "Completed"
	}
}

func (c *Coordinator) ConfirmTask(args *ConfirmTaskArgs, reply *ConfirmTaskReply) {
	switch c.stage {
	case Mapping:
		c.confirmMapTask(args, reply)
	case Reducing:
		c.confirmReduceTask(args, reply)
	}
}

func removeTask(s []TaskInfo, i int) []TaskInfo {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}


func (c *Coordinator) reassignTask(info TaskInfo) {
	timer := time.NewTimer(10 * time.Second)
	<-timer.C
	c.taskStatusLock.Lock()
	defer c.taskStatusLock.Unlock()
	for i, t := range c.assignedTasks {
		if t.taskType == info.taskType && t.taskID == info.taskID {
			info.workerID = ""
			c.pendingTasks = append(c.pendingTasks, info)
			c.assignedTasks = removeTask(c.assignedTasks, i)
			return
		}
	}
}



func (c *Coordinator) getMapTask(args *RequestTaskArgs, reply *RequestTaskReply) {
	c.taskStatusLock.Lock()
	defer c.taskStatusLock.Unlock()
	for i, info := range c.pendingTasks {
		if info.taskType == Map {
			reply.Type = Map
			reply.InputFile = info.inputFile
			reply.TaskID = info.taskID
			reply.Err = ""
			info.workerID = args.WorkerID
			c.assignedTasks = append(c.assignedTasks, info)
			c.pendingTasks = removeTask(c.pendingTasks, i)

			go c.reassignTask(info)
				
			return
		}
	}
	reply.Err = "NoMapTaskLeft"
	return
}

func (c *Coordinator) getReduceTask(args *RequestTaskArgs, reply *RequestTaskReply) {
	c.taskStatusLock.Lock()
	defer c.taskStatusLock.Unlock()
	for i, info := range c.pendingTasks {
		if info.taskType == Reduce {
			reply.Type = Reduce
			reply.InputFile = info.inputFile
			reply.TaskID = info.taskID
			reply.Err = ""
			info.workerID = args.WorkerID
			c.assignedTasks = append(c.assignedTasks, info)
			c.pendingTasks = removeTask(c.pendingTasks, i)

			go c.reassignTask(info)

			return
		}
	}
	reply.Err = "NoReduceTaskLeft"
	return
}

func (c *Coordinator) submitMapTask(args *SubmitTaskArgs, reply *SubmitTaskReply) {
	if args.Type != Map {
		reply.Err = "TaskTypeError"
		return
	}
	c.taskStatusLock.Lock()
	defer c.taskStatusLock.Unlock()
	for _, info := range c.assignedTasks {
		if info.taskID == args.TaskID {
			if info.workerID == args.WorkerID {
				reply.Err = ""
				return
			} else {
				reply.Err = "WrongWorkerID"
				return
			}
		}
	}
	reply.Err = "NoSuchAssignedTaskID"
	return
}

func (c *Coordinator) submitReduceTask(args *SubmitTaskArgs, reply *SubmitTaskReply) {
	if args.Type != Reduce {
		reply.Err = "TaskTypeError"
		return
	}
	c.taskStatusLock.Lock()
	defer c.taskStatusLock.Unlock()
	for _, info := range c.assignedTasks {
		if info.taskID == args.TaskID {
			if info.workerID == args.WorkerID {
				reply.Err = ""
				return
			} else {
				reply.Err = "WrongWorkerID"
				return
			}
		}
	}
	reply.Err = "NoSuchAssignedTaskID"
	return
}

func (c *Coordinator) confirmMapTask(args *ConfirmTaskArgs, reply *ConfirmTaskReply) {
	if args.Type != Map {
		return
	}
	c.taskStatusLock.Lock()
	for i, info := range c.assignedTasks {
		if info.taskType == Map && info.taskID == args.TaskID {
			c.finishedTasks = append(c.finishedTasks, info)
			c.assignedTasks = removeTask(c.assignedTasks, i)
			c.taskStatusLock.Unlock()

			c.updateTaskStage()
			if c.stage == Sorting {
				c.sortMapResults()
			}

			return
		}
	}
}

func (c *Coordinator) confirmReduceTask(args *ConfirmTaskArgs, reply *ConfirmTaskReply) {
	if args.Type != Reduce {
		return
	}
	c.taskStatusLock.Lock()
	for i, info := range c.assignedTasks {
		if info.taskType == Reduce && info.taskID == args.TaskID {
			c.finishedTasks = append(c.finishedTasks, info)
			c.assignedTasks = removeTask(c.assignedTasks, i)
			c.taskStatusLock.Unlock()

			c.updateTaskStage()
			return
		}
	}
}

func (c *Coordinator) sortMapResults() error {
	return nil
}

func (c *Coordinator) updateTaskStage() {
	c.taskStatusLock.Lock()
	defer c.taskStatusLock.Unlock()
	switch c.stage {
	case Mapping:
		for _, info := range c.pendingTasks {
			if info.taskType == Map {
				return
			}
		}
		for _, info := range c.assignedTasks {
			if info.taskType == Map {
				return
			}
		}
		c.stage = Sorting
	case Reducing:
		for _, info := range c.pendingTasks {
			if info.taskType == Reduce {
				return
			}
		}
		for _, info := range c.assignedTasks {
			if info.taskType == Reduce {
				return
			}
		}
		c.stage = Completed
	}
	return
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	return c.stage == Completed
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Initiate tasks status
	c.inputFiles = files
	for _, _ = range files {
		c.mapTaskInfo = append(c.mapTaskInfo, TaskInfo{"", Pending})
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTaskInfo = append(c.reduceTaskInfo, TaskInfo{"", Pending})
	}

	c.server()
	return &c
}
