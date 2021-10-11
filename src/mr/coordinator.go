package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"
import "strconv"



type TaskInfo struct {
	workerID string
	status TaskStatus
}

type Coordinator struct {
	mu sync.Mutex
	mapResultSorted bool
	inputFiles []string
	mapTaskInfo []TaskInfo
	reduceTaskInfo []TaskInfo
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) mapTaskDone() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, info := range c.mapTaskInfo {
		if info.status != Finished {
			return false
		}
	}
	return true
}

func (c *Coordinator) Dispatch(args *DispatchArgs, reply *DispatchReply) { 
	if c.mapTaskDone() {
		reply.Type = Reduce
	} else {
		reply.Type = Map
	}
	return
}

func (c *Coordinator) GetMapTask(args *GetMapTaskArgs, reply *GetMapTaskReply) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, info := range c.mapTaskInfo {
		if info.status == Pending {
			c.mapTaskInfo[i].workerID = args.WorkerID
			c.mapTaskInfo[i].status = Assigned
			reply.Err = ""
			reply.MapID = i
			reply.InputFile = c.inputFiles[i]
			go func() {
				t := time.NewTimer(10 * time.Second)
				<-t.C
				c.mu.Lock()
				defer c.mu.Unlock()
				if c.mapTaskInfo[i].status != Finished {
					c.mapTaskInfo[i].status = Pending
					c.mapTaskInfo[i].workerID = ""
				}
				return
			}()

			return
		}
	}
	reply.Err = "no map task left"
	return 
}

func (c *Coordinator) checkMapStatusAndSort() error {
	if c.mapResultSorted {
		return nil
	}
	if c.mapTaskDone() {

	}
	return nil
}



func (c *Coordinator) SummitMapResult(args *SummitMapResultArgs, reply *SummitMapResultReply) {
	c.mu.Lock()
	defer c.mu.Unlock()
	info := c.mapTaskInfo[args.MapID]
	if info.workerID == args.WorkerID && info.status == Assigned {
		reply.Err = ""
		reply.Success = true
		return
	}
	reply.Err = "worker id changed or status isn't assigned"
	reply.Success = false
	return
}

func (c *Coordinator) ConfirmMapResultArgs(args *ConfirmMapResultArgs, reply *ConfirmMapResultReply) {
	c.mu.Lock()
	c.mapTaskInfo[args.MapID].status = Finished
	c.mu.Unlock()

	c.checkMapStatusAndSort()
}


func (c *Coordinator) SummitReduceResult(args *SummitReduceResultArgs, reply *SummitReduceResultReply) {
	c.mu.Lock()
	defer c.mu.Unlock()
	info := c.reduceTaskInfo[args.ReduceID]
	if info.workerID == args.WorkerID && info.status == Assigned {
		reply.Err = ""
		reply.Success = true
		return
	}
	reply.Err = "worker id changed or status isn't assigned"
	reply.Success = false
	return
}

func (c *Coordinator) ConfirmReduceResultArgs(args *ConfirmReduceResultArgs, reply *ConfirmReduceResultReply) {
	c.mu.Lock()
	c.reduceTaskInfo[args.ReduceID].status = Finished
	c.mu.Unlock()
}



func (c *Coordinator) GetReduceTask(args *GetReduceTaskArgs, reply *GetReduceTaskReply) {
	if !c.mapTaskDone() {
		reply.Err = "map tasks haven't been finished"
		return
	}
	if !c.mapResultSorted {
		reply.Err = "map results haven't been sorted"
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, info := range c.reduceTaskInfo {
		if info.status == Pending {
			c.reduceTaskInfo[i].workerID = args.WorkerID
			c.reduceTaskInfo[i].status = Assigned
			reply.Err = ""
			reply.ReduceID = i
			reply.InputFile = "mr-inter-" + strconv.Itoa(i)
			go func() {
				t := time.NewTimer(10 * time.Second)
				<-t.C
				c.mu.Lock()
				defer c.mu.Unlock()
				if c.reduceTaskInfo[i].status != Finished {
					c.reduceTaskInfo[i].status = Pending
					c.reduceTaskInfo[i].workerID = ""
				}
				return
			}()

			return
		}
	}
	reply.Err = "no reduce task left"
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
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, info := range c.reduceTaskInfo {
		if info.status != Finished {
			return false
		}
	}

	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Initiate tasks status
	c.mapResultSorted = false
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
