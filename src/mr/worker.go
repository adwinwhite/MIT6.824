package mr

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	log "github.com/sirupsen/logrus"
	"net/rpc"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type KeyValueList struct {
	Key string
	Value []string
}


//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	workerID := uuid.NewString()

	// Your worker implementation here.
	for {
		reqArgs := RequestTaskArgs{WorkerID: workerID}
		reqReply, err := RequestTask(&reqArgs)
		if err != nil {
			log.Error(err)
			continue
		}
		switch reqReply.Err {
		case "Completed":
			log.Info("All tasks completed")
			break
		case "NoMapTaskLeft", "NoReduceTaskLeft", "NoSuchAssignedTaskID", "Sorting":
			time.Sleep(100 * time.Millisecond)
			continue
		}
		switch reqReply.Type {
		case Map:
			log.Info("New map task", reqReply.TaskID)
			err := execMapTask(workerID, mapf, reqReply)
			if err != nil {
				log.Error(err)
			}
		case Reduce:
			log.Info("New reduce task", reqReply.TaskID)
			err := execReduceTask(workerID, reducef, reqReply)
			if err != nil {
				log.Error(err)
			}
		}
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//

func execMapTask(workerID string, mapf func(string, string) []KeyValue, reply *RequestTaskReply) error {
	dat, err := os.ReadFile(reply.InputFile)
	if err != nil {
		return err
	}
	var encoders []*json.Encoder
	var tempFiles []string
	var writers []*bufio.Writer
	for i := 0; i < reply.NReduce; i++ {
		// f, err := os.OpenFile("mr-" + reply.TaskID + "-" + strconv.Itoa(i), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		// f, err := os.CreateTemp("", "temp-mr-" + reply.TaskID + "-" + strconv.Itoa(i))
		tempName := fmt.Sprint(time.Now().UnixNano())
		err := os.MkdirAll("./tmpfiles", 0755)
		if err != nil {
			return err
		}
		f, err := os.Create("./tmpfiles/" + tempName)
		tempFiles = append(tempFiles, tempName)
		defer f.Close()
		if err != nil {
			return err
		}
		writer := bufio.NewWriter(f)
		writers = append(writers, writer)
		encoder := json.NewEncoder(writer)
		encoders = append(encoders, encoder)
	}
	kvs := mapf(reply.InputFile, string(dat))
	for _, kv := range kvs {
		reduceInd := ihash(kv.Key) % reply.NReduce
		err = encoders[reduceInd].Encode(&kv)
		if err != nil {
			return err
		}
	}
	submitReply, err := SubmitTask(&SubmitTaskArgs{WorkerID: workerID, Type: Map, TaskID: reply.TaskID})
	log.Info("Submitted map task", reply.TaskID)
	if err != nil {
		return err
	}
	if submitReply.Err != "" {
		return errors.New(submitReply.Err)
	}
	for i := 0; i < reply.NReduce; i++ {
		err = writers[i].Flush()
		if err != nil {
			return err
		}
		err = os.Rename("./tmpfiles/" + tempFiles[i], "./tmpfiles/mr-" + reply.TaskID + "-" + strconv.Itoa(i))
		if err != nil {
			return err
		}
	}

	_, err = ConfirmTask(&ConfirmTaskArgs{Type: Map, TaskID: reply.TaskID})
	if err != nil {
		return err
	}
	return nil
}

func execReduceTask(workerID string, reducef func(string, []string) string, reply *RequestTaskReply) error {
	f, err := os.Open("./tmpfiles/mr-reduce-in-" + reply.TaskID)
	defer f.Close()
	if err != nil {
		return err
	}
	reader := bufio.NewReader(f)
	decoder := json.NewDecoder(reader)

	// outputFile, err := os.Create("mr-out-" + reply.TaskID)
	tempName := fmt.Sprint(time.Now().UnixNano())
	tempFile, err := os.Create("./tmpfiles/" + tempName)
	defer tempFile.Close()

	if err != nil {
		return err
	}
	for {
		var kvl KeyValueList
		if err = decoder.Decode(&kvl); err != nil {
			break
		}
		value := reducef(kvl.Key, kvl.Value)
		tempFile.WriteString(fmt.Sprintf("%v %v\n", kvl.Key, value))
	}
	tempFile.Sync()

	submitReply, err := SubmitTask(&SubmitTaskArgs{WorkerID: workerID, Type: Reduce, TaskID: reply.TaskID})
	log.Info("Submitted reduce task", reply.TaskID)
	if err != nil {
		return err
	}
	if submitReply.Err != "" {
		return errors.New(submitReply.Err)
	}
	err = os.Rename("./tmpfiles/" + tempName, "./mr-out-" + reply.TaskID)
	if err != nil {
		return err
	}

	_, err = ConfirmTask(&ConfirmTaskArgs{Type: Reduce, TaskID: reply.TaskID})
	if err != nil {
		return err
	}

	return nil
}

func RequestTask(args *RequestTaskArgs) (reply *RequestTaskReply, err error) {
	var rep RequestTaskReply
	if call("Coordinator.RequestTask", args, &rep) {
		return &rep, nil
	}
	return nil, errors.New("failed to call")
}

func SubmitTask(args *SubmitTaskArgs) (reply *SubmitTaskReply, err error) {
	var rep SubmitTaskReply
	if call("Coordinator.SubmitTask", args, &rep) {
		return &rep, nil
	}
	return nil, errors.New("failed to call")
}

func ConfirmTask(args *ConfirmTaskArgs) (reply *ConfirmTaskReply, err error) {
	var rep ConfirmTaskReply
	if call("Coordinator.ConfirmTask", args, &rep) {
		return &rep, nil
	}
	return nil, errors.New("failed to call")
}


//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	log.Error(err)
	return false
}
