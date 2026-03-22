package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Define struct
type KeyValue struct {
	Key   string
	Value string
}

// Hash function
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string

// Define worker function
func Worker(sockname string,
	mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	coordSockName = sockname

	args := RPCArgs{
		DoneTaskType: InitTask,
		DoneTaskID:   -1,
	}

	for {

		// RPC call
		reply := RPCReply{}
		ok := call("Coordinator.AssignTask", &args, &reply)
		if !ok {
			time.Sleep(time.Second)
			continue
		}

		// reset
		args.DoneTaskID = -1

		// Confirm the processing procedure according to the task type
		switch reply.TaskType {

		case MapTask:
			log.Printf("worker received a map task %d/%d, processing...", reply.TaskID, reply.NMap)
			doMapTask(mapf, &reply)
			args.DoneTaskID = reply.TaskID
			args.DoneTaskType = MapTask

		case ReduceTask:
			log.Printf("worker received a reduce task %d/%d, processing...", reply.TaskID, reply.NReduce)
			doReduceTask(reducef, &reply)
			args.DoneTaskID = reply.TaskID
			args.DoneTaskType = ReduceTask

		case InitTask, MapWait, ReduceWait:
			// log.Printf("worker received an other task.")
			time.Sleep(time.Second)

		case ExitTask:
			// log.Printf("worker is ready to exit.")
			return
		}
	}
}

func doMapTask(mapf func(string, string) []KeyValue, reply *RPCReply) {

	file, err := os.Open(reply.Filename)
	if err != nil {
		panic(err)
	}
	content, err := io.ReadAll(file)
	file.Close()
	if err != nil {
		panic(err)
	}

	kva := mapf(reply.Filename, string(content))

	buckets := make([][]KeyValue, reply.NReduce)

	for _, kv := range kva {
		r := ihash(kv.Key) % reply.NReduce
		buckets[r] = append(buckets[r], kv)
	}

	for r := 0; r < reply.NReduce; r++ {

		tmp, err := os.CreateTemp(".", "mr-map-*")
		if err != nil {
			panic(err)
		}

		enc := json.NewEncoder(tmp)

		for _, kv := range buckets[r] {
			if err := enc.Encode(&kv); err != nil {
				panic(err)
			}
		}

		tmp.Close()

		// Atomic file process
		oname := fmt.Sprintf("mr-part-%d-%d", reply.TaskID, r)
		os.Rename(tmp.Name(), oname)
	}
}

func doReduceTask(reducef func(string, []string) string, reply *RPCReply) {

	intermediate := []KeyValue{}

	// Aggregate the Key-value pairs from various files.
	for m := 0; m < reply.NMap; m++ {

		name := fmt.Sprintf("mr-part-%d-%d", m, reply.TaskID)

		file, err := os.Open(name)
		if err != nil {
			continue
		}

		dec := json.NewDecoder(file)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}

		file.Close()
	}

	// Sort all key-value pair
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	tmp, err := os.CreateTemp(".", "mr-out-*")
	if err != nil {
		panic(err)
	}

	// Count the number of consecutive identical primary keys
	i := 0
	for i < len(intermediate) {

		j := i + 1
		for j < len(intermediate) &&
			intermediate[j].Key == intermediate[i].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(tmp, "%v %v\n",
			intermediate[i].Key,
			output)

		i = j
	}

	tmp.Close()

	oname := fmt.Sprintf("mr-out-%d", reply.TaskID)
	os.Rename(tmp.Name(), oname)
}

func call(rpcname string, args interface{}, reply interface{}) bool {

	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	return err == nil
}
