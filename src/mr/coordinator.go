package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Completed
)

type Task struct {
	ID        int
	Filename  string
	State     TaskState
	StartTime time.Time
}

type Coordinator struct {
	mu sync.Mutex

	mapTasks    []Task
	reduceTasks []Task

	nMap     int
	nMapDone int

	nReduce     int
	nReduceDone int

	done bool
}

// ================= RPC Handler =================
func (c *Coordinator) AssignTask(args *RPCArgs, reply *RPCReply) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	// ---------- 1. Mark completed ----------
	if args.DoneTaskID != -1 {

		switch args.DoneTaskType {

		case MapTask:
			if c.mapTasks[args.DoneTaskID].State != Completed {
				c.mapTasks[args.DoneTaskID].State = Completed
				c.nMapDone++
			}

		case ReduceTask:
			if c.reduceTasks[args.DoneTaskID].State != Completed {
				c.reduceTasks[args.DoneTaskID].State = Completed
				c.nReduceDone++
			}
		}
	}

	// ---------- 2. MAP PHASE ----------
	if c.nMapDone < c.nMap {

		if c.assignMapTask(reply) {
			return nil
		}

		reply.TaskType = MapWait
		return nil
	}

	// ---------- 3. REDUCE PHASE ----------
	if c.nReduceDone < c.nReduce {

		if c.assignReduceTask(reply) {
			return nil
		}

		reply.TaskType = ReduceWait
		return nil
	}

	// ---------- 4. EXIT ----------
	reply.TaskType = ExitTask
	c.done = true
	return nil
}

// ================= Assign Map =================
func (c *Coordinator) assignMapTask(reply *RPCReply) bool {

	for i := range c.mapTasks {

		t := &c.mapTasks[i]

		if t.State == Idle {

			t.State = InProgress
			t.StartTime = time.Now()

			reply.TaskType = MapTask
			reply.TaskID = t.ID
			reply.Filename = t.Filename
			reply.NMap = c.nMap
			reply.NReduce = c.nReduce

			return true
		}
	}

	return false
}

// ================= Assign Reduce =================
func (c *Coordinator) assignReduceTask(reply *RPCReply) bool {

	for i := range c.reduceTasks {

		t := &c.reduceTasks[i]

		if t.State == Idle {

			t.State = InProgress
			t.StartTime = time.Now()

			reply.TaskType = ReduceTask
			reply.TaskID = t.ID
			reply.NMap = c.nMap
			reply.NReduce = c.nReduce

			return true
		}
	}

	return false
}

// ================= Timeout Monitor =================
func (c *Coordinator) monitorTimeout() {

	for {

		time.Sleep(time.Second)

		c.mu.Lock()

		if c.done {
			c.mu.Unlock()
			return
		}

		if c.nMapDone < c.nMap {
			// Map timeout
			for i := range c.mapTasks {
				t := &c.mapTasks[i]
				if t.State == InProgress &&
					time.Since(t.StartTime) > 10*time.Second {
					log.Printf("Map task timeout → reset %d", t.ID)
					t.State = Idle
				}
			}
		} else {
			// Reduce timeout
			for i := range c.reduceTasks {
				t := &c.reduceTasks[i]
				if t.State == InProgress &&
					time.Since(t.StartTime) > 10*time.Second {
					log.Printf("Reduce task timeout → reset %d", t.ID)
					t.State = Idle
				}
			}
		}

		c.mu.Unlock()
	}
}

// ================= Server =================
func (c *Coordinator) server(sockname string) {

	rpc.Register(c)
	rpc.HandleHTTP()

	os.Remove(sockname)

	l, e := net.Listen("unix", sockname)

	if e != nil {
		log.Fatalf("listen error %v", e)
	}

	go http.Serve(l, nil)
}

// ================= Constructor =================
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {

	c := Coordinator{
		mapTasks:    make([]Task, len(files)),
		reduceTasks: make([]Task, nReduce),
		nMap:        len(files),
		nReduce:     nReduce,
	}

	for i, file := range files {
		c.mapTasks[i] = Task{
			ID:       i,
			Filename: file,
			State:    Idle,
		}
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{
			ID:    i,
			State: Idle,
		}
	}

	go c.monitorTimeout()

	c.server(sockname)

	return &c
}

// ================= Done =================
func (c *Coordinator) Done() bool {

	c.mu.Lock()
	defer c.mu.Unlock()

	return c.done
}
