package mr

//
// RPC definitions.
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskType int

const (
	InitTask TaskType = iota
	MapTask
	MapWait
	ReduceTask
	ReduceWait
	ExitTask
)

type RPCArgs struct {
	DoneTaskID   int      // completed task ID
	DoneTaskType TaskType // Completed task type

}

type RPCReply struct {
	TaskType
	NMap     int    // number of map task
	NReduce  int    // number of reduce task
	TaskID   int    // task ID
	Filename string // only for map task
}
