package mr

import "os"
import "strconv"

type TaskType int

const (
	MapTask    TaskType = 1
	ReduceTask TaskType = 2
	Done       TaskType = 3
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type TaskArgs struct {
}

type TaskReply struct {
	Type     TaskType
	Filename string
	NReduce  int
	NMap     int
	TaskNum  int
}

type FinishedTaskArgs struct {
	TaskType TaskType
	TaskNum  int
}

type FinishedTaskReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
