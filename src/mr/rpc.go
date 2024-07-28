package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"strconv"
)

const (
	MAP    = "MAP"
	REDUCE = "REDUCE"
	DONE   = "DONE"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ApplyArgs struct {
	WorkerID     int
	LastTaskType string
	LastTaskID   int
}

type ReplyArgs struct {
	TaskId    int
	TaskType  string
	InputFile string
	MapNum    int
	ReduceNum int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func tmpMapResult(workerID int, taskID int, reduceId int) string {
	return fmt.Sprintf("tmp-worker-%d-%d-%d", workerID, taskID, reduceId)
}

func finalMapResult(taskID int, reduceID int) string {
	return fmt.Sprintf("mr-%d-%d", taskID, reduceID)
}

func tmpReduceResult(workerID int, reduceId int) string {
	return fmt.Sprintf("tmp-worker-%d-out-%d", workerID, reduceId)
}

func finalReduceResult(reduceID int) string {
	return fmt.Sprintf("mr-out-%d", reduceID)
}
