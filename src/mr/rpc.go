package mr

import (
	"os"
	"strconv"
	"time"
)

const (
	IDLE = iota
	IN_PROGRESS
	COMPLETED
)

const (
	MAP = iota
	REDUCE
)

type Task struct {
	ID        int
	Status    int
	WorkerId  string
	StartedAt time.Time
}

type MapTask struct {
	FileName string
	NReduce  int
	Task
}

type ReduceTask struct {
	Files  []string
	Reduce int
	Task
}

type RequestTaskArgs struct {
	WorkerId string
}

type RequestTaskReply struct {
	TaskType int
	Map      *MapTask
	Reduce   *ReduceTask

	Done bool
	Wait bool
}

type FinishTaskArgs struct {
	TaskType int
	TaskId   int
}

type FinishTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
