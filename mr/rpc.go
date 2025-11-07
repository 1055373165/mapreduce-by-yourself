package mr

import (
	"os"
	"strconv"
)

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask // wait for other tasks to finish
	ExitTask // all tasks are finished
)

type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Completed
)

type Task struct {
	TaskType  TaskType
	TaskID    int
	NReduce   int    // Reduce task total count
	NMap      int    // Map task total count
	InputFile string // input file name for map task
	ReduceID  int    // reduce task id for reduce task
}

// The get task request from worker
type GetTaskArgs struct {
	WorkerID string
}

// The assign result from master
type GetTaskReply struct {
	Task Task
}

// The task execute complete result from worker
type TaskCompleteArgs struct {
	TaskType TaskType
	TaskID   int
	WorkerID string
}

// The reply for task completed report from master
type TaskCompleteReply struct {
	Success bool
}

// coordinatorSock Unix filename
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
