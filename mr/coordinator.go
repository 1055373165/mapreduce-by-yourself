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

// Task internal trace info
type TaskInfo struct {
	State     TaskState
	StartTime time.Time
	WorkerID  string
}

// Coordinator contorl node
type Coordinator struct {
	mu          sync.Mutex
	phase       string     // "map", "reduce", "done"
	nReduce     int        // Reduce task count
	nMap        int        // Map task count
	inputFiles  []string   // input file list
	mapTasks    []TaskInfo // all map tasks state info
	reduceTasks []TaskInfo // all reduce tasks state info
	done        bool       // all task is done
}

// MakeCoordinator init a coordinator node
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := &Coordinator{
		phase:       "map",
		nReduce:     nReduce,
		nMap:        len(files),
		inputFiles:  files,
		mapTasks:    make([]TaskInfo, len(files)),
		reduceTasks: make([]TaskInfo, nReduce),
	}

	// set all task state to idle
	for i := range c.mapTasks {
		c.mapTasks[i].State = Idle
	}
	for i := range c.reduceTasks {
		c.reduceTasks[i].State = Idle
	}

	log.Printf("[Coordinator] Initialized with %d map tasks, %d reduce tasks",
		len(files), nReduce)

	// start backend monitor goroutine
	go c.monitor()

	// start rpc server
	c.server()

	return c
}

// GetTask RPC handler
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.phase {
	case "map":
		// 分配 Map 任务
		for i, task := range c.mapTasks {
			if task.State == Idle {
				c.mapTasks[i].State = InProgress
				c.mapTasks[i].WorkerID = args.WorkerID
				c.mapTasks[i].StartTime = time.Now()

				reply.Task = Task{
					TaskType:  MapTask,
					TaskID:    i,
					NReduce:   c.nReduce,
					InputFile: c.inputFiles[i],
				}

				log.Printf("[Coordinator] Assigned Map task %d to worker %s", i, args.WorkerID)
				return nil
			}
		}

		// all map task assigned but not completed
		reply.Task = Task{TaskType: WaitTask}
		return nil

	case "reduce":
		for i, task := range c.reduceTasks {
			if task.State == Idle {
				c.reduceTasks[i].State = InProgress
				c.reduceTasks[i].WorkerID = args.WorkerID
				c.reduceTasks[i].StartTime = time.Now()

				reply.Task = Task{
					TaskType: ReduceTask,
					TaskID:   i,
					NMap:     c.nMap,
					ReduceID: i,
				}
				log.Printf("[Coordinator] Assigned Reduce task %d to worker %s", i, args.WorkerID)
				return nil
			}
		}

		reply.Task = Task{TaskType: WaitTask}
		return nil
	default:
		reply.Task = Task{TaskType: ExitTask}
		return nil
	}
}

// TaskComplete RPC handler
func (c *Coordinator) TaskComplete(args *TaskCompleteArgs, reply *TaskCompleteReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.TaskType {
	case MapTask:
		if args.TaskID < len(c.mapTasks) && c.mapTasks[args.TaskID].State == InProgress {
			c.mapTasks[args.TaskID].State = Completed
			log.Printf("[Coordinator] Map task %d completed by worker %s", args.TaskID, args.WorkerID)

			// check whether all map tasks are completed
			if c.allMapCompleted() {
				log.Println("[Coordinator] All map tasks completed, transitioning to reduce phase")
				c.phase = "reduce"
			}
		}
	case ReduceTask:
		if args.TaskID < len(c.reduceTasks) && c.reduceTasks[args.TaskID].State == InProgress {
			c.reduceTasks[args.TaskID].State = Completed
			log.Printf("[Coordinator] Reduce task %d completed by worker %s", args.TaskID, args.WorkerID)

			// check whether all reduce tasks are completed
			if c.allReduceCompleted() {
				log.Println("[Coordinator] All reduce tasks completed, job done")
				c.phase = "done"
				c.done = true
			}
		}
	}

	reply.Success = true
	return nil
}

// monnitor task timeout
func (c *Coordinator) monitor() {
	for !c.Done() {
		time.Sleep(1 * time.Second)

		c.mu.Lock()

		if c.phase == "map" {
			for i, task := range c.mapTasks {
				if task.State == InProgress && time.Since(task.StartTime) > 10*time.Second {
					log.Printf("[Coordinator] Map task %d timeout, resetting", i)
					c.mapTasks[i].State = Idle
				}
			}
		}

		if c.phase == "reduce" {
			for i, task := range c.reduceTasks {
				if task.State == InProgress && time.Since(task.StartTime) > 10*time.Second {
					log.Printf("[Coordinator] Reduce task %d timeout, resetting", i)
					c.reduceTasks[i].State = Idle
				}
			}
		}

		c.mu.Unlock()
	}
}

func (c *Coordinator) allMapCompleted() bool {
	for _, task := range c.mapTasks {
		if task.State != Completed {
			return false
		}
	}
	return true
}

func (c *Coordinator) allReduceCompleted() bool {
	for _, task := range c.reduceTasks {
		if task.State != Completed {
			return false
		}
	}
	return true
}

// Done regularly call to check if the entire job is completed
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.done
}

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()

	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error: ", e)
	}

	go http.Serve(l, nil)
}
