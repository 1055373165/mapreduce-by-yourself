package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

type KeyValue struct {
	Key   string
	Value string
}

// ByKey for sort
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	workerID := fmt.Sprintf("worker-%d", os.Getpid())
	log.Printf("[Worker %s] Started", workerID)

	for {
		// 1. 请求任务
		task := requestTask(workerID)

		switch task.TaskType {
		case MapTask:
			doMap(workerID, task, mapf)
			reportTaskComplete(workerID, MapTask, task.TaskID)
		case ReduceTask:
			doReduce(workerID, task, reducef)
			reportTaskComplete(workerID, ReduceTask, task.TaskID)
		case WaitTask:
			log.Printf("[Worker %s] No task available, waiting...", workerID)
			time.Sleep(1 * time.Second)
		case ExitTask:
			log.Printf("[Worker %s] All tasks completed, exiting", workerID)
			return
		}
	}
}

// requestTask request task from coordinator
func requestTask(workerID string) Task {
	args := GetTaskArgs{WorkerID: workerID}
	reply := GetTaskReply{}

	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		log.Fatal("[Worker %s] Failed to get task", workerID)
	}

	return reply.Task
}

// reportTaskComplete report task complete to coordinator
func reportTaskComplete(workerID string, taskType TaskType, taskID int) {
	args := TaskCompleteArgs{
		TaskType: taskType,
		TaskID:   taskID,
		WorkerID: workerID,
	}
	reply := TaskCompleteReply{}
	ok := call("Coordinator.TaskComplete", &args, &reply)
	if !ok {
		log.Printf("[Worker %s] Failed to report task completion", workerID)
	}
}

// doMap execute map task
func doMap(workerID string, task Task, mapf func(string, string) []KeyValue) {
	log.Printf("[Worker %s] Executing Map task %d (file: %s)", workerID, task.TaskID, task.InputFile)

	// 1. read content from input file
	content, err := ioutil.ReadFile(task.InputFile)
	if err != nil {
		log.Fatalf("[Worker %s] Cannot read %v: %v", workerID, task.InputFile, err)
	}

	// 2. call map function
	kva := mapf(task.InputFile, string(content))

	// 3. part output to nReduce files
	intermediate := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		reduceID := ihash(kv.Key) % task.NReduce
		intermediate[reduceID] = append(intermediate[reduceID], kv)
	}

	// 4. write intermediate to files
	for i := 0; i < task.NReduce; i++ {
		filename := fmt.Sprintf("mr-%d-%d", task.TaskID, i)
		file, _ := os.Create(filename)

		enc := json.NewEncoder(file)
		for _, kv := range intermediate[i] {
			enc.Encode(&kv)
		}

		file.Close()
	}

	log.Printf("[Worker %s] Map task %d completed, generated %d intermediate files", workerID, task.TaskID, task.NReduce)
}

// doReduce execute reduce task
func doReduce(workerID string, task Task, reducef func(string, []string) string) {
	log.Printf("[Worker %s] Executing Reduce task %d", workerID, task.ReduceID)

	// 1. read all intermediate files
	var intermediate []KeyValue
	for mapID := 0; mapID < task.NMap; mapID++ {
		filename := fmt.Sprintf("mr-%d-%d", mapID, task.ReduceID)

		file, err := os.Open(filename)
		if err != nil {
			log.Printf("[Worker %s] Cannot open %v: %v", workerID, filename, err)
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

	log.Printf("[Worker %s] Loaded %d intermediate KVs", workerID, len(intermediate))

	// 2. sort
	sort.Sort(ByKey(intermediate))

	// 3. create tmp output file
	tmpFile, _ := ioutil.TempFile("", "mr-out-*")

	// 4. group and call the Reduce function
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	tmpFile.Close()

	// 5. atomic rename
	outputFile := fmt.Sprintf("mr-out-%d", task.ReduceID)
	os.Rename(tmpFile.Name(), outputFile)

	log.Printf("[Worker %s] Reduce task %d completed, output: %s", workerID, task.ReduceID, outputFile)
}

// ihash hash function (used for partitioning)
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// call send rpc request
func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing: ", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	log.Println(err)
	return false
}
