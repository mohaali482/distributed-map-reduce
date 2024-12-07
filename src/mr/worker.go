package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type WorkerNode struct {
	ID         string
	mapf       func(string, string) []KeyValue
	reducef    func(string, []string) string
	shouldExit bool
	shouldWait bool
}

func (w *WorkerNode) RequestTask() {
	args := RequestTaskArgs{WorkerId: w.ID}
	reply := RequestTaskReply{}
	call("Coordinator.RequestTask", &args, &reply)

	if reply.Done {
		w.shouldExit = true
	} else if reply.Wait {
		w.shouldWait = true
	} else if reply.TaskType == MAP {
		w.Map(reply.Map)
	} else if reply.TaskType == REDUCE {
		w.Reduce(reply.Reduce)
	}

}

func (w *WorkerNode) Reduce(task *ReduceTask) {
	fileNames := task.Files
	intermediate := make([]KeyValue, 0)

	for _, fileName := range fileNames {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v \n", fileName)
		}

		scanner := bufio.NewScanner(file)

		for scanner.Scan() {
			kv := strings.Split(scanner.Text(), " ")
			intermediate = append(intermediate, KeyValue{Key: kv[0], Value: kv[1]})
		}
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%v", task.Reduce)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := w.reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	call("Coordinator.FinishTask", &FinishTaskArgs{TaskType: REDUCE, TaskId: task.Task.ID}, &FinishTaskReply{})
}

func (w *WorkerNode) Map(task *MapTask) {
	file, err := os.Open(task.FileName)
	if err != nil {
		log.Fatalf("cannot open %v \n", task.FileName)
	}

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.FileName)
	}

	kva := w.mapf(task.FileName, string(content))
	intermediate := make([][]KeyValue, task.NReduce)
	for _, val := range kva {
		pos := ihash(val.Key) % task.NReduce
		intermediate[pos] = append(intermediate[pos], val)
	}

	for pos, vals := range intermediate {
		fileName := fmt.Sprintf("mr-%v-%v", task.Task.ID, pos)
		ofile, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("cannot open output file, %v", fileName)
		}

		for _, kva := range vals {
			fmt.Fprintf(ofile, "%v %v\n", kva.Key, kva.Value)
		}

		ofile.Close()
	}

	call("Coordinator.FinishTask", &FinishTaskArgs{TaskType: MAP, TaskId: task.Task.ID}, &FinishTaskReply{})
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	id := uuid.New()

	worker := WorkerNode{
		ID:      id.String(),
		mapf:    mapf,
		reducef: reducef,
	}

	for !worker.shouldExit {
		worker.RequestTask()

		if worker.shouldWait {
			time.Sleep(time.Second)
			worker.shouldWait = false
		}
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

	fmt.Println(err)
	return false
}
