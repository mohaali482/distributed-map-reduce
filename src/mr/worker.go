package mr

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"

	"6.5840/mr/mrpb"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

type Communication interface {
	RequestTask(args *RequestTaskArgs) *RequestTaskReply
	FinishTask(args *FinishTaskArgs) *FinishTaskReply
	Close()
}

type RPCCommunication struct{}

func NewRPCClient() *RPCCommunication {
	return &RPCCommunication{}
}

func (r *RPCCommunication) RequestTask(args *RequestTaskArgs) *RequestTaskReply {
	reply := &RequestTaskReply{}
	call("CoordinatorRPC.RequestTask", args, reply)

	return reply
}

func (r *RPCCommunication) FinishTask(args *FinishTaskArgs) *FinishTaskReply {
	reply := &FinishTaskReply{}
	call("CoordinatorRPC.FinishTask", args, reply)

	return reply
}

func (r *RPCCommunication) Close() {}

type GRPCCommunication struct {
	conn   *grpc.ClientConn
	client mrpb.CoordinatorClient
}

func NewGRPCClient() *GRPCCommunication {
	conn, err := grpc.NewClient("localhost:5051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	client := mrpb.NewCoordinatorClient(conn)

	return &GRPCCommunication{
		conn:   conn,
		client: client,
	}
}

func (r *GRPCCommunication) RequestTask(args *RequestTaskArgs) *RequestTaskReply {
	reply, _ := r.client.RequestTask(context.TODO(),
		&mrpb.RequestTaskRequest{
			WorkerId: args.WorkerId,
		})

	reqReply := &RequestTaskReply{}

	if reply.Done {
		reqReply.Done = true
	} else if reply.Wait {
		reqReply.Wait = true
	} else if reply.TaskType == mrpb.TaskType_MAP {
		reqReply.TaskType = MAP
		reqReply.Map = &MapTask{
			FileName: reply.GetMapTask().FileName,
			NReduce:  int(reply.GetMapTask().NReducer),
			Task: Task{
				ID:     int(reply.GetMapTask().GetTask().Id),
				Status: int(reply.GetMapTask().GetTask().Status),
			},
		}
	} else {
		reqReply.TaskType = REDUCE
		reqReply.Reduce = &ReduceTask{
			Files:  reply.GetReduceTask().Files,
			Reduce: int(reply.GetReduceTask().Reduce),
			Task: Task{
				ID:     int(reply.GetReduceTask().GetTask().Id),
				Status: int(reply.GetReduceTask().GetTask().Status),
			},
		}
	}

	return reqReply

}

func (r *GRPCCommunication) FinishTask(args *FinishTaskArgs) *FinishTaskReply {
	r.client.FinishTask(context.TODO(), &mrpb.FinishTaskRequest{
		TaskType: mrpb.TaskType(args.TaskType),
		TaskId:   int32(args.TaskId),
	})

	reqReply := &FinishTaskReply{}

	return reqReply
}

func (r *GRPCCommunication) Close() {
	r.conn.Close()
}

type WorkerNode struct {
	ID         string
	mapf       func(string, string) []KeyValue
	reducef    func(string, []string) string
	shouldExit bool
	shouldWait bool
	comm       Communication
}

func (w *WorkerNode) RequestTask() {
	args := RequestTaskArgs{WorkerId: w.ID}
	reply := w.comm.RequestTask(&args)

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

		dec := json.NewDecoder(file)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%v", task.Reduce)
	tempOname := fmt.Sprintf("mr-out-%v-%v", task.Reduce, w.ID)

	if _, err := os.Stat(oname); err != nil {
		if !os.IsNotExist(err) {
			log.Fatal("Unexpected error occurred,", err)
		}

		ofile, err := os.Create(tempOname)

		if err != nil {
			log.Fatal("unable to open reduce file")
		}

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

			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

			i = j
		}

		ofile.Close()
		os.Rename(tempOname, oname)
	}

	w.comm.FinishTask(&FinishTaskArgs{TaskType: REDUCE, TaskId: task.Task.ID})
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

	wg := sync.WaitGroup{}
	for pos, vals := range intermediate {
		wg.Add(1)
		go func(pos int, vals []KeyValue) {
			fileName := fmt.Sprintf("mr-%v-%v", task.Task.ID, pos)

			if _, err := os.Stat(fileName); err == nil {
				wg.Done()
				return
			}

			ofile, err := os.Create(fileName)
			if err != nil {
				log.Fatalf("cannot open output file, %v", fileName)
			}

			enc := json.NewEncoder(ofile)
			for _, kv := range vals {
				err := enc.Encode(kv)
				if err != nil {
					log.Fatal("cannot write into file")
				}
			}
			ofile.Close()
			wg.Done()
		}(pos, vals)
	}

	wg.Wait()
	w.comm.FinishTask(&FinishTaskArgs{TaskType: MAP, TaskId: task.Task.ID})
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
		comm:    NewGRPCClient(),
	}

	defer worker.comm.Close()

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
