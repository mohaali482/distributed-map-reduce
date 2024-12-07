package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	IDLE = iota
	IN_PROGRESS
	COMPLETED
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

type Coordinator struct {
	lock        *sync.Mutex
	wg          *sync.WaitGroup
	MapTasks    []*MapTask
	ReduceTasks []*ReduceTask

	RemainingMapTasks    int
	RemainingReduceTasks int
}

func (c *Coordinator) checkAndPrempt(taskID int, taskType int) {
	time.Sleep(time.Second * 10)

	c.lock.Lock()
	defer c.lock.Unlock()

	if taskType == MAP {
		task := c.MapTasks[taskID]
		if task.Status == IN_PROGRESS {
			// fmt.Printf("Worker %v, took too long to compute map operation, making map task with an ID of %v available again \n", task.ID, task.WorkerId)
			task.Status = IDLE
		}
	} else {
		task := c.ReduceTasks[taskID]
		if task.Status == IN_PROGRESS {
			// fmt.Printf("Worker %v, took too long to compute reduce operation, making reduce task with an ID of %v available again \n", task.ID, task.WorkerId)
			task.Status = IDLE
		}
	}

	c.wg.Done()
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.RemainingMapTasks > 0 {
		for id, task := range c.MapTasks {
			if task.Status == IDLE {
				task.Status = IN_PROGRESS
				task.WorkerId = args.WorkerId
				task.StartedAt = time.Now()

				reply.TaskType = MAP
				reply.Map = task
				reply.Done = false

				// fmt.Printf("Map Task with an ID of %v assigned to Worker with an ID of %v at time: %v \n",
				// 	task.ID, task.WorkerId,
				// 	task.StartedAt.Format(time.RFC3339Nano),
				// )
				c.wg.Add(1)
				go c.checkAndPrempt(id, MAP)
				return nil
			}
		}

		reply.Wait = true
		return nil
	}

	if c.RemainingReduceTasks > 0 {
		for id, task := range c.ReduceTasks {
			if task.Status == IDLE {
				task.Status = IN_PROGRESS
				task.WorkerId = args.WorkerId
				task.StartedAt = time.Now()

				reply.TaskType = REDUCE
				reply.Reduce = task
				reply.Done = false

				// fmt.Printf("Reduce Task with an ID of %v assigned to Worker with an ID of %v at time: %v \n",
				// 	task.ID, task.WorkerId,
				// 	task.StartedAt.Format(time.RFC3339Nano),
				// )
				c.wg.Add(1)
				go c.checkAndPrempt(id, REDUCE)
				return nil
			}
		}

		reply.Wait = true
		return nil
	}

	reply.Done = true
	return nil
}

type FinishTaskArgs struct {
	TaskType int
	TaskId   int
}

type FinishTaskReply struct {
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if args.TaskType == MAP {
		task := c.MapTasks[args.TaskId]

		if task.Status == IN_PROGRESS {
			// fmt.Printf("Map Task with an ID of %v completed by Worker with an ID of %v at time: %v. Total time taken: %v \n",
			// 	task.ID, task.WorkerId,
			// 	time.Now().Format(time.RFC3339Nano),
			// 	time.Since(task.StartedAt),
			// )
			task.Status = COMPLETED
			c.RemainingMapTasks--
		}
	} else if args.TaskType == REDUCE {
		task := c.ReduceTasks[args.TaskId]

		if task.Status == IN_PROGRESS {
			// fmt.Printf("Reduce Task with an ID of %v completed by Worker with an ID of %v at time: %v. Total time taken: %v \n",
			// 	task.ID, task.WorkerId,
			// 	time.Now().Format(time.RFC3339Nano),
			// 	time.Since(task.StartedAt),
			// )
			task.Status = COMPLETED
			c.RemainingReduceTasks--
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.RemainingMapTasks == 0 && c.RemainingReduceTasks == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		lock:                 &sync.Mutex{},
		wg:                   &sync.WaitGroup{},
		MapTasks:             make([]*MapTask, len(files)),
		ReduceTasks:          make([]*ReduceTask, nReduce),
		RemainingMapTasks:    len(files),
		RemainingReduceTasks: nReduce,
	}

	for pos, fileName := range files {
		c.MapTasks[pos] = &MapTask{
			FileName: fileName,
			NReduce:  nReduce,
			Task: Task{
				ID:     pos,
				Status: IDLE,
			},
		}
	}

	for pos := range c.ReduceTasks {
		c.ReduceTasks[pos] = &ReduceTask{
			Reduce: pos,
			Files:  generateFileNames(pos, len(files)),
			Task: Task{
				ID:     pos,
				Status: IDLE,
			},
		}
	}

	c.server()
	return &c
}

func generateFileNames(pos int, numberOfFiles int) []string {
	fileNames := make([]string, numberOfFiles)

	for i := range fileNames {
		fileNames[i] = fmt.Sprintf("mr-%v-%v", i, pos)
	}
	return fileNames
}
