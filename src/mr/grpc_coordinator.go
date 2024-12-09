package mr

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"6.5840/mr/mrpb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type CoordinatorGRPCImpl struct {
	mrpb.UnimplementedCoordinatorServer

	lock        *sync.Mutex
	MapTasks    []*mrpb.MapTask
	ReduceTasks []*mrpb.ReduceTask

	RemainingMapTasks    int
	RemainingReduceTasks int
}

func (c *CoordinatorGRPCImpl) checkAndPrempt(taskID int, taskType int32) {
	time.Sleep(time.Second * 10)

	c.lock.Lock()
	defer c.lock.Unlock()

	if taskType == int32(mrpb.TaskType_MAP) {
		task := c.MapTasks[taskID]
		if task.Task.Status == mrpb.TaskStatus_IN_PROGRESS {
			// fmt.Printf("Worker %v, took too long to compute map operation, making map task with an ID of %v available again \n", task.ID, task.WorkerId)
			task.Task.Status = mrpb.TaskStatus_IDLE
		}
	} else {
		task := c.ReduceTasks[taskID]
		if task.Task.Status == mrpb.TaskStatus_IN_PROGRESS {
			// fmt.Printf("Worker %v, took too long to compute reduce operation, making reduce task with an ID of %v available again \n", task.ID, task.WorkerId)
			task.Task.Status = mrpb.TaskStatus_IDLE
		}
	}
}

func (c *CoordinatorGRPCImpl) RequestTask(_ctx context.Context, args *mrpb.RequestTaskRequest) (*mrpb.RequestTaskReply, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	reply := &mrpb.RequestTaskReply{}

	if c.RemainingMapTasks > 0 {
		for id, task := range c.MapTasks {
			if task.Task.Status == mrpb.TaskStatus_IDLE {
				task.Task.Status = mrpb.TaskStatus_IN_PROGRESS
				task.Task.WorkerId = args.WorkerId
				task.Task.StartedAt = timestamppb.Now()

				reply.TaskType = mrpb.TaskType_MAP
				reply.Task = &mrpb.RequestTaskReply_MapTask{
					MapTask: task,
				}
				// 	TaskType: mrpb.TaskType_MAP,
				// 	Task:     task,
				// 	Done:     false,
				// 	Wait:     false,
				// }

				// fmt.Printf("Map Task with an ID of %v assigned to Worker with an ID of %v at time: %v \n",
				// 	task.ID, task.WorkerId,
				// 	task.StartedAt.Format(time.RFC3339Nano),
				// )
				go c.checkAndPrempt(id, MAP)
				return reply, nil
			}
		}

		reply.Wait = true
		return reply, nil
	}

	if c.RemainingReduceTasks > 0 {
		for id, task := range c.ReduceTasks {
			if task.Task.Status == mrpb.TaskStatus_IDLE {
				task.Task.Status = mrpb.TaskStatus_IN_PROGRESS
				task.Task.WorkerId = args.WorkerId
				task.Task.StartedAt = timestamppb.Now()

				reply.TaskType = mrpb.TaskType_REDUCE
				reply.Task = &mrpb.RequestTaskReply_ReduceTask{
					ReduceTask: task,
				}

				// fmt.Printf("Reduce Task with an ID of %v assigned to Worker with an ID of %v at time: %v \n",
				// 	task.ID, task.WorkerId,
				// 	task.StartedAt.Format(time.RFC3339Nano),
				// )
				go c.checkAndPrempt(id, REDUCE)
				return reply, nil
			}
		}

		reply.Wait = true
		return reply, nil
	}

	reply.Done = true
	return reply, nil
}

func (c *CoordinatorGRPCImpl) FinishTask(_ctx context.Context, args *mrpb.FinishTaskRequest) (*mrpb.FinishTaskReply, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	reply := &mrpb.FinishTaskReply{}

	if args.TaskType == MAP {
		task := c.MapTasks[args.TaskId]

		if task.Task.Status != mrpb.TaskStatus_COMPLETED {
			// fmt.Printf("Map Task with an ID of %v completed by Worker with an ID of %v at time: %v. Total time taken: %v \n",
			// 	task.ID, task.WorkerId,
			// 	time.Now().Format(time.RFC3339Nano),
			// 	time.Since(task.StartedAt),
			// )
			task.Task.Status = mrpb.TaskStatus_COMPLETED
			c.RemainingMapTasks--
		}
	} else if args.TaskType == REDUCE {
		task := c.ReduceTasks[args.TaskId]

		if task.Task.Status != mrpb.TaskStatus_COMPLETED {
			// fmt.Printf("Reduce Task with an ID of %v completed by Worker with an ID of %v at time: %v. Total time taken: %v \n",
			// 	task.ID, task.WorkerId,
			// 	time.Now().Format(time.RFC3339Nano),
			// 	time.Since(task.StartedAt),
			// )
			task.Task.Status = mrpb.TaskStatus_COMPLETED
			c.RemainingReduceTasks--
		}
	}

	return reply, nil
}

// start a thread that listens for RPCs from worker.go
func (c *CoordinatorGRPCImpl) server() {
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", 5051))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	mrpb.RegisterCoordinatorServer(grpcServer, c)
	go grpcServer.Serve(lis)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *CoordinatorGRPCImpl) Done() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.RemainingMapTasks == 0 && c.RemainingReduceTasks == 0
}

func MakeGRPCCoordinator(files []string, nReduce int) Coordinator {
	c := CoordinatorGRPCImpl{
		lock:                 &sync.Mutex{},
		MapTasks:             make([]*mrpb.MapTask, len(files)),
		ReduceTasks:          make([]*mrpb.ReduceTask, nReduce),
		RemainingMapTasks:    len(files),
		RemainingReduceTasks: nReduce,
	}

	for pos, fileName := range files {
		c.MapTasks[pos] = &mrpb.MapTask{
			FileName: fileName,
			NReducer: int32(nReduce),
			Task: &mrpb.Task{
				Id:     int32(pos),
				Status: mrpb.TaskStatus_IDLE,
			},
		}
	}

	for pos := range c.ReduceTasks {
		c.ReduceTasks[pos] = &mrpb.ReduceTask{
			Reduce: int32(pos),
			Files:  GenerateFileNames(pos, len(files)),
			Task: &mrpb.Task{
				Id:     int32(pos),
				Status: IDLE,
			},
		}
	}

	return &c
}
