package mr

import (
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Task struct {
	id        int
	inputFile string
	worker    int
	taskType  string
	deadLine  time.Time
}

type Coordinator struct {
	// Your definitions here.
	mtx        sync.Mutex
	inputFile  []string
	reduceNum  int
	mapNum     int
	taskStates map[string]Task
	todoList   chan Task
	stage      string
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) ApplyForTask(args *ApplyArgs, reply *ReplyArgs) error {
	// process the last task
	if args.LastTaskID != -1 {
		taskId := createTaskId(args.LastTaskID, args.LastTaskType)
		c.mtx.Lock()
		if task, ok := c.taskStates[taskId]; ok && task.worker != -1 {
			// log.Printf("worker %d finish task %d", args.WorkerID, task.id)
			if args.LastTaskType == MAP {
				for i := 0; i < c.reduceNum; i++ {
					err := os.Rename(tmpMapResult(task.worker, task.id, i), finalMapResult(task.id, i))
					if err != nil {
						log.Fatalf("can not rename %s: %e", tmpMapResult(task.worker, task.id, i), err)
					}
				}
			} else if args.LastTaskType == REDUCE {
				err := os.Rename(tmpReduceResult(task.worker, task.id), finalReduceResult(task.id))
				if err != nil {
					log.Fatalf("can not rename %s: %e", tmpReduceResult(task.worker, task.id), err)
				}
			}
			delete(c.taskStates, taskId)
			if len(c.taskStates) == 0 {
				c.shift()
			}
		}
		c.mtx.Unlock()
	}
	// assign the new task
	task, ok := <-c.todoList
	if !ok {
		return nil
	}
	reply.InputFile = task.inputFile
	reply.MapNum = c.mapNum
	reply.ReduceNum = c.reduceNum
	reply.TaskId = task.id
	reply.TaskType = task.taskType
	task.worker = args.WorkerID
	task.deadLine = time.Now().Add(10 * time.Second)
	// log.Printf("assign %s task %d to worker %d", task.taskType, task.id, args.WorkerID)
	c.mtx.Lock()
	c.taskStates[createTaskId(task.id, task.taskType)] = task
	c.mtx.Unlock()
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

func (c *Coordinator) shift() {
	// 加锁状态
	if c.stage == MAP {
		// log.Printf("Map Task finished")
		c.stage = REDUCE
		// 分配reduce task
		for i := 0; i < c.reduceNum; i++ {
			task := Task{
				id:       i,
				worker:   -1,
				taskType: REDUCE,
			}
			c.todoList <- task
			c.taskStates[createTaskId(i, REDUCE)] = task
		}
	} else if c.stage == REDUCE {
		close(c.todoList)
		c.stage = DONE
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	c.mtx.Lock()
	defer c.mtx.Unlock()
	return c.stage == DONE
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mtx:        sync.Mutex{},
		inputFile:  files,
		reduceNum:  nReduce,
		mapNum:     len(files),
		taskStates: make(map[string]Task),
		todoList:   make(chan Task, int(math.Max(float64(nReduce), float64(len(files))))),
		stage:      MAP,
	}

	for i, file := range files {
		task := Task{
			id:        i,
			inputFile: file,
			worker:    -1,
			taskType:  MAP,
		}
		c.todoList <- task
		c.taskStates[createTaskId(i, MAP)] = task
	}
	// 回收任务
	go c.collectTask()
	c.server()
	return &c
}

func createTaskId(id int, taskType string) string {
	return fmt.Sprintf("%d-%s", id, taskType)
}

func (c *Coordinator) collectTask() {
	for {
		time.Sleep(500 * time.Millisecond)
		c.mtx.Lock()
		if c.stage == DONE {
			c.mtx.Unlock()
			return
		}
		for _, task := range c.taskStates {
			if task.worker != -1 && time.Now().After(task.deadLine) {
				// task is expired
				task.worker = -1
				// log.Printf("task %d is expired", task.id)
				c.todoList <- task
			}
		}
		c.mtx.Unlock()
	}
}
