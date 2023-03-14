package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "net/rpc"
import "net/http"

var mu sync.Mutex

type Coordinator struct {
	// Your definitions here.
	TaskQueue        chan *Task               // 等待执行的task
	TaskMeta         map[int]*CoordinatorTask // 当前所有task的信息 int is taskid
	CoordinatorPhase State                    // Master的阶段
	NReduce          int
	InputFiles       []string
	Intermediates    [][]string // Map任务产生的R个中间文件的信息
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) createMapTask() {
	for idx, filename := range c.InputFiles {
		taskMap := Task{
			Input:   filename,
			State:   Map,
			TaskId:  idx,
			NReduce: c.NReduce,
		}
		c.TaskQueue <- &taskMap
		c.TaskMeta[idx] = &CoordinatorTask{
			TaskStatus:    Idle,
			TaskReference: &taskMap,
		}
	}
}

func (c *Coordinator) createReduceTask() {
	c.TaskMeta = make(map[int]*CoordinatorTask, c.NReduce)
	for i, intermediate := range c.Intermediates {
		task := &Task{
			NReduce:      c.NReduce,
			Intermediate: intermediate,
			TaskId:       i,
			State:        Reduce,
		}
		c.TaskQueue <- task
		c.TaskMeta[i] = &CoordinatorTask{
			TaskStatus:    Idle,
			TaskReference: task,
		}
	}
}

func (c *Coordinator) AssignTask(args *ExampleArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	if len(c.TaskQueue) > 0 {
		*reply = *<-c.TaskQueue
		c.TaskMeta[reply.TaskId].TaskStatus = InProcess
		c.TaskMeta[reply.TaskId].StartTime = time.Now()
	} else if c.CoordinatorPhase == Exit {
		*reply = Task{State: Exit}
	} else {
		*reply = Task{State: Wait}
	}
	return nil
}

func (c *Coordinator) TaskCompleted(task *Task, reply *ExampleReply) error {
	mu.Lock()
	if task.State != c.CoordinatorPhase || c.TaskMeta[task.TaskId].TaskStatus == Completed {
		return nil
	}
	c.TaskMeta[task.TaskId].TaskStatus = Completed
	mu.Unlock()
	defer c.processTaskResult(task)
	return nil
}

func (c *Coordinator) processTaskResult(task *Task) {
	mu.Lock()
	defer mu.Unlock()
	switch task.State {
	case Map:
		//fmt.Println("map here")
		for reduceTaskId, filePath := range task.Intermediate {
			//fmt.Println(filePath)
			c.Intermediates[reduceTaskId] = append(c.Intermediates[reduceTaskId], filePath)
		}
		if c.allTaskDone() {
			c.createReduceTask()
			c.CoordinatorPhase = Reduce
		}
	case Reduce:
		if c.allTaskDone() {
			//fmt.Println("exit")
			c.CoordinatorPhase = Exit
		}
	}
}

func (c Coordinator) allTaskDone() bool {
	for _, task := range c.TaskMeta {
		if task.TaskStatus != Completed {
			return false
		}
	}
	return true
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *Task, reply *ExampleReply) error {
	for _, s := range args.Intermediate {
		fmt.Println(s)
	}
	fmt.Println(len(args.Intermediate))
	fmt.Println(args.Input)

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	//registering the coordinator as an rpc server
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	//sockname := coordinatorSock()
	//os.Remove(sockname)
	//l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	mu.Lock()
	defer mu.Unlock()
	// Your code here.
	ret = c.CoordinatorPhase == Exit

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		TaskQueue:        make(chan *Task, max(nReduce, len(files))),
		TaskMeta:         map[int]*CoordinatorTask{},
		CoordinatorPhase: Map,
		NReduce:          nReduce,
		InputFiles:       files,
		Intermediates:    make([][]string, nReduce),
	}
	c.createMapTask()
	// 启动一个goroutine 检查超时的任务
	go c.catchTimeOut()
	c.server()

	return &c
}

func (c *Coordinator) catchTimeOut() {
	for {
		time.Sleep(time.Second * 5)
		mu.Lock()
		if c.CoordinatorPhase == Exit {
			return
		}
		for _, coordinatorTask := range c.TaskMeta {
			if coordinatorTask.TaskStatus == InProcess && time.Now().Sub(coordinatorTask.StartTime) > 10*time.Second {
				c.TaskQueue <- coordinatorTask.TaskReference
				coordinatorTask.TaskStatus = Idle
			}
		}
		mu.Unlock()
	}
}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}
