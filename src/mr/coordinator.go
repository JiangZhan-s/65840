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

// TaskState 是map/reduce任务的状态
type TaskState int

const (
	Idle TaskState = iota
	Running
	Completed
)

// JobPhase 是mapReduce 的阶段
type JobPhase int

const (
	MapPhase JobPhase = iota
	ReducePhase
	AllDonePhase
)

// TaskInfo 是单个任务的信息
type TaskInfo struct {
	state     TaskState
	startTime time.Time
}

// Coordinator 是调度所需信息
type Coordinator struct {
	// Your definitions here.
	files   []string // 待处理文件
	nReduce int      // reduce处理文件数量

	phase JobPhase

	mapTask    []TaskInfo // map任务，长度为len(files)
	reduceTask []TaskInfo // reduce任务，长度为nReduce
	mu         sync.Mutex // 保护临界资源
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.phase == MapPhase {
		allFinished := true
		for i := range c.mapTask {
			if c.mapTask[i].state != Completed {
				allFinished = false
			}

			if c.mapTask[i].state == Idle ||
				(c.mapTask[i].state == Running && time.Since(c.mapTask[i].startTime) > 10*time.Second) {

				c.mapTask[i].state = Running
				c.mapTask[i].startTime = time.Now()

				reply.TaskType = MapTask
				reply.ID = i
				reply.NReduce = c.nReduce
				reply.Files = []string{c.files[i]}

				return nil
			}
		}
		if allFinished {
			c.phase = ReducePhase
		} else {
			reply.TaskType = WaitTask
			return nil
		}
	}
	if c.phase == ReducePhase {
		allFinished := true
		for i := range c.reduceTask {
			if c.reduceTask[i].state != Completed {
				allFinished = false
			}
			if c.reduceTask[i].state == Idle ||
				(c.reduceTask[i].state == Running && time.Since(c.reduceTask[i].startTime) > 10*time.Second) {
				c.reduceTask[i].state = Running
				c.reduceTask[i].startTime = time.Now()

				reply.TaskType = ReduceTask
				reply.ID = i
				reply.NFile = len(c.files)
				reply.Files = []string{}

				return nil
			}
		}
		if allFinished {
			c.phase = AllDonePhase
		} else {
			reply.TaskType = WaitTask
			return nil
		}
	}
	if c.phase == AllDonePhase {
		reply.TaskType = ExitTask
		return nil
	}
	return nil
}

func (c *Coordinator) TaskDone(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == MapTask {
		if c.mapTask[args.ID].state == Completed {
			return nil
		}

		c.mapTask[args.ID].state = Completed
	} else if args.TaskType == ReduceTask {
		if c.reduceTask[args.ID].state == Completed {
			return nil
		}

		c.reduceTask[args.ID].state = Completed
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.phase == AllDonePhase {
		ret = true
	}
	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:      files,
		nReduce:    nReduce,
		mapTask:    make([]TaskInfo, len(files)),
		reduceTask: make([]TaskInfo, nReduce),
		phase:      MapPhase,
	}

	// Your code here.

	c.server(sockname)
	return &c
}
