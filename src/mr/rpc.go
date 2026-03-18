package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskType int32

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

// TaskArgs 作为worker的任务请求结构体
type TaskArgs struct {
	TaskType TaskType // Map 还是 Reduce 任务
	ID       int      // 任务的编号（下标）
}

// TaskReply 作为coordinator分配任务结构体
type TaskReply struct {
	TaskType TaskType // MapTask, ReduceTask, WaitTask 还是 ExitTask
	ID       int      // 任务唯一索引
	NReduce  int      // 用于map阶段，即中间结果需要切成多少份
	NFile    int      // 用于reduce阶段，即有多少个map产生的文件要读
	Files    []string // 文件名切片
	// 如果是 MapTask，待处理的原始输入文件
	// 如果是 ReduceTask，所有 Map 产生的中间文件路径
}
