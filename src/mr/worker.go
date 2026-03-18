package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

func doMapTask(mapf func(string, string) []KeyValue, reply *TaskReply) error {
	// 读取文件内容
	inFileName := reply.Files[0]
	content, err := os.ReadFile(inFileName)
	if err != nil {
		fmt.Printf("OpenFile failed\n")
		return err
	}

	// 使用mapf函数得到切片结果
	kvs := mapf(reply.Files[0], string(content))

	// 使用nReduce个桶来存放中间结果
	bucket := make([][]KeyValue, reply.NReduce)
	for _, kv := range kvs {
		index := ihash(kv.Key) % reply.NReduce
		bucket[index] = append(bucket[index], kv)
	}

	// 创建NReduce个临时文件，将bucket写入临时文件
	for i := range reply.NReduce {
		outFileName := fmt.Sprintf("mr-%d-%d", reply.ID, i)

		f, err := os.CreateTemp(".", "mr-tmp-*")
		if err != nil {
			fmt.Printf("Creat TempFile failed\n")
			return err
		}
		// 使用json编码
		encoder := json.NewEncoder(f)
		for _, kv := range bucket[i] {
			err := encoder.Encode(&kv)
			if err != nil {
				fmt.Printf("Encode failed\n")
				return err
			}
		}
		// 刷新文件
		f.Close()
		// 原子重命名
		os.Rename(f.Name(), outFileName)
	}

	return nil

}

func doReduceTask(reducef func(string, []string) string, reply *TaskReply) error {
	// 循环打开所有中间文件，有reply.NFile个任务
	// 我要看的是mr-NFile...-ID
	var kvs []KeyValue

	for i := range reply.NFile {
		// 把所有的KeyValue都加到kvs中
		fileName := fmt.Sprintf("mr-%d-%d", i, reply.ID)
		f, err := os.Open(fileName)
		if err != nil {
			fmt.Printf("Open File failed\n")
			return err
		}
		// 读取json
		decoder := json.NewDecoder(f)
		for {
			var kv KeyValue

			if err := decoder.Decode(&kv); err == io.EOF {
				break
			} else if err != nil {
				fmt.Printf("Decode failed\n")
				f.Close()
				return err
			}

			kvs = append(kvs, kv)
		}
		f.Close()
	}

	// 按照key进行排序，便于后续合并
	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].Key < kvs[j].Key
	})

	// 正式进行reduce
	outFileName := fmt.Sprintf("mr-out-%d", reply.ID)
	of, err := os.CreateTemp(".", "mr-out-*")
	if err != nil {
		fmt.Printf("Create TempFile failed\n")
		return err
	}

	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}

		output := reducef(kvs[i].Key, values)

		fmt.Fprintf(of, "%v %v\n", kvs[i].Key, output)

		i = j
	}

	of.Close()
	os.Rename(of.Name(), outFileName)

	return nil

}

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.
	for {
		args := TaskArgs{}
		reply := TaskReply{}
		ok := call("Coordinator.AssignTask", &args, &reply)
		if !ok {
			fmt.Printf("call failed!\n")
			break
		}
		switch reply.TaskType {
		case MapTask:
			err := doMapTask(mapf, &reply)
			if err == nil {
				doneArgs := TaskArgs{
					TaskType: reply.TaskType,
					ID:       reply.ID,
				}
				doneReply := TaskReply{}
				call("Coordinator.TaskDone", &doneArgs, &doneReply)
			}
		case ReduceTask:
			err := doReduceTask(reducef, &reply)
			if err == nil {
				doneArgs := TaskArgs{
					TaskType: reply.TaskType,
					ID:       reply.ID,
				}
				doneReply := TaskReply{}
				call("Coordinator.TaskDone", &doneArgs, &doneReply)
			}
		case WaitTask:
			time.Sleep(time.Second)
		case ExitTask:
			return
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
