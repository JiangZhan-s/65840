package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"reflect"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	tester "6.5840/tester1"
)

type ValueVersion struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me  int
	rsm *rsm.RSM

	// 你可以在这里添加你的定义。
	mu   sync.Mutex
	data map[string]ValueVersion
}

// 要将 req 类型转换为正确的类型，可以参考 Go 的类型 switch 或类型断言：
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// 在这里编写你的代码
	raft.DPrintln(kv.me, "开始类型判断", req, reflect.TypeOf(req))
	switch args := req.(type) {
	case rpc.GetArgs:
		kv.mu.Lock()
		defer kv.mu.Unlock()
		valueVersion, ok := kv.data[args.Key]
		reply := rpc.GetReply{}
		raft.DPrintln("到了GetArg类型里")
		if ok {
			reply.Err = rpc.OK
			reply.Value = valueVersion.Value
			reply.Version = valueVersion.Version
		} else {
			reply.Err = rpc.ErrNoKey
			reply.Value = ""
			reply.Version = 0
		}
		return reply

	case rpc.PutArgs:
		kv.mu.Lock()
		defer kv.mu.Unlock()
		reply := rpc.PutReply{}
		raft.DPrintln("到了PutArg类型里")
		if val, ok := kv.data[args.Key]; !ok {
			// 没有这个key
			if args.Version == 0 {
				kv.data[args.Key] = ValueVersion{
					Value:   args.Value,
					Version: 1,
				}
				raft.DPrintln("已经 放进来了", args.Key, kv.data[args.Key].Value, kv.data[args.Key].Version)
				reply.Err = rpc.OK
			} else {
				raft.DPrintf("版本冲突")
				reply.Err = rpc.ErrVersion
			}
		} else {
			// 有这个key
			if val.Version == args.Version {
				kv.data[args.Key] = ValueVersion{
					Value:   args.Value,
					Version: args.Version + 1,
				}
				reply.Err = rpc.OK
			} else {
				raft.DPrintf("版本冲突")
				reply.Err = rpc.ErrVersion
			}
		}
		return reply
	default:
		fmt.Println("全错！！！")
		return nil
	}
}

func (kv *KVServer) Snapshot() []byte {
	// 在这里编写你的代码
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// 在这里编写你的代码
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.data) != nil {
		log.Fatalf("%v couldn't decode Kvdata", kv.me)
	}
	// fmt.Println(kv.me, kv.data)
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// 在这里编写你的代码。使用 kv.rsm.Submit() 提交 args
	// 你可以用 Go 的类型转换将 Submit() 的 any 返回值转为 GetReply：rep.(rpc.GetReply)
	err, rep := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	if rep == nil {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	submitReply := rep.(rpc.GetReply)
	*reply = submitReply
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// 在这里编写你的代码。使用 kv.rsm.Submit() 提交 args
	// 你可以用 Go 的类型转换将 Submit() 的 any 返回值转为 PutReply：rep.(rpc.PutReply)
	raft.DPrintln("我是server", kv.me, "我收到请求Put", args)
	err, rep := kv.rsm.Submit(*args)
	raft.DPrintln("server收到了返回，目前hi", err, rep)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	if rep == nil {
		raft.DPrintln("rep是空的")
		reply.Err = rpc.ErrWrongLeader
		return
	}
	submitReply := rep.(rpc.PutReply)
	*reply = submitReply
}

// StartKVServer() 和 MakeRSM() 必须快速返回，因此对于任何耗时的工作应启动 goroutine。
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// 对你希望 Go 的 RPC 序列化/反序列化的结构体调用 labgob.Register。
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(rpc.GetReply{})
	labgob.Register(rpc.PutReply{})

	kv := &KVServer{
		me:   me,
		data: make(map[string]ValueVersion),
	}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// 你可能需要在这里添加初始化代码。
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
