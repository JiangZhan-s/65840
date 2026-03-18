package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ValueVersion struct {
	value   string
	version rpc.Tversion
}

type KVServer struct {
	mu   sync.Mutex
	data map[string]ValueVersion
	// Your definitions here.
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		data: make(map[string]ValueVersion),
	}
	// Your code here.
	return kv
}

// Get 返回 args.Key 对应的 value 和 version，如果 key 存在。
// 否则，Get 返回 ErrNoKey。
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if val, ok := kv.data[args.Key]; ok {
		reply.Value = val.value
		reply.Version = val.version
		reply.Err = rpc.OK
	} else {
		reply.Version = 0
		reply.Err = rpc.ErrNoKey
	}
}

// 只有当 args.Version 与服务器上该 key 的 version 匹配时，才更新 key 的 value。
// 如果版本号不匹配，返回 ErrVersion。
// 如果 key 不存在，且 args.Version 为 0，则插入该值；否则返回 ErrNoKey。
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if val, ok := kv.data[args.Key]; !ok {
		if args.Version == 0 {
			kv.data[args.Key] = ValueVersion{
				value:   args.Value,
				version: 1,
			}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
	} else {
		if val.version == args.Version {
			kv.data[args.Key] = ValueVersion{
				value:   args.Value,
				version: args.Version + 1,
			}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrVersion
		}
	}
}

// 所有参数都可以忽略；它们用于分布式（副本）KV服务器。
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
