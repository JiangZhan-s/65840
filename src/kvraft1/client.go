package kvraft

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
	// 你可以在这个结构体中添加字段。
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// 你需要在这里添加代码。
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

// Get 获取指定 key 的当前值和版本。如果 key 不存在，则返回 ErrNoKey。
// 对于其他所有错误，它会一直重试，直到成功。
//
// 你可以这样向第 i 个服务器发送 RPC：
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// args 和 reply 的类型（包括是否为指针）必须与 RPC 处理函数声明的参数类型一致。
// 此外，reply 必须作为指针传递。
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	args := rpc.GetArgs{
		Key: key,
	}

	for {
		reply := rpc.GetReply{}
		if ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Get", &args, &reply); !ok {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		switch reply.Err {
		case rpc.OK:
			return reply.Value, reply.Version, reply.Err
		case rpc.ErrNoKey:
			return "", 0, rpc.ErrNoKey
		default:
			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// Put 仅当请求中的 version 与服务器上该 key 的 version 匹配时，才会用 value 更新 key。
// 如果版本号不匹配，服务器应返回 ErrVersion。
// 如果 Put 在第一次 RPC 时收到 ErrVersion，Put 应返回 ErrVersion，
// 因为此时 Put 操作肯定没有在服务器执行。
// 如果在重试 RPC 时服务器返回 ErrVersion，则 Put 必须返回 ErrMaybe，
// 因为之前的 RPC 可能已经被服务器成功处理，但响应丢失，Clerk 无法确定 Put 是否被执行。
//
// 你可以这样向第 i 个服务器发送 RPC：
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// args 和 reply 的类型（包括是否为指针）必须与 RPC 处理函数声明的参数类型一致。
// 此外，reply 必须作为指针传递。
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	args := rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}

	for i := 1; ; i++ {
		reply := rpc.PutReply{}
		if ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Put", &args, &reply); !ok {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		switch reply.Err {
		case rpc.OK:
			return rpc.OK
		case rpc.ErrNoKey:
			return rpc.ErrNoKey
		case rpc.ErrVersion:
			if i == 1 {
				return rpc.ErrVersion
			}
			return rpc.ErrMaybe
		case rpc.ErrWrongLeader:
			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
		default:
			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
		}
	}
}
