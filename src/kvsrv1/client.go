package kvsrv

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	// You may add code here.
	return ck
}

// Get 获取指定 key 当前的 value 和 version。
// 如果 key 不存在，则返回 ErrNoKey。
// 如果遇到其他错误，会一直重试直到成功。
//
// 你可以这样发送一个 RPC：
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// 注意 args 和 reply 的类型（包括是否为指针）必须和 RPC 处理函数声明一致。
// 另外，reply 必须作为指针传递。
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	args := rpc.GetArgs{
		Key: key,
	}
	reply := rpc.GetReply{}

	for {
		if ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply); !ok {
			time.Sleep(100 * time.Millisecond)
		}
		switch reply.Err {
		case rpc.OK:
			return reply.Value, reply.Version, reply.Err
		case rpc.ErrNoKey:
			return "", 0, rpc.ErrNoKey
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// Put 只有当请求中的 version 与服务器上该 key 的 version 匹配时，才会用 value 更新 key。
// 如果版本号不匹配，服务器应返回 ErrVersion。
// 如果 Put 第一次 RPC 就收到 ErrVersion，Put 应直接返回 ErrVersion，
// 因为此时 Put 肯定没有在服务器执行。
// 如果是重试时收到 ErrVersion，则 Put 必须返回 ErrMaybe，
// 因为之前的 RPC 可能已经被服务器成功处理，但响应丢失，Clerk 无法确定 Put 是否执行。
//
// 你可以这样发送一个 RPC：
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// 注意 args 和 reply 的类型（包括是否为指针）必须和 RPC 处理函数声明一致。
// 另外，reply 必须作为指针传递。
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	args := rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}
	reply := rpc.PutReply{}

	for i := 1; ; i++ {
		if ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply); !ok {
			time.Sleep(100 * time.Millisecond)
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
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
}
