package shardgrp

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type Clerk struct {
	*tester.Clnt
	servers []string
	leader  int // 上一次成功请求的 leader（在 servers[] 中的下标）
	// 你可以在这个结构体中添加字段。
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{Clnt: clnt, servers: servers}
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// 在这里补充代码
	args := rpc.GetArgs{
		Key: key,
	}

	for {
		reply := rpc.GetReply{}
		if ok := ck.Clnt.Call(ck.servers[ck.leader], "KVServer.Get", &args, &reply); !ok {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		switch reply.Err {
		case rpc.OK:
			return reply.Value, reply.Version, reply.Err
		case rpc.ErrNoKey:
			return "", 0, rpc.ErrNoKey
		case rpc.ErrWrongGroup:
			return "", 0, rpc.ErrWrongGroup
		default:
			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// 在这里补充代码
	args := rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}
	for i := 1; ; i++ {
		reply := rpc.PutReply{}
		if ok := ck.Clnt.Call(ck.servers[ck.leader], "KVServer.Put", &args, &reply); !ok {
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
		case rpc.ErrWrongGroup:
			return rpc.ErrWrongGroup
		case rpc.ErrWrongLeader:
			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
		default:
			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// 在这里补充代码
	args := shardrpc.FreezeShardArgs{
		Shard: s,
		Num:   num,
	}
	for {
		reply := shardrpc.FreezeShardReply{}
		ok := ck.Clnt.Call(ck.servers[ck.leader], "KVServer.FreezeShard", &args, &reply)
		if !ok {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if reply.Err == rpc.ErrWrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return reply.State, reply.Err
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// 在这里补充代码
	args := shardrpc.InstallShardArgs{
		Shard: s,
		State: state,
		Num:   num,
	}
	for {
		reply := shardrpc.InstallShardReply{}
		ok := ck.Clnt.Call(ck.servers[ck.leader], "KVServer.InstallShard", &args, &reply)
		if !ok {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if reply.Err == rpc.ErrWrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		return reply.Err
	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// 在这里补充代码
	args := shardrpc.DeleteShardArgs{
		Shard: s,
		Num:   num,
	}
	for {
		reply := shardrpc.DeleteShardReply{}
		ok := ck.Clnt.Call(ck.servers[ck.leader], "KVServer.DeleteShard", &args, &reply)
		if !ok {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if reply.Err == rpc.ErrWrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return reply.Err
	}
}
