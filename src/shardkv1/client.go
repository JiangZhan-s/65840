package shardkv

//
// 与分片 key/value 服务交互的客户端代码。
//
// 客户端通过 shardctrler 查询当前配置，
// 找到 shard（key）到 group 的分配关系，
// 然后与持有该 key 所在 shard 的 group 通信。
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/shardkv1/shardgrp"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

// 路由客户端
type Clerk struct {
	clnt *tester.Clnt                    // 测试的rpc客户端(用作通信)
	sck  *shardctrler.ShardCtrler        // 查找配置的控制器
	rcks map[tester.Tgid]*shardgrp.Clerk // 组对应的查询客户端--组内客户端(这是个缓存)	gid->clerk
	// 你需要修改这个结构体。
	clientId  int64
	commandId int64
	mu        sync.Mutex
}

func nrand() int64 {
	// 生成一个 [0, 2^62) 之间的随机大整数
	max := big.NewInt(1)
	max.Lsh(max, 62)

	n, _ := rand.Int(rand.Reader, max)
	return n.Int64()
}

// tester 调用 MakeClerk 并传入一个 shardctrler，
// 这样客户端就可以调用其 Query 方法。
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt:      clnt,
		sck:       sck,
		clientId:  nrand(),
		commandId: 0,
	}
	ck.rcks = make(map[tester.Tgid]*shardgrp.Clerk)
	// 你需要在这里补充代码。
	return ck
}

func (ck *Clerk) GetClerk(gid tester.Tgid) (*shardgrp.Clerk, bool) {
	rck, ok := ck.rcks[gid]
	return rck, ok
}

func (ck *Clerk) NextCommandId() int64 {
	atomic.AddInt64(&ck.commandId, 1)
	return ck.commandId
}

// 从 shardgrp 读取一个 key。
// 你可以用 shardcfg.Key2Shard(key) 找到负责该 key 的 shard，
// 再用 ck.sck.Query() 读取当前配置并查到负责该 key 的 group 服务器。
// 你可以调用 shardgrp.MakeClerk(ck.clnt, servers) 为该 group 创建 clerk。
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// 你需要修改这个函数。
	for {
		shard := shardcfg.Key2Shard(key)
		config := ck.sck.Query()
		// 获取这个分片对应的组、组内的raft服务器们
		gid, servers, ok := config.GidServers(shard)
		if !ok || len(servers) == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// 到这个位置就是查到gid且有servers了
		// 那么就要尝试着使用这个组的客户端进行读取了
		rck, ok := ck.rcks[gid]
		if !ok {
			// 这说明目前还没有给这个组分配客户端
			rck = shardgrp.MakeClerk(ck.clnt, servers)
			// 这是把当前rpc网络和这个组的raft服务器组分给这个客户端
			ck.rcks[gid] = rck
		}

		// 到这个位置组里一定有客户端了，可以让这个客户端去查询了
		value, version, err := rck.Get(key) //这个处理是交给下层客户端去处理了
		if err == rpc.ErrWrongGroup {
			// 组不对，可能切换了，那重新查吧
			// 还得删掉这个gid向服务器组(和客户端)的映射，因为极有可能这个组是刚刚分配的
			// 这个客户端老掉牙了大叔，虽然好像不删也行
			delete(ck.rcks, gid)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return value, version, err
	}
}

// 向某个 shard group 写入一个 key。
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// 你需要修改这个函数。
	for {
		// 依然是老动作啊，和Get一样
		shard := shardcfg.Key2Shard(key)
		config := ck.sck.Query()
		gid, servers, ok := config.GidServers(shard)
		if !ok {
			time.Sleep(100 * time.Microsecond)
			continue
		}

		rck, ok := ck.rcks[gid]
		if !ok {
			rck = shardgrp.MakeClerk(ck.clnt, servers)
			ck.rcks[gid] = rck
		}

		// 写！
		err := rck.Put(key, value, version, ck.clientId, ck.NextCommandId())
		if err == rpc.ErrWrongGroup {
			delete(ck.rcks, gid)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return err
	}
}
