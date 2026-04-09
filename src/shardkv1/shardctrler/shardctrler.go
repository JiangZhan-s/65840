package shardctrler

//
// 带有 InitConfig、Query 和 ChangeConfigTo 方法的 Shardctrler
//

import (
	"sync"
	"time"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

// ShardCtrler：供控制器和 kv clerk 使用。
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // 由 Kill() 设置

	// 在这里添加你的数据。
	clerk map[tester.Tgid]*shardgrp.Clerk
	mu    sync.Mutex
}

const firstConfig = "firstConfig"
const nextConfig = "nextConfig"

// 创建一个 ShardCtrler，其状态存储在 kvsrv 中。
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	sck.clerk = make(map[tester.Tgid]*shardgrp.Clerk)
	// 在这里补充代码。
	return sck
}

// tester 会在启动一个新控制器前调用 InitController()。
// 在 A 部分，这个方法不需要做任何事。
// 在 B 和 C 部分，这个方法需要实现恢复逻辑。
func (sck *ShardCtrler) InitController() {
	for {
		curr := sck.getConfig(firstConfig)
		next := sck.getConfig(nextConfig)

		if next == nil || curr == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// 如果 next 比 curr 新，说明上次变更中途宕机了，需要继续完成
		if next.Num > curr.Num {
			// 直接复用变更逻辑，而不是开启递归调用
			sck.processMigration(curr, next)
		} else {
			// 两者相等，说明目前没有待处理任务
			return
		}
	}
}

// tester 只会调用一次，用于提供第一版配置。
// 你可以用 shardcfg.String() 把 ShardConfig 序列化为字符串，
// 然后在版本 0 时 Put 到控制器使用的 kvsrv 中。
// 配置的 key 名称可以自行选择。
// 初始配置应将所有 shard 都分配给 shardgrp shardcfg.Gid1。
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// 在这里补充代码
	sck.Put(firstConfig, cfg.String(), 0)
	sck.Put(nextConfig, cfg.String(), 0)
}

func (sck *ShardCtrler) GetGroupClerk(gid tester.Tgid, servers []string) *shardgrp.Clerk {
	sck.mu.Lock()
	defer sck.mu.Unlock()
	clk, ok := sck.clerk[gid]
	if !ok {
		clk = shardgrp.MakeClerk(sck.clnt, servers)
		sck.clerk[gid] = clk
	}
	return clk
}

func (sck *ShardCtrler) ApplyChange(old, new *shardcfg.ShardConfig, shard shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// 这里是处理一个切片的迁移
	// 分为三个步骤，冻结，安装，删除
	// 获得该分片的原分组和迁移目的分组
	oldGroup := old.Shards[shard]
	newGroup := new.Shards[shard]
	// 如果两个组一样，直接完事了
	if oldGroup == newGroup {
		return rpc.OK
	}

	// 由于进行切换这个操作需要由clk进行发送，所以需要获取这个组的clk
	oldServers, ok := old.Groups[oldGroup]
	// 都没有服务器，换啥
	if !ok || len(oldServers) == 0 {
		return rpc.OK
	}
	newServers, ok := new.Groups[newGroup]
	if !ok || len(newServers) == 0 {
		return rpc.OK
	}

	oldClk := sck.GetGroupClerk(oldGroup, oldServers)
	newClk := sck.GetGroupClerk(newGroup, newServers)

	//接下来进行三个步骤

	state, err := oldClk.FreezeShard(shard, num)
	if err != rpc.OK {
		return err
	}

	err = newClk.InstallShard(shard, state, num)
	if err != rpc.OK {
		return err
	}

	err = oldClk.DeleteShard(shard, num)
	if err != rpc.OK {
		return err
	}
	return rpc.OK

}

func (sck *ShardCtrler) processMigration(curr *shardcfg.ShardConfig, target *shardcfg.ShardConfig) bool {
	allShardsDone := true
	for shard := 0; shard < shardcfg.NShards; shard++ {
		if curr.Shards[shard] == target.Shards[shard] {
			continue
		}

		// 执行 冻结 -> 安装 -> 删除
		err := sck.ApplyChange(curr, target, shardcfg.Tshid(shard), target.Num)
		if err != rpc.OK {
			allShardsDone = false
			break
		}
	}

	if allShardsDone {
		err := sck.Put(firstConfig, target.String(), rpc.Tversion(curr.Num))
		return err == rpc.OK
	}
	return false
}

// tester 调用此方法，要求控制器将配置从当前版本变更为 new。
// 在配置变更过程中，当前控制器可能会被另一个控制器取代。
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	for {
		curr := sck.Query() // 获取当前公认已完成的配置
		if curr.Num >= new.Num {
			return
		}

		// 先尝试把“愿望”写入nextConfig
		pendingNext := sck.getConfig(nextConfig)

		if pendingNext.Num < new.Num {
			// 尝试更新nextConfig
			sck.Put(nextConfig, new.String(), rpc.Tversion(pendingNext.Num))
		}

		// 重新获取最新的next，因为可能被别人改了
		target := sck.getConfig(nextConfig)

		// 执行迁移
		if sck.processMigration(curr, target) {
			if target.Num >= new.Num {
				return
			}
		}

		time.Sleep(30 * time.Millisecond)
	}
}

// 返回当前配置
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// 在这里补充代码。
	value, _, ok := sck.Get(firstConfig)
	if ok == rpc.ErrNoKey {
		return nil
	}
	config := shardcfg.FromString(value)
	return config
}

func (sck *ShardCtrler) getConfig(key string) *shardcfg.ShardConfig {
	val, _, ok := sck.Get(key)
	if ok != rpc.OK {
		return nil
	}
	return shardcfg.FromString(val)
}
