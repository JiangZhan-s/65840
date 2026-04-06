package shardctrler

//
// 带有 InitConfig、Query 和 ChangeConfigTo 方法的 Shardctrler
//

import (
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
}

const firstConfig = "firstConfig"

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
}

// tester 只会调用一次，用于提供第一版配置。
// 你可以用 shardcfg.String() 把 ShardConfig 序列化为字符串，
// 然后在版本 0 时 Put 到控制器使用的 kvsrv 中。
// 配置的 key 名称可以自行选择。
// 初始配置应将所有 shard 都分配给 shardgrp shardcfg.Gid1。
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// 在这里补充代码
	sck.Put(firstConfig, cfg.String(), 0)
}

func (sck *ShardCtrler) GetGroupClerk(gid tester.Tgid, servers []string) *shardgrp.Clerk {
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
	for err != rpc.OK {
		time.Sleep(50 * time.Microsecond)
		state, err = oldClk.FreezeShard(shard, num)
	}

	err = newClk.InstallShard(shard, state, num)
	for err != rpc.OK {
		time.Sleep(50 * time.Microsecond)
		err = newClk.InstallShard(shard, state, num)
	}

	err = oldClk.DeleteShard(shard, num)
	for err != rpc.OK {
		time.Sleep(50 * time.Microsecond)
		err = oldClk.DeleteShard(shard, num)
	}
	return rpc.OK

}

// tester 调用此方法，要求控制器将配置从当前版本变更为 new。
// 在配置变更过程中，当前控制器可能会被另一个控制器取代。
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// 在这里补充代码。
	// 检查合法性
	for {
		cfg := sck.Query()
		if cfg.Num >= new.Num {
			return
		}
		for shard := range shardcfg.NShards {
			// 对于每一个分片执行变更
			err := sck.ApplyChange(cfg, new, shardcfg.Tshid(shard), new.Num)
			if err == rpc.ErrWrongGroup {
				continue
			}
		}
		err := sck.Put(firstConfig, new.String(), rpc.Tversion(cfg.Num))
		if err == rpc.OK {
			return
		}
		if err == rpc.ErrMaybe {

			curcfg, _, err := sck.Get(firstConfig)
			if err == rpc.OK && shardcfg.FromString(curcfg).Num >= new.Num {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
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
