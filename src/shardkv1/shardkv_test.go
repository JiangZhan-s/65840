package shardkv

import (
	//"log"
	"fmt"
	"testing"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	tester "6.5840/tester1"
)

const (
	NGRP  = 8
	NKEYS = 5 * shardcfg.NShards
)

// 使用 kvsrv1 实验中的 key/value 服务器，
// 测试 shard controller 的 Init 和 Query。
func TestInitQuery5A(t *testing.T) {

	// MakeTest 会通过 `kvsrv.StartKVServer`
	// 启动你的 lab2 key/value 服务器。
	ts := MakeTest(t, "Test (5A): Init and Query ...", true)
	defer ts.Cleanup()

	// 创建一个 shard controller
	sck := shardctrler.MakeShardCtrler(ts.Config.MakeClient())

	// 创建一个空的 shard 配置
	scfg := shardcfg.MakeShardConfig()

	// 计算一个新的 shard 配置，模拟 `shardcfg.Gid1` 加入集群，
	// 并把所有 shard 分配给 `shardcfg.Gid1`。
	scfg.JoinBalance(map[tester.Tgid][]string{shardcfg.Gid1: []string{"xxx"}})

	// 调用 controller 初始化并存储第一版配置
	sck.InitConfig(scfg)

	// 读取初始配置并检查
	cfg := sck.Query()
	if cfg.Num != 1 || cfg.Shards[0] != shardcfg.Gid1 {
		ts.t.Fatalf("Static wrong %v", cfg)
	}
	cfg.CheckConfig(t, []tester.Tgid{shardcfg.Gid1})
}

// 测试 shardkv clerk 在 1 个 shardgrp 下的 Get/Put（无重配置）
func TestStaticOneShardGroup5A(t *testing.T) {
	ts := MakeTest(t, "Test (5A): one shard group ...", true)
	defer ts.Cleanup()

	// tester 的 setupKVService() 会为 controller 建立一个 kvsrv
	// 用于存储配置，并调用 controller 的 Init() 方法，
	// 创建包含 1 个 shardgrp 的第一版配置。
	ts.setupKVService()

	ck := ts.MakeClerk()               // 创建一个 shardkv clerk
	ka, va := ts.SpreadPuts(ck, NKEYS) // 执行一些 put
	n := len(ka)
	for i := 0; i < n; i++ {
		ts.CheckGet(ck, ka[i], va[i], rpc.Tversion(1)) // 检查 put 结果
	}

	// 关闭 shardgrp 的 raft leader，并检查 key 仍然可用
	ts.shutdownLeader(ck.(*kvtest.TestClerk), shardcfg.Gid1)

	for i := 0; i < n; i++ {
		ts.CheckGet(ck, ka[i], va[i], rpc.Tversion(1)) // 检查 put 结果
	}
}

// 测试 shardctrler 的 join：加入新 group Gid2 后，
// 必须把部分 shard 迁到新 group，旧 group 应拒绝
// 对已迁移 shard 的 Get/Put。
func TestJoinBasic5A(t *testing.T) {
	ts := MakeTest(t, "Test (5A): a group joins...", true)
	defer ts.Cleanup()

	gid1 := ts.setupKVService()
	ck := ts.MakeClerk()
	ka, va := ts.SpreadPuts(ck, NKEYS)

	sck := ts.ShardCtrler()
	cfg := sck.Query()

	gid2 := ts.newGid()
	if ok := ts.joinGroups(sck, []tester.Tgid{gid2}); !ok {
		ts.t.Fatalf("TestJoinBasic5A: joinGroups failed")
	}

	cfg1 := sck.Query()
	if cfg.Num+1 != cfg1.Num {
		ts.t.Fatalf("TestJoinBasic5A: wrong num %d expected %d ", cfg1.Num, cfg.Num+1)
	}

	if !cfg1.IsMember(gid2) {
		ts.t.Fatalf("TestJoinBasic5A: %d isn't a member of %v", gid2, cfg1)
	}

	ts.checkShutdownSharding(gid1, ka, va)

	for i := 0; i < len(ka); i++ {
		ts.CheckGet(ck, ka[i], va[i], rpc.Tversion(1))
	}

	ts.checkShutdownSharding(gid2, ka, va)

	for i := 0; i < len(ka); i++ {
		ts.CheckGet(ck, ka[i], va[i], rpc.Tversion(1))
	}
}

// 测试 shardgrp 删除已迁移的 shard
func TestDeleteBasic5A(t *testing.T) {
	const (
		MAXRAFTSTATE = 1000
		VALUESIZE    = 10000
	)

	ts := MakeTestMaxRaft(t, "Test (5A): delete ...", true, false, VALUESIZE)
	defer ts.Cleanup()

	gid1 := ts.setupKVService()
	ck := ts.MakeClerk()

	ka, va := ts.SpreadPutsSize(ck, NKEYS, MAXRAFTSTATE)

	sz := ts.Group(gid1).SnapshotSize()

	sck := ts.ShardCtrler()
	gid2 := ts.newGid()
	if ok := ts.joinGroups(sck, []tester.Tgid{gid2}); !ok {
		ts.t.Fatalf("TestDeleteBasic5A: joinGroups failed")
	}

	// 再执行更多 Get，使所有副本都进行快照
	for j := 0; j < 5; j++ {
		for i := 0; i < len(ka); i++ {
			ts.CheckGet(ck, ka[i], va[i], rpc.Tversion(1))
		}
	}

	sz1 := ts.Group(gid1).SnapshotSize()
	sz2 := ts.Group(gid2).SnapshotSize()
	if sz1+sz2 > sz+10000 {
		ts.t.Fatalf("TestDeleteBasic5A: gid1 %d + gid2 %d = %d use too much space %d", sz1, sz2, sz1+sz2, sz)
	}
}

// 测试 shardctrler 的 leave
func TestJoinLeaveBasic5A(t *testing.T) {
	ts := MakeTest(t, "Test (5A): basic groups join/leave ...", true)
	defer ts.Cleanup()

	gid1 := ts.setupKVService()
	ck := ts.MakeClerk()
	ka, va := ts.SpreadPuts(ck, NKEYS)

	sck := ts.ShardCtrler()
	gid2 := ts.newGid()
	if ok := ts.joinGroups(sck, []tester.Tgid{gid2}); !ok {
		ts.t.Fatalf("TestJoinLeaveBasic5A: joinGroups failed")
	}

	ts.checkShutdownSharding(gid1, ka, va)

	for i := 0; i < len(ka); i++ {
		ts.CheckGet(ck, ka[i], va[i], rpc.Tversion(1))
	}

	ts.leave(sck, shardcfg.Gid1)
	if ok := ts.checkMember(sck, shardcfg.Gid1); ok {
		ts.t.Fatalf("%d is a member after leave", shardcfg.Gid1)
	}

	ts.Group(shardcfg.Gid1).Shutdown()

	for i := 0; i < len(ka); i++ {
		ts.CheckGet(ck, ka[i], va[i], rpc.Tversion(1))
	}

	// 将崩溃的 shard/group 恢复运行。
	ts.Group(shardcfg.Gid1).StartServers()

	// 重新加入
	ts.join(sck, shardcfg.Gid1, ts.Group(shardcfg.Gid1).SrvNames())

	for i := 0; i < len(ka); i++ {
		ts.CheckGet(ck, ka[i], va[i], rpc.Tversion(1))
	}

	ts.checkShutdownSharding(gid2, ka, va)
}

// 测试多个 group 反复 join/leave（可靠或不可靠网络）
func joinLeave5A(t *testing.T, reliable bool, part string) {
	ts := MakeTest(t, "Test (5A): many groups join/leave ...", reliable)
	defer ts.Cleanup()

	ts.setupKVService()
	ck := ts.MakeClerk()
	ka, va := ts.SpreadPuts(ck, NKEYS)

	sck := ts.ShardCtrler()
	grps := ts.groups(NGRP)

	ts.joinGroups(sck, grps)

	ts.checkShutdownSharding(grps[0], ka, va)

	for i := 0; i < len(ka); i++ {
		ts.CheckGet(ck, ka[i], va[i], rpc.Tversion(1))
	}

	ts.leaveGroups(sck, grps)

	for i := 0; i < len(ka); i++ {
		ts.CheckGet(ck, ka[i], va[i], rpc.Tversion(1))
	}
}

func TestManyJoinLeaveReliable5A(t *testing.T) {
	joinLeave5A(t, true, "Test (5A): many groups join/leave reliable...")
}

func TestManyJoinLeaveUnreliable5A(t *testing.T) {
	joinLeave5A(t, false, "Test (5A): many groups join/leave unreliable...")
}

// 测试完全停机后的恢复
func TestShutdown5A(t *testing.T) {
	const NJOIN = 2
	const NGRP = 2 + NJOIN

	ts := MakeTest(t, "Test (5A): shutdown ...", true)
	defer ts.Cleanup()

	ts.setupKVService()

	ck := ts.MakeClerk()
	ka, va := ts.SpreadPuts(ck, NKEYS)

	sck := ts.ShardCtrler()
	grps := ts.groups(NJOIN)
	ts.joinGroups(sck, grps)

	ts.checkShutdownSharding(grps[0], ka, va)

	for i := 0; i < len(ka); i++ {
		ts.CheckGet(ck, ka[i], va[i], rpc.Tversion(1))
	}

	for i := shardcfg.Gid1; i < NGRP; i++ {
		ts.Group(i).Shutdown()
	}

	for i := shardcfg.Gid1; i < NGRP; i++ {
		ts.Group(i).StartServers()
	}

	for i := 0; i < len(ka); i++ {
		ts.CheckGet(ck, ka[i], va[i], rpc.Tversion(1))
	}
}

// 测试：对于仍存活 group 上的 key，Get 能返回
func TestProgressShutdown5A(t *testing.T) {
	const (
		NJOIN = 4
		NSEC  = 2
	)

	ts := MakeTest(t, "Test (5A): progress ...", true)
	defer ts.Cleanup()

	ts.setupKVService()

	ck := ts.MakeClerk()
	ka, va := ts.SpreadPuts(ck, NKEYS)

	sck := ts.ShardCtrler()
	grps := ts.groups(NJOIN)
	ts.joinGroups(sck, grps)

	end := 2
	for _, g := range grps[0:2] {
		//log.Printf("shutdown %d", g)
		ts.Group(g).Shutdown()
	}

	alive := make(map[tester.Tgid]bool)
	for _, g := range grps[end:] {
		alive[g] = true
	}

	cfg := sck.Query()

	ch := make(chan rpc.Err)
	go func() {
		for i := 0; i < len(ka); i++ {
			s := shardcfg.Key2Shard(ka[i])
			g := cfg.Shards[s]
			if _, ok := alive[g]; ok {
				//log.Printf("key lookup %v(%d) gid %d", ka[i], s, g)
				ts.CheckGet(ck, ka[i], va[i], rpc.Tversion(1))
			}
		}
		ch <- rpc.OK
	}()

	select {
	case <-ch:
	case <-time.After(NSEC * time.Second):
		ts.Fatalf("Gets didn't finish")
	}
}

// 测试：来自未迁移 shard 的 Get 能快速返回
func TestProgressJoin5A(t *testing.T) {
	const (
		NJOIN = 4
		NSEC  = 4
		NCNT  = 100
	)

	ts := MakeTest(t, "Test (5A): progress ...", true)
	defer ts.Cleanup()

	ts.setupKVService()

	ck := ts.MakeClerk()
	ka, va := ts.SpreadPuts(ck, NKEYS)

	sck := ts.ShardCtrler()
	grps := ts.groups(NJOIN)
	ts.joinGroups(sck, grps)

	cfg := sck.Query()
	newcfg := cfg.Copy()
	newgid := tester.Tgid(NJOIN + 3)
	if ok := newcfg.JoinBalance(map[tester.Tgid][]string{newgid: []string{"xxx"}}); !ok {
		t.Fatalf("JoinBalance failed")
	}
	newcfg1 := newcfg.Copy()
	if ok := newcfg1.LeaveBalance([]tester.Tgid{newgid}); !ok {
		t.Fatalf("JoinBalance failed")
	}

	// 计算哪些 shard 不会移动，以及哪些 group 参与了 shard 迁移
	stable := make(map[shardcfg.Tshid]bool)
	participating := make(map[tester.Tgid]bool)
	for i, g := range newcfg1.Shards {
		if newcfg.Shards[i] == g {
			stable[shardcfg.Tshid(i)] = true
		} else {
			participating[g] = true
		}
	}

	//log.Printf("groups participating %v stable %v", participating, stable)
	//log.Printf("\ncfg %v\n %v\n %v", cfg.Shards, newcfg.Shards, newcfg1.Shards)

	ch0 := make(chan rpc.Err)
	go func() {
		for true {
			select {
			case <-ch0:
				return
			default:
				//log.Printf("join/leave %v", newgid)
				if ok := ts.joinGroups(sck, []tester.Tgid{newgid}); !ok {
					t.Fatalf("TestProgressJoin: join failed")
				}
				if ok := ts.leaveGroups(sck, []tester.Tgid{newgid}); !ok {
					t.Fatalf("TestProgressJoin: leave failed")
				}
			}
		}
	}()

	ch1 := make(chan int)
	go func() {
		// 获取这些 key：它们位于参与 join 的 group 上，
		// 但不在正在迁移的 shard 中
		t := time.Now().Add(NSEC * time.Second)
		nget := 0
		for time.Now().Before(t) {
			for i := 0; i < len(ka); i++ {
				s := shardcfg.Key2Shard(ka[i])
				if _, ok := stable[s]; ok {
					g := newcfg1.Shards[s]
					if _, ok := participating[g]; ok {
						// log.Printf("key lookup %v(%d) gid %d", ka[i], s, g)
						ts.CheckGet(ck, ka[i], va[i], rpc.Tversion(1))
						nget++
					}
				}
			}
		}
		ch1 <- nget
	}()

	select {
	case cnt := <-ch1:
		//log.Printf("cnt %d", cnt)
		if cnt < NCNT {
			ts.Fatalf("Two few gets finished %d; expected more than %d", cnt, NCNT)
		}

	case <-time.After(2 * NSEC * time.Second):
		ts.Fatalf("Gets didn't finish")
	}
	ch0 <- rpc.OK
}

// 测试线性一致性：group 在 join/leave 的同时，
// `nclnt` 个并发 clerk 在不可靠网络中执行 put/get。
func concurrentClerk(t *testing.T, nclnt int, reliable bool, part string) {
	const (
		NSEC = 20
	)

	ts := MakeTest(t, part, reliable)
	defer ts.Cleanup()

	ts.setupKVService()

	ka := kvtest.MakeKeys(NKEYS)
	ch := make(chan []kvtest.ClntRes)

	go func(ch chan []kvtest.ClntRes) {
		rs := ts.SpawnClientsAndWait(nclnt, NSEC*time.Second, func(me int, ck kvtest.IKVClerk, done chan struct{}) kvtest.ClntRes {
			return ts.OneClientPut(me, ck, ka, done)
		})
		ch <- rs
	}(ch)

	sck := ts.ShardCtrler()
	grps := ts.groups(NGRP)
	if ok := ts.joinGroups(sck, grps); !ok {
		t.Fatalf("concurrentClerk: joinGroups failed")
	}

	if ok := ts.leaveGroups(sck, grps); !ok {
		t.Fatalf("concurrentClerk: leaveGroups failed")
	}

	<-ch

	ts.CheckPorcupine()
}

// 测试线性一致性：group join/leave + 1 个并发 clerk put/get
func TestOneConcurrentClerkReliable5A(t *testing.T) {
	concurrentClerk(t, 1, true, "Test (5A): one concurrent clerk reliable...")
}

// 测试线性一致性：group join/leave + 多个并发 clerk put/get
func TestManyConcurrentClerkReliable5A(t *testing.T) {
	const NCLNT = 10
	concurrentClerk(t, NCLNT, true, "Test (5A): many concurrent clerks reliable...")
}

// 测试线性一致性：group join/leave + 1 个并发 clerk put/get
func TestOneConcurrentClerkUnreliable5A(t *testing.T) {
	concurrentClerk(t, 1, false, "Test (5A): one concurrent clerk unreliable ...")
}

// 测试线性一致性：group join/leave + 多个并发 clerk put/get
func TestManyConcurrentClerkUnreliable5A(t *testing.T) {
	const NCLNT = 10
	concurrentClerk(t, NCLNT, false, "Test (5A): many concurrent clerks unreliable...")
}

// 测试：如果 shardgrp 暂时宕机，join/leave 最终应能完成，
// 但在 shardgrp 宕机期间不应完成。
func TestJoinLeave5B(t *testing.T) {
	const NSEC = 2

	ts := MakeTest(t, "Test (5B): Join/leave while a shardgrp is down...", true)
	defer ts.Cleanup()

	gid1 := ts.setupKVService()
	ck := ts.MakeClerk()
	ka, va := ts.SpreadPuts(ck, NKEYS)

	sck := ts.ShardCtrler()
	cfg := sck.Query()

	ts.Group(gid1).Shutdown()

	gid2 := ts.newGid()
	ch := make(chan bool)
	go func() {
		ch <- ts.joinGroups(sck, []tester.Tgid{gid2})
	}()

	select {
	case err := <-ch:
		ts.Fatalf("Join finished %v", err)
	case <-time.After(1 * NSEC):
		// 给 Join 一些时间去尝试执行
	}

	// 现在 Join 应该可以完成
	ts.Group(gid1).StartServers()

	select {
	case ok := <-ch:
		if !ok {
			ts.Fatalf("TestJoinLeave5B: Join returned %t", ok)
		}
	case <-time.After(time.Second * NSEC):
		ts.Fatalf("Join didn't complete")
	}

	cfg1 := sck.Query()
	if cfg.Num+1 != cfg1.Num {
		ts.t.Fatalf("wrong num %d expected %d ", cfg1.Num, cfg.Num+1)
	}

	ts.Group(gid2).Shutdown()

	go func() {
		ch <- ts.leaveGroups(sck, []tester.Tgid{shardcfg.Gid1})
	}()

	select {
	case <-ch:
		ts.Fatalf("Leave finished")
	case <-time.After(NSEC * time.Second):
		// 给 Leave 一些时间去尝试执行
	}

	// 现在 Leave 应该可以完成
	ts.Group(gid2).StartServers()

	select {
	case ok := <-ch:
		if !ok {
			ts.Fatalf("TestJoinLeave5B: Leave failed %t", ok)
		}
	case <-time.After(time.Second * NSEC):
		ts.Fatalf("Leave didn't complete")
	}
	for i := 0; i < len(ka); i++ {
		ts.CheckGet(ck, ka[i], va[i], rpc.Tversion(1))
	}
}

// 测试被分区控制器的恢复
func TestRecoverCtrler5B(t *testing.T) {
	const (
		NPARTITION = 5
	)

	ts := MakeTest(t, "Test (5B): recover controller ...", true)
	defer ts.Cleanup()

	gid := ts.setupKVService()
	ck := ts.MakeClerk()
	ka, va := ts.SpreadPuts(ck, NKEYS)

	for i := 0; i < NPARTITION; i++ {
		ts.partitionCtrler(ck, gid, ka, va)
	}
}

// 测试并发控制器争夺领导权（可靠网络）
func TestConcurrentReliable5C(t *testing.T) {
	ts := MakeTestLeases(t, "Test (5C): Concurrent ctrlers ...", true)
	defer ts.Cleanup()
	ts.setupKVService()
	ck := ts.MakeClerk()
	ka, va := ts.SpreadPuts(ck, NKEYS)
	ts.concurCtrler(ck, ka, va)
}

// 测试并发控制器争夺领导权（不可靠网络）
func TestAcquireLockConcurrentUnreliable5C(t *testing.T) {
	ts := MakeTestLeases(t, "Test (5C): Concurrent ctrlers ...", false)
	defer ts.Cleanup()
	ts.setupKVService()
	ck := ts.MakeClerk()
	ka, va := ts.SpreadPuts(ck, NKEYS)
	ts.concurCtrler(ck, ka, va)
}

// 测试旧 leader 在 Join 过程中断线后重连时，
// 是否会被正确隔离（fenced off）。
func TestPartitionControllerJoin5C(t *testing.T) {
	const (
		NSLEEP = 2
		NSEC   = 1
		RAND   = 1000
	)

	ts := MakeTestLeases(t, "Test (5C): partition controller in join...", true)
	defer ts.Cleanup()
	ts.setupKVService()
	ck := ts.MakeClerk()
	ka, va := ts.SpreadPuts(ck, NKEYS)

	sck, clnt := ts.makeShardCtrlerClnt()
	sck.InitController()

	ch := make(chan rpc.Err)
	ngid := tester.Tgid(0)
	go func() {
		ngid = ts.newGid()
		args := []string{fmt.Sprintf("--max-raft-state=%d", ts.maxraftstate)}
		ts.Config.MakeGroupStart("shardgrp1d", args, ngid, NSRV)
		ts.Group(ngid).Shutdown()
		ts.join(sck, ngid, ts.Group(ngid).SrvNames())
		ch <- rpc.OK
	}()

	// 先休眠一段时间，让控制器有机会因 gid 宕机而卡在 join 过程中
	time.Sleep(1 * time.Second)

	// 将 sck 与网络分区
	clnt.DisconnectAll()

	// 在重启 shardgrp `ngid` 之前先等待一会儿
	time.Sleep(NSEC * time.Second)

	ts.Group(ngid).StartServers()

	// 启动新控制器，取代被分区的旧控制器
	sck0 := ts.makeShardCtrler()
	sck0.InitController()

	scfg := sck0.Query()
	if !scfg.IsMember(ngid) {
		t.Fatalf("Didn't recover gid %d", ngid)
	}

	// 重新连接旧控制器；它不应完成 ChangeConfigTo
	clnt.ConnectAll()

	// 等待旧控制器结束/退出
	<-ch

	time.Sleep(1 * time.Second)

	for i := 0; i < len(ka); i++ {
		ts.CheckGet(ck, ka[i], va[i], rpc.Tversion(1))
	}
}

// 让 leader 控制器在 join/leave 期间失去领导权，
// 并测试下一个控制器能否正确恢复。
func partitionRecovery5C(t *testing.T, reliable bool, npart, nclnt int) {
	const NSEC = 60
	ts := MakeTestLeases(t, "Test (5C): controllers with leased leadership ...", reliable)
	defer ts.Cleanup()
	gid := ts.setupKVService()
	ck := ts.MakeClerk()

	ka := make([]string, 0)
	va := make([]string, 0)
	if nclnt <= 0 {
		ka, va = ts.SpreadPuts(ck, NKEYS)
	}

	ch := make(chan []kvtest.ClntRes)
	if nclnt > 0 {
		ka := kvtest.MakeKeys(1)
		go func(ch chan []kvtest.ClntRes) {
			rs := ts.SpawnClientsAndWait(nclnt, NSEC*time.Second, func(me int, ck kvtest.IKVClerk, done chan struct{}) kvtest.ClntRes {
				return ts.OneClientPut(me, ck, ka, done)
			})
			ch <- rs
		}(ch)
	}

	for i := 0; i < npart; i++ {
		ts.partitionCtrler(ck, gid, ka, va)
	}

	if nclnt > 0 {
		<-ch
		ts.CheckPorcupine()
	}
}

func TestPartitionRecoveryReliableNoClerk5C(t *testing.T) {
	const (
		NPARTITION = 5
	)
	partitionRecovery5C(t, true, NPARTITION, 0)
}

func TestPartitionRecoveryUnreliableNoClerk5C(t *testing.T) {
	const (
		NPARTITION = 3
	)
	partitionRecovery5C(t, false, NPARTITION, 0)
}

func TestPartitionRecoveryReliableClerks5C(t *testing.T) {
	const (
		NPARTITION = 5
	)
	partitionRecovery5C(t, true, NPARTITION, 5)
}

func TestPartitionRecoveryUnreliableClerks5C(t *testing.T) {
	const (
		NPARTITION = 3
	)
	partitionRecovery5C(t, false, NPARTITION, 5)
}
