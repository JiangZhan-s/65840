package shardgrp

import (
	"bytes"
	"log"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

const (
	ENVKEY = "65840ENV"
)

type ValueVersion struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me  int
	rsm *rsm.RSM
	gid tester.Tgid

	// 在这里补充代码
	mu sync.Mutex

	// 数据相关
	data map[string]ValueVersion

	//配置相关
	frozen map[shardcfg.Tshid]bool // 分片是否冻结
	owned  map[shardcfg.Tshid]bool // 分片是否属于我
	// 存储见到过的每个shard的最大num
	numm map[shardcfg.Tshid]shardcfg.Tnum

	freezeData map[shardcfg.Tshid][]byte // 数据缓存
}

func (kv *KVServer) DoOp(req any) any {
	switch args := req.(type) {
	case rpc.GetArgs:
		return kv.applyGet(args)

	case rpc.PutArgs:
		return kv.applyPut(args)

	case shardrpc.FreezeShardArgs:
		return kv.applyFreeze(args)

	case shardrpc.InstallShardArgs:
		return kv.applyInstall(args)

	case shardrpc.DeleteShardArgs:
		return kv.applyDelete(args)
	default:
		return nil
	}
}

func (kv *KVServer) applyDelete(args shardrpc.DeleteShardArgs) (reply shardrpc.DeleteShardReply) {

	kv.mu.Lock()
	defer kv.mu.Unlock()
	shard, num := args.Shard, args.Num
	// 看看是不是过时的
	// 看看是不是自己的, 以及是不是过时的
	if kv.owned[shard] == false {
		// 说明自己处理过了
		reply.Err = rpc.OK
		return
	}
	if num < kv.numm[shard] {
		reply.Err = rpc.OK
		return
	}

	// 删除数据
	for key := range kv.data {
		if shardcfg.Key2Shard(key) == shard {
			delete(kv.data, key)
		}
	}

	delete(kv.freezeData, shard)
	// 状态改变
	kv.numm[shard] = num
	kv.owned[shard] = false
	kv.frozen[shard] = false

	reply.Err = rpc.OK

	return
}

func (kv *KVServer) applyInstall(args shardrpc.InstallShardArgs) (reply shardrpc.InstallShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shard, state, num := args.Shard, args.State, args.Num
	// 同一个配置编号的 Install 必须幂等；重复执行会回滚后续写入。
	if num <= kv.numm[shard] {
		reply.Err = rpc.OK
		return
	}
	// 安装数据
	freezeData := kv.decodeFreezeData(state)
	for key, value := range freezeData {
		if shardcfg.Key2Shard(key) == shard {
			kv.data[key] = value
		}
	}
	// 状态改变
	kv.owned[shard] = true
	kv.frozen[shard] = false
	kv.numm[shard] = num
	delete(kv.freezeData, shard)

	reply.Err = rpc.OK
	return

}

func (kv *KVServer) getFreezeData(shard shardcfg.Tshid) []byte {
	// 先看看当前数据库缓存有没有
	_, ok := kv.freezeData[shard]
	if !ok {
		// 没有就创建一份
		freezeData := make(map[string]ValueVersion)
		for key, value := range kv.data {
			if shardcfg.Key2Shard(key) == shard {
				freezeData[key] = value
			}
		}
		kv.freezeData[shard] = kv.encodeFreezeData(freezeData)
	}
	return kv.freezeData[shard]
}

func (kv *KVServer) applyFreeze(args shardrpc.FreezeShardArgs) (reply shardrpc.FreezeShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shard, num := args.Shard, args.Num

	// 幂等性
	if num < kv.numm[shard] {
		reply.State = kv.getFreezeData(shard)
		reply.Num = num
		reply.Err = rpc.OK
		return
	}
	// 说明已经处理过了，直接返回信息就行
	if num == kv.numm[shard] {
		reply.Num = num
		reply.State = kv.getFreezeData(shard)
		reply.Err = rpc.OK
		return
	}

	// 看看是不是自己的
	if kv.owned[shard] == false {
		reply.Err = rpc.ErrWrongGroup
		return
	}

	// 备份数据
	freezeData := kv.getFreezeData(shard)
	// 状态改变
	kv.numm[shard] = num
	kv.frozen[shard] = true

	reply.State = freezeData
	reply.Num = num
	reply.Err = rpc.OK

	return
}

func (kv *KVServer) encodeFreezeData(freezeData map[string]ValueVersion) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(freezeData); err != nil {
		log.Fatal("Encode FreezeData failed")
	}
	return w.Bytes()
}

func (kv *KVServer) decodeFreezeData(data []byte) map[string]ValueVersion {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var freezeData map[string]ValueVersion
	if err := d.Decode(&freezeData); err != nil {
		log.Fatal("Decode FreezeData failed")
	}
	return freezeData
}

func (kv *KVServer) applyGet(args rpc.GetArgs) rpc.GetReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// 看看组错没错
	shard := shardcfg.Key2Shard(args.Key)
	if !kv.owned[shard] || kv.frozen[shard] {
		return rpc.GetReply{Err: rpc.ErrWrongGroup}
	}
	valueVersion, ok := kv.data[args.Key]
	if !ok {
		return rpc.GetReply{Err: rpc.ErrNoKey, Value: "", Version: 0}
	}

	return rpc.GetReply{
		Err:     rpc.OK,
		Value:   valueVersion.Value,
		Version: valueVersion.Version,
	}
}

func (kv *KVServer) applyPut(args rpc.PutArgs) rpc.PutReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// 	检查组
	shard := shardcfg.Key2Shard(args.Key)
	if !kv.owned[shard] || kv.frozen[shard] {
		return rpc.PutReply{Err: rpc.ErrWrongGroup}
	}
	current, ok := kv.data[args.Key]

	if !ok {
		if args.Version != 0 {
			return rpc.PutReply{Err: rpc.ErrVersion}
		}
		kv.data[args.Key] = ValueVersion{Value: args.Value, Version: 1}
		return rpc.PutReply{Err: rpc.OK}
	}

	if current.Version != args.Version {
		return rpc.PutReply{Err: rpc.ErrVersion}
	}

	kv.data[args.Key] = ValueVersion{Value: args.Value, Version: args.Version + 1}
	return rpc.PutReply{Err: rpc.OK}
}

func (kv *KVServer) Snapshot() []byte {
	// 在这里补充代码
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.owned)
	e.Encode(kv.frozen)
	e.Encode(kv.numm)
	e.Encode(kv.freezeData)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// 在这里补充代码
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.data) != nil {
		log.Fatalf("%v couldn't decode Kvdata", kv.me)
	}
	if d.Decode(&kv.owned) != nil {
		log.Fatalf("%v couldn't decode owned", kv.me)
	}
	if d.Decode(&kv.frozen) != nil {
		log.Fatalf("%v couldn't decode frozen", kv.me)
	}
	if d.Decode(&kv.numm) != nil {
		log.Fatalf("%v couldn't decode numm", kv.me)
	}
	if d.Decode(&kv.freezeData) != nil {
		log.Fatalf("%v couldn't decode freezeData", kv.me)
	}
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// 在这里补充代码
	err, rep := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader || rep == nil {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	submitReply := rep.(rpc.GetReply)
	*reply = submitReply
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// 在这里补充代码
	err, rep := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader || rep == nil {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	submitReply := rep.(rpc.PutReply)
	*reply = submitReply
}

// 冻结指定 shard（即拒绝该 shard 之后的 Get/Put），
// 并返回该 shard 中存储的键值数据。
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	// 在这里补充代码
	err, rep := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader || rep == nil {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	submitReply := rep.(shardrpc.FreezeShardReply)
	*reply = submitReply
}

// 为指定 shard 安装给定状态。
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// 在这里补充代码
	err, rep := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader || rep == nil {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	submitReply := rep.(shardrpc.InstallShardReply)
	*reply = submitReply
}

// 删除指定 shard。
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// 在这里补充代码
	err, rep := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader || rep == nil {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	submitReply := rep.(shardrpc.DeleteShardReply)
	*reply = submitReply
}

// StartShardServerGrp 为 shardgrp `gid` 启动一个服务器。
//
// StartShardServerGrp() 和 MakeRSM() 必须快速返回，
// 因此任何耗时工作都应放到 goroutine 中执行。
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// 对需要由 Go RPC 库编解码的结构体调用 labgob.Register。
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(rpc.PutReply{})
	labgob.Register(rpc.GetReply{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(shardrpc.FreezeShardReply{})
	labgob.Register(shardrpc.InstallShardReply{})
	labgob.Register(shardrpc.DeleteShardReply{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{
		gid:        gid,
		me:         me,
		data:       make(map[string]ValueVersion),
		owned:      make(map[shardcfg.Tshid]bool),
		frozen:     make(map[shardcfg.Tshid]bool),
		numm:       make(map[shardcfg.Tshid]shardcfg.Tnum),
		freezeData: make(map[shardcfg.Tshid][]byte),
	}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// 在这里补充代码
	for shard := range shardcfg.NShards {
		kv.frozen[shardcfg.Tshid(shard)] = false
		kv.owned[shardcfg.Tshid(shard)] = kv.gid == shardcfg.Gid1
	}

	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartServerShardGrp(ends, grp, srv, persister, tester.MaxRaftState)
}
