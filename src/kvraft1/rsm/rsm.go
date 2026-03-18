package rsm

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Op struct {
	// 在这里定义你的字段。
	// 字段名必须以大写字母开头，
	// 否则 RPC 会出错。
	Me  int
	Req any
	Id  string
}

// 想要实现自身复制的服务器（例如 ../server.go）会调用 MakeRSM，
// 并且必须实现 StateMachine 接口。
// 该接口允许 rsm 包与服务器进行交互以完成特定于服务器的操作：
// 服务器必须实现 DoOp 以执行操作（例如 Get 或 Put 请求），
// 还要实现 Snapshot/Restore 以快照和恢复服务器状态。
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // 如果日志变得很大就进行快照
	sm           StateMachine
	// 在这里定义你的字段。
	waitCh map[int]chan submitResult
	seq    uint64
}

type submitResult struct {
	op  Op
	req any
}

// servers[] 包含一组将通过 Raft 协作的服务器端口，
// 这些服务器共同组成容错的键值服务。
//
// me 是当前服务器在 servers[] 中的索引。
//
// k/v 服务器应通过底层 Raft 实现来存储快照，
// 该实现应调用 persister.SaveStateAndSnapshot()，
// 以原子方式保存 Raft 状态和快照。
// 当 Raft 保存的状态超过 maxraftstate 字节时，RSM 应该进行快照，
// 以便 Raft 可以对其日志进行垃圾回收。如果 maxraftstate 为 -1，
// 则不需要快照。
//
// MakeRSM() 必须快速返回，因此应为任何耗时的工作启动 goroutine。
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		waitCh:       map[int]chan submitResult{},
	}
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	if sn := persister.ReadSnapshot(); len(sn) > 0 {
		fmt.Println("HUIfu")
		rsm.sm.Restore(sn)
	}
	go rsm.reader()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

func (rsm *RSM) getWaitCh(index int) chan submitResult {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	ch, ok := rsm.waitCh[index]
	if !ok {
		ch = make(chan submitResult, 1)
		rsm.waitCh[index] = ch
	}
	return ch
}

func (rsm *RSM) peekWaitCh(index int) (chan submitResult, bool) {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	ch, ok := rsm.waitCh[index]
	return ch, ok
}

func (rsm *RSM) nextOpID() string {
	seq := atomic.AddUint64(&rsm.seq, 1)
	return fmt.Sprintf("%d-%d", rsm.me, seq)
}

// 向 Raft 提交一个命令，并等待其被提交。
// 如果客户端需要寻找新的 leader 并重试，则应返回 ErrWrongLeader。
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	op := Op{
		Me:  rsm.me,
		Id:  rsm.nextOpID(),
		Req: req,
	}

	if rsm.Raft() == nil {
		return rpc.ErrWrongLeader, nil
	}

	type startReply struct {
		index    int
		isLeader bool
	}
	startCh := make(chan startReply, 1)
	go func() {
		index, _, isLeader := rsm.Raft().Start(op)
		startCh <- startReply{index: index, isLeader: isLeader}
	}()

	var index int
	select {
	case s := <-startCh:
		if !s.isLeader {
			return rpc.ErrWrongLeader, nil // 我挂了，试试其他服务器。
		}
		index = s.index
	case <-time.After(600 * time.Millisecond):
		return rpc.ErrWrongLeader, nil
	}

	ch := rsm.getWaitCh(index)
	defer func() {
		rsm.mu.Lock()
		delete(rsm.waitCh, index)
		rsm.mu.Unlock()
	}()
	// 这两个定时器用于等待日志条目被应用，同时定期检查当前服务器是否仍然是领导者。
	ticker := time.NewTicker(40 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(10 * time.Second)
	defer timeout.Stop()

	for {
		select {
		case applied := <-ch:
			if applied.op.Id != op.Id || applied.op.Me != op.Me {
				return rpc.ErrWrongLeader, nil
			}
			return rpc.OK, applied.req
		case <-ticker.C:
			// 定期检查当前服务器是否仍然是领导者，如果不是则返回ErrWrongLeader。
			stateCh := make(chan bool, 1)
			go func() {
				_, isLeader := rsm.Raft().GetState()
				stateCh <- isLeader
			}()
			select {
			// 如果当前服务器不再是领导者，则返回ErrWrongLeader。
			case isLeader := <-stateCh:
				if !isLeader {
					return rpc.ErrWrongLeader, nil
				}
			case <-time.After(20 * time.Millisecond):
				// 如果检查领导者状态的操作超时了，继续等待日志条目被应用。
			}
		case <-timeout.C:
			// 如果等待日志条目被应用的总时间超过了10秒，则返回ErrWrongLeader。
			return rpc.ErrWrongLeader, nil
		}
	}
}

func (rsm *RSM) reader() {
	for msg := range rsm.applyCh {

		if msg.SnapshotValid {
			rsm.sm.Restore(msg.Snapshot)
			continue
		}
		if !msg.CommandValid {
			continue
		}

		op, ok := msg.Command.(Op)
		if !ok {
			continue
		}

		result := rsm.sm.DoOp(op.Req)

		if rsm.maxraftstate != -1 && float64(rsm.rf.PersistBytes()) > float64(rsm.maxraftstate)*0.9 {
			sn := rsm.sm.Snapshot()
			rsm.rf.Snapshot(msg.CommandIndex, sn)
		}

		ch, waiting := rsm.peekWaitCh(msg.CommandIndex)
		if !waiting {
			continue
		}
		select {
		// 这里使用select是为了避免在发送结果到通道时发生阻塞。如果通道已经满了（即没有等待的客户端），则默认分支会被执行，避免了阻塞。
		case ch <- submitResult{op: op, req: result}:
			//
		default:
		}
	}
}
