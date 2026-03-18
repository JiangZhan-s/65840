package tester

//
// support for Raft and kvraft to save persistent
// Raft state (log &c) and k/v server snapshots.
//
// we will use the original persister.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import "sync"

type Persister struct {
	mu        sync.Mutex
	raftstate []byte
	snapshot  []byte
}

// MakePersister 创建一个新的 Persister 实例。
func MakePersister() *Persister {
	return &Persister{}
}

// clone 返回原始字节切片的一个副本。
func clone(orig []byte) []byte {
	x := make([]byte, len(orig))
	copy(x, orig)
	return x
}

// Checkpoint 返回当前 Persister 的快照副本。
func (ps *Persister) Checkpoint() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.raftstate = clone(ps.raftstate)
	np.snapshot = clone(ps.snapshot)
	return np
}

// ReadRaftState 返回持久化的 Raft 状态副本。
func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.raftstate)
}

// RaftStateSize 返回持久化的 Raft 状态的字节长度。
func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

// Save 以原子操作同时保存 Raft 状态和 K/V 快照，防止两者不同步。
func (ps *Persister) Save(raftstate []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = clone(raftstate)
	ps.snapshot = clone(snapshot)
}

// ReadSnapshot 返回持久化的快照副本。
func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.snapshot)
}

// SnapshotSize 返回持久化快照的字节长度。
func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}
