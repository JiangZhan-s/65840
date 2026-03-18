package raftapi

// Raft 接口
type Raft interface {
	// 启动对新日志条目的共识达成，并返回该条目的日志索引、
	// 任期，以及该节点是否为领导者。
	Start(command interface{}) (int, int, bool)

	// 询问 Raft 节点的当前任期，以及它是否认为自己是
	// 领导者
	GetState() (int, bool)

	// 用于快照 (3D)
	Snapshot(index int, snapshot []byte)
	PersistBytes() int
}

// 当每个 Raft 节点意识到连续的日志条目被提交时，该节点
// 应该通过传递给 Make() 的 applyCh 向服务（或测试程序）
// 发送一个 ApplyMsg。将 CommandValid 设置为 true
// 以表明该 ApplyMsg 包含一个新提交的日志条目。
//
// 你会在稍后的实验中发现 Snapshot 相关的字段很有用。
// CommandValid 和 SnapshotValid 中必须且只能有一个为 true。
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
