package raft

// 文件 ../raftapi/raftapi.go 定义了 raft 必须暴露给服务器（或测试器）的接口，
// 但请参阅下面每个函数的注释以获取更多细节。
//
// 另外，Make() 会创建一个实现 raft 接口的新 raft 节点。

import (
	//	"bytes"

	"bytes"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const (
	StateFollower = iota
	StateCandidate
	StateLeader
)

// log 条目
type LogEntry struct {
	Term    int
	Command interface{}
}

// 一个实现单个 Raft 节点的 Go 对象。
type Raft struct {
	mu sync.Mutex // 用于保护该节点状态的互斥锁
	// 下面需要加锁的有：currentTerm、votedFor、logs、
	// commitIndex、lastApplied、state、lastHeartbeat、
	// electionTimeout、nextIndex、matchIndex

	peers     []*labrpc.ClientEnd // 所有节点的 RPC 端点
	persister *tester.Persister   // 保存该节点持久化状态的对象
	me        int                 // 该节点在 peers[] 中的索引
	dead      int32               // 还活着吗

	// 所有节点的持久化状态（在崩溃和重启后必须恢复）
	currentTerm   int        // 该节点当前的任期
	votedFor      int        // 该节点在当前任期内投票给了哪个候选人
	logs          []LogEntry // 该节点的日志条目列表 3A还用不到
	snapshot      []byte     // 该节点的快照
	snapshotIndex int        // 快照包含最后一条日志的编号
	snapshotTerm  int        // 快照包含最后一条日志的任期

	// 所有节点的易失性状态（在崩溃和重启后不需要恢复）
	commitIndex     int           // 已知被提交的最高日志条目的索引
	lastApplied     int           // 已经被应用到状态机的最高日志条目的索引
	state           int           // 该节点当前的状态（StateFollower、StateCandidate 或 StateLeader）
	lastHeartbeat   time.Time     // 最近一次收到来自 leader 的心跳的时间
	electionTimeout time.Duration // 选举超时时间，随机在 300-600ms 之间

	// Leader 节点的易失性状态（在崩溃和重启后不需要恢复）
	nextIndex  []int // 对于每个服务器，发送给该服务器的下一个日志条目的索引
	matchIndex []int // 对于每个服务器，已知被复制到该服务器的最高日志条目的索引

	// 通道
	applyCh chan raftapi.ApplyMsg // 用于向服务发送 ApplyMsg 的通道
	// 阻塞锁
	applyCond *sync.Cond // 用于唤醒应用日志的线程

	// 你的数据结构在这里 (3A, 3B, 3C)。
	// 看论文的 Figure 2 来了解 Raft 服务器必须维护哪些状态。

}

// AppendRPC结构体，包括心跳和日志Append
type AppendEntriesRequest struct {
	Term         int // Leader的任期
	LeaderId     int // Leader的唯一标识
	PrevLogTerm  int // 上一条日志的任期
	PrevLogIndex int // 上一条日志的索引

	Entries []LogEntry // 3A 不需要

	LeaderCommit int // leader已经提交的索引
}

type AppendEntriesReply struct {
	Term          int  // Follower? 的任期
	Success       bool // 信息是否满足一致性
	ConflictTerm  int  // 冲突日志的任期
	ConflictIndex int  // 冲突日志的索引
}

type InstallSnapshotRequest struct {
	Term              int    // Leader的任期
	LeaderId          int    // Leader的唯一标识
	LastIncludedIndex int    // 快照里最后一条的逻辑编号
	LastIncludedTerm  int    // 快照里最后一条的任期
	Offset            int    // 快照的偏移
	Data              []byte // 快照内容
	Done              bool   // 是否是最后一块快照
}

type InstallSnapshotReply struct {
	Term int // Follower? 的任期
}

func (rf *Raft) LogicalToPhysical(index int) int {
	return index - rf.snapshotIndex
}

func (rf *Raft) PhysicalToLogical(index int) int {
	return index + rf.snapshotIndex
}

func (rf *Raft) GetLastLogIndex() int {
	return rf.PhysicalToLogical(len(rf.logs) - 1)
}

func (rf *Raft) GetLastLogTerm() int {
	return rf.logs[len(rf.logs)-1].Term
}

// 返回当前任期（currentTerm）以及该服务器是否认为自己是 leader。
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// 在此处实现你的代码 (3A)。
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == StateLeader
	return term, isleader
}

// 将 Raft 的持久化状态保存到稳定存储，
// 以便在崩溃和重启后可以恢复。
// 具体需要持久化哪些内容请参考论文 Figure 2。
// 在你实现快照（snapshot）之前，应该将 nil 作为 persister.Save() 的第二个参数。
// 实现快照后，传递当前快照（如果还没有快照则传 nil）。
func (rf *Raft) persist() {
	// 在此处实现你的代码 (3C)。
	// 示例：
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(&rf.currentTerm); err != nil {
		return
	}
	if err := e.Encode(&rf.votedFor); err != nil {
		return
	}
	if err := e.Encode(&rf.logs); err != nil {
		return
	}
	if err := e.Encode(&rf.snapshotIndex); err != nil {
		return
	}
	if err := e.Encode(&rf.snapshotTerm); err != nil {
		return
	}
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// 恢复之前持久化的状态。
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // 没有任何状态时引导启动？
		return
	}
	// 在此处实现你的代码 (3C)。
	// 示例：
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	var snapshotIndex int
	var snapshotTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&snapshotIndex) != nil ||
		d.Decode(&snapshotTerm) != nil {
		panic("server recover failed")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		if len(rf.logs) == 0 {
			rf.logs = []LogEntry{{Term: snapshotTerm}}
		}
		rf.snapshotIndex = snapshotIndex
		rf.snapshotTerm = snapshotTerm
		rf.logs[0].Term = snapshotTerm
		if rf.commitIndex < snapshotIndex {
			rf.commitIndex = snapshotIndex
		}
		if rf.lastApplied < snapshotIndex {
			rf.lastApplied = snapshotIndex
		}
		for i := range rf.peers {
			rf.nextIndex[i] = snapshotIndex + 1
		}
	}
}

// Raft 持久化日志占用多少字节？
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// 服务端通知已创建快照，包含了直到 index 的所有信息。
// 这意味着服务端不再需要 index 及之前的日志。
// Raft 现在应该尽可能地裁剪日志。
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// 在此处实现你的代码 (3D)。
	// 看index是否合法
	// index应该是逻辑编号
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index < rf.snapshotIndex {
		return
	}
	// 更新快照
	rf.snapshot = snapshot
	// 更新快照最后任期为index处log的任期
	rf.snapshotTerm = rf.logs[rf.LogicalToPhysical(index)].Term
	// 只保留index之后的内容
	newLen := rf.PhysicalToLogical(len(rf.logs)) - index
	newLog := make([]LogEntry, newLen)
	newLog[0] = LogEntry{
		Term:    rf.snapshotTerm,
		Command: nil,
	}
	copy(newLog[1:], rf.logs[rf.LogicalToPhysical(index+1):])
	rf.logs = newLog
	rf.snapshotIndex = index
	rf.persist()
}

// RequestVote RPC 参数结构体示例。
// 字段名必须以大写字母开头！
type RequestVoteArgs struct {
	// 在此处填写你的数据 (3A, 3B)。
	Term         int // 候选者的当前任期
	CandidateId  int // 候选者ID
	LastLogTerm  int // 候选者最后一条日志的任期
	LastLogIndex int // 候选者最后一条日志的编号
}

// RequestVote RPC 回复结构体示例。
// 字段名必须以大写字母开头！
type RequestVoteReply struct {
	// 在此处填写你的数据 (3A)。
	Term        int  // 投票者的当前任期
	VoteGranted bool // 是否投票同意
}

// RequestVote RPC 处理函数示例。
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// 在此处实现你的代码 (3A, 3B)。
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 有没有资格
	// 先看轮次

	reply.Term = rf.currentTerm
	rfChanged := false
	defer func() {
		if rfChanged {
			rf.persist() // RPC返回前统一持久化（批量优化）
		}
	}()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		rfChanged = true
	}
	reply.Term = rf.currentTerm

	// 执行到这，投票节点和候选节点一定在同一任期

	// 检查日志是否合法
	// 如果日志最后条目的任期落后，拒绝
	// lastLogIndex := len(rf.logs) - 1
	lastLogIndex := rf.GetLastLogIndex()
	// lastLogTerm := rf.logs[lastLogIndex].Term
	lastLogTerm := rf.GetLastLogTerm()
	if (lastLogTerm > args.LastLogTerm) || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
		reply.VoteGranted = false
		return
	}

	// 没投过票 或者 我是要头给你（一般是包丢失过）
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		rf.resetElectionTimeout() // 投票也要重置计时器，防止竞争
		rf.state = StateFollower
		reply.VoteGranted = true
		rfChanged = true
	} else {
		// 这一般是之前已经投给别人了
		reply.VoteGranted = false
	}

}

// 发送 RequestVote RPC 到某个服务器的示例代码。
// server 是目标服务器在 rf.peers[] 中的索引。
// args 是 RPC 参数。
// *reply 用于接收 RPC 回复，因此调用者应传递 &reply。
// 传递给 Call() 的参数和返回值类型必须与处理函数声明的类型一致（包括是否为指针）。
//
// labrpc 包模拟了一个不可靠的网络，服务器可能无法访问，
// 请求和回复也可能丢失。
// Call() 发送请求并等待回复。如果在超时时间内收到回复，Call() 返回 true；否则返回 false。
// 因此 Call() 可能会等待一段时间。
// 返回 false 可能是因为目标服务器宕机、无法访问、请求丢失或回复丢失。
//
// Call() 一定会返回（可能会延迟），*除非*服务器端的处理函数没有返回。
// 因此无需自己实现超时机制。
//
// 更多细节请参考 ../labrpc/labrpc.go 的注释。
//
// 如果 RPC 无法正常工作，请检查结构体字段名是否大写，
// 以及调用者是否用 & 传递 reply 结构体的地址，而不是结构体本身。
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// 使用 Raft 的服务（如 k/v 服务器）希望就下一个要追加到 Raft 日志的命令达成一致。
// 如果该服务器不是 leader，则返回 false。
// 如果是 leader，则开始达成一致并立即返回。
// 不能保证该命令一定会被提交到 Raft 日志，因为 leader 可能会失败或失去选举。
//
// 第一个返回值是该命令如果被提交后所在的索引。
// 第二个返回值是当前任期。
// 第三个返回值是该服务器是否认为自己是 leader。
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// 在此处实现你的代码 (3B)。
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isLeader = rf.state == StateLeader

	if !isLeader {
		return -1, term, isLeader
	}

	entry := LogEntry{
		Term:    term,
		Command: command,
	}
	rf.logs = append(rf.logs, entry)
	index = rf.PhysicalToLogical(len(rf.logs) - 1)
	// 在触发心跳（发送RPC）前持久化
	rf.persist()

	go rf.HeartBeats()

	return index, term, isLeader
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// 在此处实现你的代码 (3A)
		// 检查是否应该发起 leader 选举。
		rf.mu.Lock()
		switch rf.state {
		// 两种身份的人会执行不同的逻辑，Sleep不同的时间
		case StateFollower, StateCandidate:
			if rf.state != StateLeader && time.Since(rf.lastHeartbeat) > rf.electionTimeout {
				go rf.startElection()
				rf.resetElectionTimeout()
			}
			rf.mu.Unlock()
			ms := 50 + (rand.Int63() % 50)
			time.Sleep(time.Duration(ms) * time.Millisecond)

		case StateLeader:
			rf.mu.Unlock()
			rf.HeartBeats()

			ms := 100 + (rand.Int63() % 20)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

	}
}

// 服务或测试器希望创建一个 Raft 服务器。
// 所有 Raft 服务器（包括本服务器）的端口都在 peers[] 中。
// 本服务器的端口是 peers[me]。
// 所有服务器的 peers[] 数组顺序一致。
// persister 用于保存本服务器的持久化状态，
// 并在初始化时持有最近保存的状态（如果有）。
// applyCh 是一个通道，测试器或服务期望 Raft 通过该通道发送 ApplyMsg 消息。
// Make() 必须快速返回，因此应为任何长时间运行的任务启动 goroutine。
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = StateFollower
	rf.resetElectionTimeout()
	rf.logs = make([]LogEntry, 0)
	rf.logs = append(rf.logs, LogEntry{
		Term:    0,
		Command: nil, // 哨兵不包含任何指令
	})

	rf.snapshotIndex = 0
	rf.snapshotTerm = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	//rf.readPersist(nil)
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}
