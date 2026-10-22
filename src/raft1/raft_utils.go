package raft

import (
	"math/rand"
	"sort"
	"sync/atomic"
	"time"

	"6.5840/raftapi"
)

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.mu.Lock()
	rf.applyCond.Broadcast()
	rf.mu.Unlock()
	close(rf.applyCh)
}

// 重置随机选举超时
func (rf *Raft) resetElectionTimeout() {
	rf.lastHeartbeat = time.Now()
	rf.electionTimeout = time.Duration(1000+rand.Intn(400)) * time.Millisecond
}

func (rf *Raft) killed() bool {
	// 原子查看进程是否被kill了
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 发起一次新的选举，尝试成为候选人。
func (rf *Raft) startElection() {
	rf.mu.Lock()
	// 身份转变为候选人
	rf.becomeCandidate()
	// 先存一个额外的currentTerm，以防在拉票过程中currentTerm发生变化(原Leader心跳)，导致选举失败。
	term := rf.currentTerm
	// lastLogIndex := len(rf.logs) - 1
	// lastLogTerm := rf.logs[lastLogIndex].Term
	lastLogIndex := rf.GetLastLogIndex()
	lastLogTerm := rf.GetLastLogTerm()
	// 在发送RPC前持久化（批量优化）
	rf.persist()
	rf.mu.Unlock()
	votes := 1

	// 开始拉票
	for server := range rf.peers {
		if server == rf.me {
			// 自己给自己投票
			continue
		}

		go func(server int) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex, // 逻辑编号
				LastLogTerm:  lastLogTerm,
			}
			reply := RequestVoteReply{}
			if ok := rf.sendRequestVote(server, &args, &reply); !ok {
				// RPC失败
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.state != StateCandidate || rf.currentTerm != term {
				// 已经失去身份
				return
			}

			if reply.Term > rf.currentTerm {
				// 取消参选者资格
				rf.becomeFollower(reply.Term)
				rf.persist() // RPC回复处理中需要持久化
				return
			}

			if reply.VoteGranted == true {
				// 收到赞成票
				votes++
				if votes > len(rf.peers)/2 {
					rf.becomeLeader()
				}
			}

		}(server)
	}
}

// 转为Leader
func (rf *Raft) becomeLeader() {
	rf.state = StateLeader
	for i := range rf.peers {
		rf.nextIndex[i] = rf.PhysicalToLogical(len(rf.logs))
		// 【修改】尚未确认 Follower 已安装本机快照，不能把快照边界当作匹配进度。
		// 旧代码：rf.matchIndex[i] = rf.PhysicalToLogical(0)
		rf.matchIndex[i] = 0
	}
	// fmt.Println(rf.me, " 成为了leader")
	rf.HeartBeats()

}

// 转为Follower
func (rf *Raft) becomeFollower(term int) {
	rf.state = StateFollower
	rf.currentTerm = term
	rf.votedFor = -1
}

// 转为Candidate
func (rf *Raft) becomeCandidate() {
	rf.state = StateCandidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimeout()
}

// 接收方处理（心跳或日志）
func (rf *Raft) AppendEntries(args *AppendEntriesRequest, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	needPersist := false
	defer func() {
		if needPersist {
			rf.persist()
		}
	}()

	// 先进行任期检查
	// 如果收到的任期低于自己，拒绝
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// 任期大于自己，追上任期
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		needPersist = true
	}

	// 此时任期一定相等了

	reply.Term = rf.currentTerm
	rf.state = StateFollower

	// 进行日志安全性检查
	// lastLogIndex := len(rf.logs) - 1
	lastLogIndex := rf.GetLastLogIndex()
	// follower有的太少了，无法同步，拒绝
	if args.PrevLogIndex > lastLogIndex {
		reply.Success = false
		reply.ConflictIndex = rf.PhysicalToLogical(len(rf.logs))
		reply.ConflictTerm = -1
		return
	}
	// 发的至少已经有一个在我的快照里了
	if args.PrevLogIndex < rf.snapshotIndex {
		reply.Success = false
		reply.ConflictIndex = rf.snapshotIndex + 1
		reply.ConflictTerm = -1
		return
	}
	// PrevLogIndex处的Term不匹配，拒绝 args.PrevLogIndex >= 0 &&
	// if args.PrevLogTerm != rf.logs[rf.LogicalToPhysical(args.PrevLogIndex)].Term {
	// 	reply.Success = false
	// 	reply.ConflictTerm = rf.logs[rf.LogicalToPhysical(args.PrevLogIndex)].Term
	// 	reply.ConflictIndex = args.PrevLogIndex
	// 	for i := args.PrevLogIndex - 1; i > rf.snapshotIndex; i-- {
	// 		if rf.logs[rf.LogicalToPhysical(i)].Term == reply.ConflictTerm {
	// 			reply.ConflictIndex = i
	// 		} else {
	// 			break
	// 		}
	// 	}
	// 	return
	// }
	prev := rf.LogicalToPhysical(args.PrevLogIndex)
	conflictTerm := rf.logs[prev].Term

	if args.PrevLogTerm != conflictTerm {
		reply.Success = false
		reply.ConflictTerm = conflictTerm

		// 在物理下标 [0, prev] 中找第一个 Term >= conflictTerm 的位置。
		left, right := 0, prev
		for left < right {
			mid := left + (right-left)/2
			if rf.logs[mid].Term < conflictTerm {
				left = mid + 1
			} else {
				right = mid
			}
		}

		reply.ConflictIndex = rf.PhysicalToLogical(left)
		return
	}

	rf.resetElectionTimeout() // 只要我们不拒绝这个leader，我们就重置计时器
	// 从prevLogIndex开始，跳过Term相等的条目
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx < rf.PhysicalToLogical(len(rf.logs)) {
			if rf.logs[rf.LogicalToPhysical(idx)].Term != entry.Term {
				rf.logs = append(rf.logs[:rf.LogicalToPhysical(idx)], args.Entries[i:]...)
				needPersist = true
				break
			}
			// Term 相同则跳过，防止旧包覆盖新日志
		} else {
			rf.logs = append(rf.logs, args.Entries[i:]...)
			if len(args.Entries[i:]) > 0 {
				needPersist = true
			}
			break
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		// lastNewEntry是现在logs最新的index
		lastNewEntry := args.PrevLogIndex + len(args.Entries)
		newCommitIndex := min(args.LeaderCommit, lastNewEntry)
		if newCommitIndex > rf.commitIndex {
			rf.commitIndex = newCommitIndex
			rf.applyCond.Broadcast()
		}
	}

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesRequest, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) HeartBeats() {
	for server := range rf.peers {
		if server == rf.me {
			// 不给自己发心跳
			continue
		}

		go rf.replicateToFollower(server)
	}
}

func (rf *Raft) makeAppendEntriesArgs(peerIndex int) AppendEntriesRequest {
	// 调用此函数时，外部必须已经持有rf.mu.Lock()

	// 逻辑编号
	prevLogIndex := rf.nextIndex[peerIndex] - 1

	// 确定要发送哪些日志
	var entries []LogEntry
	if rf.PhysicalToLogical(len(rf.logs)) > rf.nextIndex[peerIndex] {
		actualEntries := rf.logs[rf.LogicalToPhysical(rf.nextIndex[peerIndex]):]
		entries = make([]LogEntry, len(actualEntries))
		copy(entries, actualEntries)
	}

	return AppendEntriesRequest{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.logs[rf.LogicalToPhysical(prevLogIndex)].Term, // 假设logs[0]是哨兵
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
}

func (rf *Raft) snapshotToFollower(server int) {
	// 进来之前一定带锁
	args := InstallSnapshotRequest{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.snapshotIndex,
		LastIncludedTerm:  rf.snapshotTerm,
		Offset:            0,
		Data:              rf.snapshot,
		Done:              true,
	}
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()
	ok := rf.sendInstallSnapshot(server, &args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		// 【修改】先处理更高任期，过期请求的回复也可能携带新的任期。
		// 旧代码：身份检查原本在任期检查之前。
		// if rf.state != StateLeader || rf.currentTerm != args.Term {
		//     return
		// }
		// 任期检查
		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			rf.persist() // RPC回复处理中需要持久化
			return
		}
		// 【修改】旧任期的复制结果不能修改当前任期的复制进度。
		if rf.state != StateLeader || rf.currentTerm != args.Term {
			return
		}
		// 【修改】快照回复可能晚于日志回复，两个进度都只能向前推进。
		// 旧代码：
		// rf.nextIndex[server] = args.LastIncludedIndex + 1
		// if rf.matchIndex[server] < args.LastIncludedIndex {
		//     rf.matchIndex[server] = args.LastIncludedIndex
		// }
		rf.matchIndex[server] = max(rf.matchIndex[server], args.LastIncludedIndex)
		rf.nextIndex[server] = max(rf.nextIndex[server], rf.matchIndex[server]+1)
	}
}

func (rf *Raft) entriesToFollower(server int) {
	// 进来一定带锁了
	args := rf.makeAppendEntriesArgs(server)
	rf.mu.Unlock()

	reply := AppendEntriesReply{}
	if ok := rf.sendAppendEntries(server, &args, &reply); ok {

		// 发送日志成功处理
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// 【修改】先检查更高任期，再判断回复是否属于当前 Leader 任期。
		// 旧代码：
		// if rf.state != StateLeader || rf.currentTerm != args.Term {
		//     return
		// }
		// 任期检查
		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			rf.persist() // RPC回复处理中需要持久化
			return
		}
		// 【修改】更高任期处理完后，才过滤身份或任期已经过期的回复。
		if rf.state != StateLeader || rf.currentTerm != args.Term {
			return
		}
		// 失败处理
		if !reply.Success {
			// 【修改】已确认匹配的位置无需回退；探测位置已变化的失败回复也过期了。
			if args.PrevLogIndex <= rf.matchIndex[server] ||
				args.PrevLogIndex+1 != rf.nextIndex[server] {
				return
			}
			// 【修改】先计算候选位置，最后统一限制在已确认匹配的位置之后。
			conflictNext := reply.ConflictIndex
			if reply.ConflictTerm == -1 {
				// 任期没冲突，说明Follower的日志太短了，从follower的结尾开始发送
				// 旧代码：rf.nextIndex[server] = reply.ConflictIndex
				conflictNext = reply.ConflictIndex
			} else {
				// 任期也冲突了，看看自己之前有没有这个任期
				lastIndexWithTerm := -1
				for i := len(rf.logs) - 1; i >= 0; i-- {
					if rf.logs[i].Term == reply.ConflictTerm {
						lastIndexWithTerm = rf.PhysicalToLogical(i)
						break
					}
				}
				if lastIndexWithTerm == -1 {
					// Leader没有这个任期，直接从conflictIndex开始，因为这个任期都要被覆盖
					// 旧代码：rf.nextIndex[server] = reply.ConflictIndex
					conflictNext = reply.ConflictIndex
				} else {
					// Leader有这个任期，从该任期的下一个开始对其试试
					// 旧代码：rf.nextIndex[server] = lastIndexWithTerm + 1
					conflictNext = lastIndexWithTerm + 1
				}
			}
			// 【修改】不能回退到已确认匹配的日志以内。
			rf.nextIndex[server] = max(rf.matchIndex[server]+1, conflictNext)
		} else {
			// 成功处理
			newMatchIndex := args.PrevLogIndex + len(args.Entries)
			// 【修改】并行成功回复乱序到达时，进度只能增加。
			// 旧代码：
			// if newMatchIndex > rf.matchIndex[server] {
			//     rf.matchIndex[server] = newMatchIndex
			//     rf.nextIndex[server] = rf.matchIndex[server] + 1
			// }
			rf.matchIndex[server] = max(rf.matchIndex[server], newMatchIndex)
			rf.nextIndex[server] = max(rf.nextIndex[server], rf.matchIndex[server]+1)

			// 更新commitIndex
			// 复制一份当前的matchIndex并排序
			indices := make([]int, len(rf.peers))
			copy(indices, rf.matchIndex)
			indices[rf.me] = rf.GetLastLogIndex()
			// rf.PhysicalToLogical(len(rf.logs) - 1)
			sort.Ints(indices)

			// 取中位数
			n := len(rf.peers)
			newCommitIndex := indices[(n-1)/2]

			// 只允许提交当前任期的日志
			if newCommitIndex > rf.commitIndex && rf.logs[rf.LogicalToPhysical(newCommitIndex)].Term == rf.currentTerm {
				rf.commitIndex = newCommitIndex
				rf.applyCond.Broadcast()
			}

		}
	}
}

func (rf *Raft) replicateToFollower(server int) {
	rf.mu.Lock()
	if rf.state != StateLeader {
		// 失去身份了
		rf.mu.Unlock()
		return
	}

	// 判断是否需要发快照
	if rf.nextIndex[server] <= rf.snapshotIndex {
		rf.snapshotToFollower(server)

	} else {
		rf.entriesToFollower(server)
	}
}

// 处理安装快照的RPC请求
func (rf *Raft) InstallSnapshot(args *InstallSnapshotRequest, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	// 任期比我低
	if args.Term < rf.currentTerm {
		return
	}

	// 快照过期
	if args.LastIncludedIndex <= rf.snapshotIndex {
		return
	}

	// 任期比我高
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	} else {
		// 同任期收到合法快照，也应转为Follower
		rf.state = StateFollower
	}
	rf.resetElectionTimeout()
	reply.Term = rf.currentTerm

	// 先不考虑分片
	rf.snapshot = args.Data
	// rf.snapshot = append([]byte(nil), args.Data...)
	// 获取本地日志的最后一个index和term，看看快照和本地日志是否有重叠
	lastLogIndex := rf.GetLastLogIndex()
	// 如果快照的最后一个index在本地日志范围内，并且term也匹配，
	// 			说明快照和本地日志有重叠，可以保留快照之后的日志；
	// 否则说明快照和本地日志没有重叠，直接丢弃本地日志。
	if args.LastIncludedIndex <= lastLogIndex &&
		rf.logs[rf.LogicalToPhysical(args.LastIncludedIndex)].Term == args.LastIncludedTerm {
		// 与本地日志匹配：保留该条目之后的后缀日志
		suffixLen := lastLogIndex - args.LastIncludedIndex
		newLogs := make([]LogEntry, 1+suffixLen)
		newLogs[0] = LogEntry{Term: args.LastIncludedTerm}
		copy(newLogs[1:], rf.logs[rf.LogicalToPhysical(args.LastIncludedIndex+1):])
		rf.logs = newLogs
	} else {
		// 不匹配，丢弃整段日志，仅保留快照哨兵
		rf.logs = []LogEntry{{Term: args.LastIncludedTerm}}
	}
	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshotTerm = args.LastIncludedTerm

	// 推进commitIndex和lastApplied，防止applier访问已被裁剪的日志
	if args.LastIncludedIndex > rf.commitIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	needApplySnapshot := args.LastIncludedIndex > rf.lastApplied
	rf.persist()

	// 仅在快照进度领先于已应用进度时触发快照下发，避免回退 lastApplied。
	if needApplySnapshot {
		rf.applyCond.Broadcast()
	}

}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotRequest, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		snapshotIndex := rf.snapshotIndex
		if snapshotIndex > rf.lastApplied {
			msg := raftapi.ApplyMsg{
				CommandValid:  false,
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotTerm:  rf.snapshotTerm,
				SnapshotIndex: rf.snapshotIndex,
			}
			rf.mu.Unlock()

			rf.applyCh <- msg
			rf.mu.Lock()
			if snapshotIndex > rf.lastApplied {
				rf.lastApplied = snapshotIndex
				if snapshotIndex > rf.commitIndex {
					rf.commitIndex = snapshotIndex
				}

			}
			rf.mu.Unlock()
		} else if rf.lastApplied < rf.commitIndex {

			commandIndex := rf.lastApplied + 1
			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[rf.LogicalToPhysical(commandIndex)].Command,
				CommandIndex: commandIndex,
			}
			rf.mu.Unlock()
			rf.applyCh <- msg

			rf.mu.Lock()
			if commandIndex >= rf.lastApplied+1 {
				rf.lastApplied = commandIndex
			}
			rf.mu.Unlock()
		} else {
			rf.applyCond.Wait()
			rf.mu.Unlock()
		}
	}
}
