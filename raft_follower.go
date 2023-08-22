package lraft

import (
	"lraft/message"
	"lraft/statem"
	"math/rand"
)

const followerCanElectionTimerKey = "election.timeout.timer"
const followerCanElectionTimerTick = 2

type raftFollower raft

func (rf *raftFollower) become(state *statem.StateData) {
	rf.startCanElectionTimer()
}

func (rf *raftFollower) exit(state *statem.StateData) {

}

func (rf *raftFollower) handleEvent(msg *message.Message) {
	r := (*raft)(rf)

	switch msg.MsgID {
	case message.MessageID_MsgAppendEntriesReq:
		if msg.Term < rf.term {
			r.send(msg.From, message.Message{
				MsgID:  message.MessageID_MsgAppendEntriesRes,
				Reject: true,
			})
			return
		}

		// 设置leader
		rf.term = msg.Term
		rf.leader = msg.From

		// 收到心跳，leader还活着，将超时选举定时器重置
		rf.resetCanElectionTimer()

		// 追加日志
		reject := false
		if rf.entries.FindTerm(msg.LastLogIndex) != msg.LastLogTerm {
			reject = true
		}

		if reject {
			r.send(msg.From, message.Message{
				MsgID:  message.MessageID_MsgAppendEntriesRes,
				Reject: true,
			})
			return
		}

		if len(msg.AppendEntriesReq.Entries) == 0 {
			// 比对leader的应用索引，将进度追平
			rf.entries.ApplyToIndex(r.state(), msg.LastLogIndex)
		} else {
			// 追加条目
			rf.entries.FollowerAppendEntries(msg.LastLogTerm, msg.LastLogIndex, msg.AppendEntriesReq.Entries)
		}
		r.send(msg.From, message.Message{
			MsgID:  message.MessageID_MsgAppendEntriesRes,
			Reject: false,
		})
	case message.MessageID_MsgRequestPreVoteReq, message.MessageID_MsgRequestVoteReq:
		resMsgID := message.MessageID_MsgRequestPreVoteRes
		if msg.MsgID == message.MessageID_MsgRequestVoteReq {
			resMsgID = message.MessageID_MsgRequestVoteRes
		}
		if msg.Term < rf.term {
			r.send(msg.From, message.Message{
				MsgID:  resMsgID,
				Reject: true,
			})
			return
		}

		rf.resetCanElectionTimer()

		if rf.votedFor != None && rf.votedFor != msg.From {
			r.send(msg.From, message.Message{
				MsgID:  resMsgID,
				Reject: true,
			})
		} else if !rf.entries.IsUpToDate(msg.LastLogIndex, msg.LastLogTerm) {
			r.send(msg.From, message.Message{
				MsgID:  resMsgID,
				Reject: true,
			})
		} else {
			r.votedFor = msg.From
			r.send(msg.From, message.Message{
				MsgID:  resMsgID,
				Reject: false,
			})
		}
	}
}

func (rf *raftFollower) startCanElectionTimer() {
	r := (*raft)(rf)
	tick := followerCanElectionTimerTick + rand.Intn(followerCanElectionTimerTick)
	r.stateMachine.InsertKeyValueTimeout(&statem.KVTimeout{
		Key:         followerCanElectionTimerKey,
		Value:       nil,
		TimeoutTick: tick,
		Callback: func(key, value any) {
			// 超时发起选举
			r.becomeCandidate()
		},
	})
}

func (rf *raftFollower) resetCanElectionTimer() {
	tick := followerCanElectionTimerTick + rand.Intn(followerCanElectionTimerTick)
	rf.stateMachine.ResetKeValueTimer(followerCanElectionTimerKey, tick)
}
