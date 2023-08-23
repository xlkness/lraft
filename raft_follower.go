package lraft

import (
	"log"
	"lraft/message"
	"lraft/statem"
	"lraft/utils"
)

const followerCanElectionTimerKey = "election.timeout.timer"

type raftFollower raft

func (rf *raftFollower) become(state *statem.StateData) {
	log.Printf("node[%v] become follower", rf.id)
	rf.startCanElectionTimer()
}

func (rf *raftFollower) exit(state *statem.StateData) {
	log.Printf("node[%v] exit follower", rf.id)
}

func (rf *raftFollower) handleEvent(msg *message.Message) {
	r := (*raft)(rf)

	switch msg.MsgID {
	case message.MessageID_MsgAppendEntriesReq:
		if msg.Term == 0 {
			r.send(rf.leader, *msg)
			return
		}
		if msg.Term < rf.term {
			r.send(msg.From, message.Message{
				MsgID:  message.MessageID_MsgAppendEntriesRes,
				Reject: true,
			})
			return
		}

		// 收到心跳，leader还活着，将超时选举定时器重置
		rf.resetCanElectionTimer()

		// 设置leader
		rf.term = msg.Term
		rf.leader = msg.From

		// 追加日志
		if rf.entries.FindTerm(msg.LastLogIndex) != msg.LastLogTerm {
			r.send(msg.From, message.Message{
				MsgID:  message.MessageID_MsgAppendEntriesRes,
				Reject: true,
			})
			return
		}

		if len(msg.AppendEntriesReq.Entries) == 0 {
			utils.Assert(msg.LastLogIndex > rf.entries.LastIndex(), true)
			// 心跳消息，比对leader的应用索引，将进度追平
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
	tick := rf.config.CanElectionTimerTick + rf.rander.Intn(rf.config.CanElectionTimerTick)
	r.stateMachine.InsertKeyValueTimeout(&statem.KVTimeout{
		Key:         followerCanElectionTimerKey,
		Value:       nil,
		TimeoutTick: tick,
		Callback: func(key, value any) {
			log.Printf("start election")
			// 超时发起选举
			r.becomeCandidate()
		},
	})
}

func (rf *raftFollower) resetCanElectionTimer() {
	tick := rf.config.CanElectionTimerTick + rf.rander.Intn(rf.config.CanElectionTimerTick)
	rf.stateMachine.ResetKeValueTimer(followerCanElectionTimerKey, tick)
}
