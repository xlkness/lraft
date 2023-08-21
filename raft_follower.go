package lraft

import (
	"google.golang.org/protobuf/proto"
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

func (rf *raftFollower) handleEvent(msgID message.MessageID, payload proto.Message) {
	r := (*raft)(rf)
	r.handleEventCommon(msgID, payload)

	switch msgID {
	case message.MessageID_MsgAppendEntriesReq:
		// 收到心跳，leader还活着，将超时选举定时器重置
		rf.resetCanElectionTimer()

		// 追加日志
		request := payload.(*message.AppendEntriesReq)

		reject := false
		if request.Term < rf.term.term {
			reject = true
		} else if rf.entries.FindTerm(request.PrevLogIndex) != request.PrevLogTerm {
			reject = true
		}

		if reject {
			r.send(request.LeaderID, message.MessageID_MsgAppendEntriesRes, &message.AppendEntriesRes{
				Success: reject,
			})
			return
		}

		if len(request.Entries) == 0 {
			// 比对leader的应用索引，将进度追平
			rf.entries.ApplyToIndex(r.state(), request.LeaderAppliedIndex)
		} else {
			// 追加条目
			rf.entries.FollowerAppendEntries(request.PrevLogTerm, request.PrevLogIndex, request.Entries)
		}
		r.send(request.LeaderID, message.MessageID_MsgAppendEntriesRes, &message.AppendEntriesRes{
			Success: true,
		})
	case message.MessageID_MsgRequestPreVoteReq, message.MessageID_MsgRequestVoteReq:
		rf.resetCanElectionTimer()

		request := payload.(*message.RequestVoteReq)
		if rf.votedFor != None && rf.votedFor != request.CandidateID {
			r.send(request.CandidateID, message.MessageID_MsgRequestPreVoteRes, &message.RequestVoteRes{VoteGranted: false})
		} else if !rf.entries.IsUpToDate(request.LastLogIndex, request.LastLogTerm) {
			r.send(request.CandidateID, message.MessageID_MsgRequestPreVoteRes, &message.RequestVoteRes{VoteGranted: false})
		} else {
			r.votedFor = request.CandidateID
			r.send(request.CandidateID, message.MessageID_MsgRequestPreVoteRes, &message.RequestVoteRes{VoteGranted: true})
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
