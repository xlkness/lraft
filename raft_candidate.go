package lraft

import (
	"google.golang.org/protobuf/proto"
	"lraft/message"
	"lraft/statem"
)

const candidatePreVoteTimeoutTimerKey = "prevote.timeout.timer"
const candidateVoteTimeoutTimerKey = "vote.timeout.timer"
const candidateRequestVoteTimeoutTick = 2

type raftCandidate raft

func (rc *raftCandidate) become(state *statem.StateData) {
	rc.startPreElection()
}

func (rc *raftCandidate) exit(state *statem.StateData) {
	rc.stopPreVoteTimeoutTimer()
	rc.stopVoteTimeoutTimer()
}

func (rc *raftCandidate) handleEvent(msgID message.MessageID, payload proto.Message) {
	r := (*raft)(rc)
	if !r.handleEventCommon(msgID, payload) {
		return
	}

	switch msgID {
	case message.MessageID_MsgAppendEntriesReq:
	case message.MessageID_MsgRequestPreVoteRes:
		msg := payload.(*message.RequestVoteRes)
		// 统计半数，达到就广播投票
		_, _, result := r.poll(msg.FromID, msg.VoteGranted)
		switch result {
		case pollResult_Win:
			rc.startElection()
		case pollResult_Lose:
			r.becomeFollower(r.term.term, None)
		case pollResult_Voting:
			// wait
		}
	case message.MessageID_MsgRequestVoteRes:
		msg := payload.(*message.RequestVoteRes)
		// 统计半数，达到就竞选成功
		_, _, result := r.poll(msg.FromID, msg.VoteGranted)
		switch result {
		case pollResult_Win:
			r.becomeLeader()
		case pollResult_Lose:
			r.becomeFollower(r.term.term, None)
		case pollResult_Voting:
			// wait
		}
	}
}

func (rc *raftCandidate) startPreElection() {
	r := (*raft)(rc)

	// 任期+1
	r.term.term = rc.term.term + 1

	// 给自己投票
	// 开启选举超时定时器
	// 广播预投票，预投票消息相当于一个探测集群半数，否则当前节点如果单独网络分区会无限竞选
	r.broadcast(message.MessageID_MsgRequestPreVoteReq, nil)
	rc.startPreVoteTimeoutTimer()
}

func (rc *raftCandidate) startElection() {
	r := (*raft)(rc)

	clear(r.peerVotes)
	rc.stopPreVoteTimeoutTimer()

	// 给自己投票
	// 开启选举超时定时器
	// 广播预投票，预投票消息相当于一个探测集群半数，否则当前节点如果单独网络分区会无限竞选
	r.broadcast(message.MessageID_MsgRequestVoteReq, nil)

	rc.startVoteTimeoutTimer()
}

func (rc *raftCandidate) startPreVoteTimeoutTimer() {
	r := (*raft)(rc)
	rc.stateMachine.InsertKeyValueTimeout(&statem.KVTimeout{
		Key:         candidatePreVoteTimeoutTimerKey,
		Value:       nil,
		TimeoutTick: candidateRequestVoteTimeoutTick,
		Callback: func(key, value any) {
			// 选举超时，变为follower
			r.becomeFollower(r.term.term, None)
		},
	})
}

func (rc *raftCandidate) stopPreVoteTimeoutTimer() {
	rc.stateMachine.DelKeyValueTimer(candidatePreVoteTimeoutTimerKey)
}

func (rc *raftCandidate) startVoteTimeoutTimer() {
	r := (*raft)(rc)
	rc.stateMachine.InsertKeyValueTimeout(&statem.KVTimeout{
		Key:         candidateVoteTimeoutTimerKey,
		Value:       nil,
		TimeoutTick: candidateRequestVoteTimeoutTick,
		Callback: func(key, value any) {
			// 选举超时，变为follower
			r.becomeFollower(r.term.term, None)
		},
	})
}

func (rc *raftCandidate) stopVoteTimeoutTimer() {
	rc.stateMachine.DelKeyValueTimer(candidateVoteTimeoutTimerKey)
}
