package lraft

import (
	"log"
	"lraft/message"
	"lraft/statem"
)

const candidatePreVoteTimeoutTimerKey = "prevote.timeout.timer"
const candidateVoteTimeoutTimerKey = "vote.timeout.timer"

type raftCandidate raft

func (rc *raftCandidate) become(state *statem.StateData) {
	log.Printf("node[%v] become candidate", rc.id)
	rc.startPreElection()
}

func (rc *raftCandidate) exit(state *statem.StateData) {
	log.Printf("node[%v] exit candidate", rc.id)
	(*raft)(rc).resetData()
	rc.stopPreVoteTimeoutTimer()
	rc.stopVoteTimeoutTimer()
}

func (rc *raftCandidate) handleEvent(msg *message.Message) {
	r := (*raft)(rc)

	switch msg.MsgID {
	case message.MessageID_MsgAppendEntriesReq:
		if msg.Term < rc.term {
			r.send(msg.From, message.Message{
				MsgID:  message.MessageID_MsgAppendEntriesRes,
				Reject: true,
			})
			return
		}
		r.becomeFollower(msg.Term, msg.From)
		r.send(rc.id, *msg)
	case message.MessageID_MsgRequestPreVoteRes:
		// 统计半数，达到就广播投票
		rc.pollResult(msg, rc.startElection)
	case message.MessageID_MsgRequestVoteRes:
		// 统计半数，达到就竞选成功
		rc.pollResult(msg, r.becomeLeader)
	}
}

func (rc *raftCandidate) pollResult(msg *message.Message, action func()) (finish bool) {
	r := (*raft)(rc)
	_, _, result := r.poll(msg.From, !msg.Reject)
	switch result {
	case pollResult_Win:
		action()
		return true
	case pollResult_Lose:
		r.becomeFollower(r.term, None)
		return true
	case pollResult_Voting:
		// wait
	}
	return false
}

func (rc *raftCandidate) startPreElection() {
	r := (*raft)(rc)

	// 任期+1
	r.term = rc.term + 1

	// 给自己投票
	r.send(rc.id, message.Message{MsgID: message.MessageID_MsgRequestPreVoteRes, Reject: false})

	// 广播预投票，预投票消息相当于一个探测集群半数，否则当前节点如果单独网络分区会无限竞选
	r.broadcast(message.Message{
		MsgID: message.MessageID_MsgRequestPreVoteReq,
	})

	// 开启选举超时定时器
	rc.startPreVoteTimeoutTimer()
}

func (rc *raftCandidate) startElection() {
	r := (*raft)(rc)

	clear(r.peerVotes)
	rc.stopPreVoteTimeoutTimer()

	// 给自己投票
	r.send(rc.id, message.Message{MsgID: message.MessageID_MsgRequestVoteRes, Reject: false})

	// 开启选举超时定时器
	// 广播预投票，预投票消息相当于一个探测集群半数，否则当前节点如果单独网络分区会无限竞选
	r.broadcast(message.Message{
		MsgID: message.MessageID_MsgRequestVoteReq,
	})

	rc.startVoteTimeoutTimer()
}

func (rc *raftCandidate) startPreVoteTimeoutTimer() {
	r := (*raft)(rc)
	rc.stateMachine.InsertKeyValueTimeout(&statem.KVTimeout{
		Key:         candidatePreVoteTimeoutTimerKey,
		Value:       nil,
		TimeoutTick: rc.config.RequestVoteTimeoutTick,
		Callback: func(key, value any) {
			// 选举超时，变为follower
			r.becomeFollower(r.term, None)
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
		TimeoutTick: rc.config.RequestVoteTimeoutTick,
		Callback: func(key, value any) {
			// 选举超时，变为follower
			r.becomeFollower(r.term, None)
		},
	})
}

func (rc *raftCandidate) stopVoteTimeoutTimer() {
	rc.stateMachine.DelKeyValueTimer(candidateVoteTimeoutTimerKey)
}
