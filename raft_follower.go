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
