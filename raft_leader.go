package lraft

import (
	"google.golang.org/protobuf/proto"
	"lraft/message"
	"lraft/statem"
)

const leaderBroadcastHeartBeatTimerKey = "broadcast.heartbeat"
const leaderBroadcastHeartBeatTimerTick = 2

type raftLeader raft

func (rl *raftLeader) become(state *statem.StateData) {
	r := (*raft)(rl)

	// 广播一条空操作的日志
	r.broadcast(message.MessageID_MsgAppendEntriesReq, nil)
}

func (rl *raftLeader) exit(state *statem.StateData) {

}

func (rl *raftLeader) handleEvent(msgID message.MessageID, payload proto.Message) {
	r := (*raft)(rl)
	if !r.handleEventCommon(msgID, payload) {
		return
	}

	switch msgID {
	case message.MessageID_MsgAppendEntriesReq:
		// 追加到本地记录
		// 广播
		r.broadcast(message.MessageID_MsgAppendEntriesReq, payload)
	case message.MessageID_MsgAppendEntriesRes:
		msg := payload.(*message.AppendEntriesRes)
		if msg.Success {
			quorumIndex := r.peers.calcQuorumLogProgress()
			rl.log.applyToIndex(r.state(), quorumIndex)
		} else {
			rl.sendAppend(msg.From)
		}
	}
}

func (rl *raftLeader) sendAppend(to int64) {
	toPeer := rl.peers.findPeer(to)
	msg := &message.AppendEntriesReq{
		Term:         rl.term.term,
		LeaderID:     rl.id,
		PrevLogIndex: rl.log.lastIndex(),
		PrevLogTerm:  rl.log.findTerm(rl.log.lastIndex()),
		Entries:      nil,
	}

	rl.transporter.Send(to, message.MessageID_MsgAppendEntriesReq, msg)
}

func (rl *raftLeader) startBroadcastHeartbeatTimer() {
	r := (*raft)(rl)
	r.stateMachine.InsertKeyValueTimeout(&statem.KVTimeout{
		Key:         leaderBroadcastHeartBeatTimerKey,
		TimeoutTick: leaderBroadcastHeartBeatTimerTick,
		Callback: func(key, value any) {
			r.broadcast(message.MessageID_MsgAppendEntriesReq, nil)
			rl.startBroadcastHeartbeatTimer()
		},
	})
}
