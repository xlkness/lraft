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

	rl.startBroadcastHeartbeatTimer()
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
		request := payload.(*message.AppendEntriesReq)
		// 追加到本地记录
		r.entries.LeaderAppendEntries(request.Entries)
		// 广播本次条目进度
		rl.broadcastAppend()
		//r.broadcast(message.MessageID_MsgAppendEntriesReq, payload)
	case message.MessageID_MsgAppendEntriesRes:
		msg := payload.(*message.AppendEntriesRes)
		if msg.Success {
			// 计算半数节点都追加过的最大索引
			quorumIndex := r.peers.calcQuorumLogProgress()
			// 将应用进度追加到这里
			rl.entries.ApplyToIndex(r.state(), quorumIndex)
		} else {
			// 对方进度不对，根据返回的进度值纠正
			if r.peers.findPeer(msg.From).goBack(msg.NeedAppendIndex, msg.RejectedIndex) {
				// 对方进度不对，发送append消息纠正进度
				rl.sendAppend(msg.From)
			}
		}
	case message.MessageID_MsgRequestPreVoteRes:
		msg := payload.(*message.RequestVoteRes)
		if !msg.VoteGranted {
			r.becomeFollower(msg.Term, None)
		} else {
			_, _, result := r.poll(msg.FromID, msg.VoteGranted)
			if result == pollResult_Win {
				r.broadcast(message.MessageID_MsgRequestVoteReq, &message.RequestVoteReq{})
			} else if result == pollResult_Lose {
				r.becomeFollower(msg.Term, None)
			}
		}
	case message.MessageID_MsgRequestVoteRes:
		msg := payload.(*message.RequestVoteRes)
		if !msg.VoteGranted {
			r.becomeFollower(msg.Term, None)
		} else {
			_, _, result := r.poll(msg.FromID, msg.VoteGranted)
			if result == pollResult_Win {
				r.becomeLeader()
			} else if result == pollResult_Lose {
				r.becomeFollower(msg.Term, None)
			}
		}
	}
}

func (rl *raftLeader) broadcastAppend() {
	r := (*raft)(rl)
	r.foreach(rl.id, func(id int64, record *peerRecord) {
		// 比对进度，发送进度之后的日志条目
		rl.sendAppend(id)
	})
}

func (rl *raftLeader) sendAppend(to int64) {
	toPeer := rl.peers.findPeer(to)
	entries, _ := rl.entries.Slice(toPeer.LogMatch, rl.entries.LastIndex())
	msg := &message.AppendEntriesReq{
		Term:         rl.term.term,
		LeaderID:     rl.id,
		PrevLogIndex: rl.entries.LastIndex(),
		PrevLogTerm:  rl.entries.FindTerm(rl.entries.LastIndex()),
		Entries:      entries,
	}

	(*raft)(rl).send(to, message.MessageID_MsgAppendEntriesReq, msg)
}

func (rl *raftLeader) startBroadcastHeartbeatTimer() {
	r := (*raft)(rl)
	r.stateMachine.InsertKeyValueTimeout(&statem.KVTimeout{
		Key:         leaderBroadcastHeartBeatTimerKey,
		TimeoutTick: leaderBroadcastHeartBeatTimerTick,
		Callback: func(key, value any) {
			r.broadcast(message.MessageID_MsgAppendEntriesReq, &message.AppendEntriesReq{})
			rl.startBroadcastHeartbeatTimer()
		},
	})
}
