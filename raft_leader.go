package lraft

import (
	"lraft/message"
	"lraft/statem"
)

const leaderBroadcastHeartBeatTimerKey = "broadcast.heartbeat"
const leaderBroadcastHeartBeatTimerTick = 2

type raftLeader raft

func (rl *raftLeader) become(state *statem.StateData) {
	r := (*raft)(rl)

	// 给自己发一条空操作的日志，广播到集群
	r.send(rl.id, message.Message{
		MsgID: message.MessageID_MsgAppendEntriesReq,
		AppendEntriesReq: &message.AppendEntriesReq{
			Entries: []*message.Entry{
				{
					Term:      rl.term,
					Index:     rl.entries.LastIndex() + 1,
					EntryType: 0,
					Data:      nil,
				},
			},
		},
	})

	rl.startBroadcastHeartbeatTimer()
}

func (rl *raftLeader) exit(state *statem.StateData) {

}

func (rl *raftLeader) handleEvent(msg *message.Message) {
	r := (*raft)(rl)

	switch msg.MsgID {
	case message.MessageID_MsgAppendEntriesReq:
		// 追加到本地记录
		r.entries.LeaderAppendEntries(msg.AppendEntriesReq.Entries)
		// 广播本次条目进度
		rl.broadcastAppend()
		//r.broadcast(message.MessageID_MsgAppendEntriesReq, payload)
	case message.MessageID_MsgAppendEntriesRes:
		if !msg.Reject {
			r.peers.findPeer(msg.From).goNext(msg.AppendEntriesRes.NeedAppendIndex)
			// 计算半数节点都追加过的最大索引
			quorumIndex := r.peers.calcQuorumLogProgress()
			// 将应用进度追加到这里
			rl.entries.ApplyToIndex(r.state(), quorumIndex)
		} else {
			// 对方进度不对，根据返回的进度值纠正
			if r.peers.findPeer(msg.From).goBack(msg.AppendEntriesRes.NeedAppendIndex, msg.AppendEntriesRes.RejectedIndex) {
				// 对方进度不对，发送append消息纠正进度
				rl.sendAppend(msg.From)
			}
		}
	case message.MessageID_MsgRequestPreVoteRes:
		_, _, result := r.poll(msg.From, msg.Reject)
		if result == pollResult_Win {
			r.broadcast(message.Message{
				MsgID: message.MessageID_MsgRequestVoteReq,
			})
		} else if result == pollResult_Lose {
			r.becomeFollower(msg.Term, None)
		}
	case message.MessageID_MsgRequestVoteRes:
		_, _, result := r.poll(msg.From, msg.Reject)
		if result == pollResult_Win {
			r.becomeLeader()
		} else if result == pollResult_Lose {
			r.becomeFollower(msg.Term, None)
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
	msg := message.Message{
		MsgID: message.MessageID_MsgAppendEntriesReq,
		Term:  rl.term,
		AppendEntriesReq: &message.AppendEntriesReq{
			Entries: entries,
		},
	}

	(*raft)(rl).send(to, msg)
}

func (rl *raftLeader) startBroadcastHeartbeatTimer() {
	r := (*raft)(rl)
	r.stateMachine.InsertKeyValueTimeout(&statem.KVTimeout{
		Key:         leaderBroadcastHeartBeatTimerKey,
		TimeoutTick: leaderBroadcastHeartBeatTimerTick,
		Callback: func(key, value any) {
			msg := message.Message{
				MsgID: message.MessageID_MsgAppendEntriesReq,
			}
			r.broadcast(msg)
			rl.startBroadcastHeartbeatTimer()
		},
	})
}
