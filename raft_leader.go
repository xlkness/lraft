package lraft

import (
	"log"
	"lraft/message"
	"lraft/statem"
)

const leaderBroadcastHeartBeatTimerKey = "broadcast.heartbeat"

type raftLeader raft

func (rl *raftLeader) become(state *statem.StateData) {
	log.Printf("node[%v] become leader", rl.id)

	r := (*raft)(rl)
	r.leader = r.id

	// leader一当选，就先把各个节点index进度设置为自己的进度，后续通过心跳消息纠正各个节点真实进度
	r.peers.resetProgress(max(rl.entries.LastIndex(), 1))

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
	(*raft)(rl).resetData()
	log.Printf("node[%v] exit candidate", rl.id)
}

func (rl *raftLeader) handleEvent(msg *message.Message) {
	r := (*raft)(rl)

	switch msg.MsgID {
	case message.MessageID_MsgAppendEntriesReq:
		// 追加到本地记录
		r.entries.LeaderAppendEntries(rl.term, msg.AppendEntriesReq.Entries)
		// 给自己发个消息追加成功推进进度
		r.send(rl.id, message.Message{
			MsgID:            message.MessageID_MsgAppendEntriesRes,
			Reject:           false,
			AppendEntriesRes: &message.AppendEntriesRes{},
		})
		// 广播本次条目进度
		rl.broadcastAppend()
	case message.MessageID_MsgAppendEntriesRes:
		if !msg.Reject {
			r.peers.findPeer(msg.From).goNext(rl.entries.LastIndex())
			// 计算半数节点都追加过的最大索引
			quorumIndex := r.peers.calcQuorumLogProgress()
			// 将应用进度追加到这里
			rl.entries.ApplyToIndex(r.state(), quorumIndex)
		} else {
			// 对方进度不对，根据返回的进度值纠正
			if r.peers.findPeer(msg.From).goBack() {
				// 对方进度不对，发送append消息纠正进度
				rl.sendAppend(msg.From)
			}
		}
	case message.MessageID_MsgRequestPreVoteRes:
		_, _, result := r.poll(msg.From, !msg.Reject)
		if result == pollResult_Win {
			r.broadcast(message.Message{
				MsgID: message.MessageID_MsgRequestVoteReq,
			})
		} else if result == pollResult_Lose {
			r.becomeFollower(msg.Term, None)
		}
	case message.MessageID_MsgRequestVoteRes:
		_, _, result := r.poll(msg.From, !msg.Reject)
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
	log.Printf("to node[%v] slice(%v,%v)", to, toPeer.LogNext, rl.entries.LastIndex())
	entries, _ := rl.entries.Slice(toPeer.LogNext, rl.entries.LastIndex())
	msg := message.Message{
		MsgID: message.MessageID_MsgAppendEntriesReq,
		Term:  rl.term,
		AppendEntriesReq: &message.AppendEntriesReq{
			Entries: entries,
		},
	}

	msg.Term = rl.term
	msg.From = rl.id
	msg.To = to
	msg.LastLogIndex = toPeer.LogNext - 1
	msg.LastLogTerm = rl.entries.FindTerm(toPeer.LogNext - 1)
	rl.transporter.Send(to, &msg)
}

func (rl *raftLeader) broadcastHeartbeat() {
	r := (*raft)(rl)
	r.foreach(rl.id, func(id int64, record *peerRecord) {
		lastApplied := min(record.LogMatch, rl.entries.LastIndex())
		msg := message.Message{
			MsgID:            message.MessageID_MsgAppendEntriesReq,
			LastLogIndex:     lastApplied,
			AppendEntriesReq: &message.AppendEntriesReq{},
		}
		r.send(id, msg)
	})
}

func (rl *raftLeader) startBroadcastHeartbeatTimer() {
	r := (*raft)(rl)
	r.stateMachine.InsertKeyValueTimeout(&statem.KVTimeout{
		Key:         leaderBroadcastHeartBeatTimerKey,
		TimeoutTick: rl.config.BroadcastHeartBeatTimerTick,
		Callback: func(key, value any) {
			rl.broadcastHeartbeat()
			rl.startBroadcastHeartbeatTimer()
		},
	})
}
