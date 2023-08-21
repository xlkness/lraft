package lraft

import (
	"google.golang.org/protobuf/proto"
	"lraft/entries"
	"lraft/message"
	"lraft/statem"
	"lraft/storage"
	"lraft/transport"
	"lraft/utils"
)

var None int64 = -1

type StateType = int

const (
	StateLeader = 1 + iota
	StateFollower
	StateCandidate
)

type role interface {
	become(stateData *statem.StateData)
	exit(stateData *statem.StateData)
	handleEvent(msgID message.MessageID, payload proto.Message)
}

type termInfo struct {
	term   uint64
	leader int64
}

type raft struct {
	id int64

	term     *termInfo
	votedFor int64

	entries *entries.Manager

	// 各节点状态记录
	peers     peersRecord
	peerVotes map[int64]bool
	// 状态机，记录角色状态切换，以及状态数据和状态内定时器的维护
	stateMachine *statem.Machine
	transporter  transport.Transporter
}

func newRaft(transporter transport.Transporter, storage storage.Storage) *raft {
	r := new(raft)
	r.transporter = transporter
	r.entries = entries.NewEntriesManager(storage)
	r.stateMachine = statem.NewStateMachine(nil)
	r.registerRoles()

	// todo load from storage
	term := &termInfo{term: 1}
	r.becomeFollower(term.term, None)
	return r
}

func (r *raft) Tick() {
	utils.Assert(r.stateMachine.Tick(), nil)
}

func (r *raft) HandleEvent(msgID message.MessageID, payload proto.Message) {
	r.stateMachine.HandleEvent(msgID, payload)
}

func (r *raft) state() *message.StorageState {
	s := &message.StorageState{
		Term:             r.term.term,
		Vote:             r.votedFor,
		LastAppliedIndex: r.entries.LastIndex(),
	}
	return s
}

func (r *raft) handleEventCommon(msgID message.MessageID, payload proto.Message) (isContinue bool) {
	switch msgID {
	case message.MessageID_MsgAppendEntriesReq:
		request := payload.(*message.AppendEntriesReq)
		success := true
		switch {
		case request.Term > r.term.term:
			r.becomeFollower(request.Term, request.LeaderID)
		case request.Term < r.term.term:
			success = false
		case r.findEntry(int(request.PrevLogIndex)) == nil:
			success = false
		case r.findTerm(int(request.PrevLogIndex)) != int(request.PrevLogTerm):
			success = false
			// todo 删除当前及以后的日志
		}

		response := &message.AppendEntriesRes{Success: success}
		if !success {
			r.send(request.LeaderID, message.MessageID_MsgAppendEntriesReq, response)
			return
		}

		// todo 添加新条目
		// 各个节点进度从大到小排序，取半数索引处的节点进度，直接追平到这个进度，表示集群半数认同的进度

		if int(request.LeaderCommit) > r.commitIndex {
			r.commitIndex = min(int(request.LeaderCommit), r.lastIndex())
		}
		r.send(request.LeaderID, response)
	case message.MessageID_MsgRequestVoteReq:
		request := payload.(*message.RequestVoteReq)
		success := false
		switch {
		case int(request.Term) > r.term.term:
			success = true
			r.becomeFollower(int(request.Term), None)
		case int(request.Term) >= r.term.term:
			success = true
		case r.votedFor == 0 && r.isUpToDate():
			success = true
		}

		r.send(request.CandidateID, &message.RequestVoteRes{VoteGranted: success})
	default:
		return true
	}
}

func (r *raft) send(to int64, msgid message.MessageID, payload proto.Message) {
	r.transporter.Send(to, msgid, payload)
}

func (r *raft) broadcast(msgid message.MessageID, payload proto.Message) {
	for k := range r.peers {
		if k == r.id {
			continue
		}
		r.send(k, msgid, payload)
	}
}

func (r *raft) foreach(except int64, f func(id int64, p *peerRecord)) {
	for k, v := range r.peers {
		if k == except {
			continue
		}
		f(k, v)
	}
}

func (r *raft) isUpToDate(lasti, term uint64) bool {
	return term > l.lastTerm() || (term == l.lastTerm() && lasti >= l.lastIndex())
}

func (r *raft) findEntry(index int) *message.Entry {
	return nil
}

func (r *raft) lastIndex() int {
	return 0
}

func (r *raft) findTerm(index int) int {
	// todo 从unstable以及stable storage中查找term
	return 0
}

func (r *raft) becomeFollower(term uint64, leader int64) {
	utils.assert(r.stateMachine.GotoState(StateFollower), nil)
	r.term.term = term
	r.term.leader = leader
}

func (r *raft) becomeCandidate() {
	utils.assert(r.stateMachine.GotoState(StateCandidate), nil)
	r.term.term = r.term.term + 1
}

func (r *raft) becomeLeader() {
	utils.assert(r.stateMachine.GotoState(StateFollower), nil)
}

type pollResult = int

const (
	pollResult_Voting = 1 + iota
	pollResult_Win
	pollResult_Lose
)

func (r *raft) poll(id int64, vote bool) (granted, rejected int, result pollResult) {
	if _, find := r.peerVotes[id]; !find {
		r.peerVotes[id] = vote
	}

	for _, v := range r.peerVotes {
		if v {
			granted++
		} else {
			rejected++
		}
	}

	if granted >= r.quorum() {
		result = pollResult_Win
	} else if rejected >= r.quorum() {
		result = pollResult_Lose
	} else {
		result = pollResult_Voting
	}

	return
}

func (r *raft) quorum() int {
	return len(r.peers)/2 + 1
}

func (r *raft) registerRoles() {
	roleRegisterList := []struct {
		state StateType
		role
	}{
		{StateLeader, (*raftLeader)(r)},
		{StateFollower, (*raftFollower)(r)},
		{StateCandidate, (*raftCandidate)(r)},
	}

	stateList := make([]*statem.StateConf[*Event], 0, len(roleRegisterList))
	for _, v := range roleRegisterList {
		stateList = append(stateList, &statem.StateConf[*Event]{
			Current:       v.state,
			EntryCallback: v.become,
			ExitCallback:  v.exit,
			HandleEvent:   v.handleEvent,
		})
	}
	r.stateMachine.Init(stateList)
}
