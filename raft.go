package lraft

import (
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
	handleEvent(msg *message.Message)
}

type termInfo struct {
	term   uint64
	leader int64
}

type raft struct {
	id int64

	term     uint64
	leader   int64
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

	// load state from storage
	state, err := storage.InitialState()
	if err != nil {
		panic(err)
	}
	term := &termInfo{term: state.Term}
	r.becomeFollower(term.term, None)
	return r
}

func (r *raft) Tick() {
	utils.Assert(r.stateMachine.Tick(), nil)
}

func (r *raft) HandleEvent(msg *message.Message) {
	r.stateMachine.HandleEvent(msg)
}

func (r *raft) state() *message.StorageState {
	s := &message.StorageState{
		Term:             r.term,
		LastAppliedIndex: r.entries.LastIndex(),
	}
	return s
}

func (r *raft) send(to int64, msg message.Message) {
	msg.From = r.id
	msg.To = to
	msg.LastLogIndex = r.entries.LastIndex()
	msg.LastLogTerm = r.entries.FindTerm(r.entries.LastIndex())
	r.transporter.Send(to, &msg)
}

func (r *raft) broadcast(msg message.Message) {
	for k := range r.peers {
		if k == r.id {
			continue
		}
		r.send(k, msg)
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

func (r *raft) becomeFollower(term uint64, leader int64) {
	utils.Assert(r.stateMachine.GotoState(StateFollower), nil)
	r.term = term
	r.leader = leader
}

func (r *raft) becomeCandidate() {
	utils.Assert(r.stateMachine.GotoState(StateCandidate), nil)
	r.term = r.term + 1
}

func (r *raft) becomeLeader() {
	utils.Assert(r.stateMachine.GotoState(StateFollower), nil)
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

	stateList := make([]*statem.StateConf, 0, len(roleRegisterList))
	for _, v := range roleRegisterList {
		stateList = append(stateList, &statem.StateConf{
			Current:       v.state,
			EntryCallback: v.become,
			ExitCallback:  v.exit,
			HandleEvent:   v.handleEvent,
		})
	}
	r.stateMachine.Init(stateList)
}
