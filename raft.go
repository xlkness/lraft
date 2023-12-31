package lraft

import (
	"log"
	"lraft/entries"
	"lraft/message"
	"lraft/statem"
	"lraft/storage"
	"lraft/transport"
	"lraft/utils"
	"math/rand"
	"sort"
	"time"
)

var None int64 = -1

type StateType = int

const (
	StateLeader = 1 + iota
	StateFollower
	StateCandidate
)

type peerRecord struct {
	LogMatch uint64 // 已知复制到节点的最高日志条目号，初始为0
	LogNext  uint64 // 发送给节点下一条日志条目索引号，一般为LogMatch+1，初始为leader最高索引号+1
}

func (pr *peerRecord) goNext(index uint64) {
	pr.LogMatch = max(pr.LogMatch, index)
	pr.LogNext = max(pr.LogNext, index+1)
}

func (pr *peerRecord) goBack() (conflict bool) {
	if pr.LogMatch != 0 {
		pr.LogNext = pr.LogMatch + 1
		return true
	}
	pr.LogNext -= 1
	return true
}

type peersRecord map[int64]*peerRecord

func (psr peersRecord) findPeer(id int64) *peerRecord {
	pr, find := psr[id]
	if !find {
		return &peerRecord{}
	}
	return pr
}

func (psr peersRecord) calcQuorumLogProgress() uint64 {
	progressList := make([]uint64, 0, len(psr))
	for _, v := range psr {
		progressList = append(progressList, v.LogMatch)
	}
	sort.SliceStable(progressList, func(i, j int) bool {
		return progressList[i] > progressList[j]
	})
	return progressList[len(psr)/2]
}

func (psr peersRecord) resetProgress(last uint64) {
	for _, v := range psr {
		v.LogNext = last
		v.LogMatch = 0
	}
}

type raft struct {
	id     int64
	config *Config

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
	rander       *rand.Rand
}

func newRaft(id int64, peers []int64, transporter transport.Transporter, storage storage.Storage, config *Config) *raft {
	r := new(raft)
	r.id = id
	r.config = config
	r.transporter = transporter
	r.entries = entries.NewEntriesManager(storage)
	r.peers = make(peersRecord)
	r.peerVotes = make(map[int64]bool)
	r.stateMachine = statem.NewStateMachine(nil)
	r.registerRoles()
	r.rander = rand.New(rand.NewSource(time.Now().UnixNano()))
	r.resetData()

	for _, v := range peers {
		r.peers[v] = &peerRecord{LogNext: 1}
	}

	// load state from storage
	state, err := storage.InitialState()
	if err != nil {
		panic(err)
	}

	r.becomeFollower(state.Term, None)
	return r
}

func (r *raft) Tick() {
	utils.Assert(r.stateMachine.Tick(), nil)
}

func (r *raft) HandleEvent(msg *message.Message) {
	log.Printf("node[%v] leader[%v] receive msg:%v", r.id, r.leader, msg.String())
	r.stateMachine.HandleEvent(msg)
}

func (r *raft) state() *message.StorageState {
	s := &message.StorageState{
		Term:             r.term,
		LastAppliedIndex: r.entries.LastIndex(),
	}
	return s
}

func (r *raft) resetData() {
	r.leader = None
	r.votedFor = None
}

func (r *raft) send(to int64, msg message.Message) {
	msg.Term = r.term
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
}

func (r *raft) becomeLeader() {
	utils.Assert(r.stateMachine.GotoState(StateLeader), nil)
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

type role interface {
	become(stateData *statem.StateData)
	exit(stateData *statem.StateData)
	handleEvent(msg *message.Message)
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
