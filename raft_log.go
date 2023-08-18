package lraft

import (
	"lraft/message"
	"sort"
)

type peerRecord struct {
	LogMatch uint64 // 已知复制到节点的最高日志条目号，初始为0
	LogNext  uint64 // 发送给节点下一条日志条目索引号，一般为LogMatch+1，初始为leader最高索引号+1
}

func (pr *peerRecord) goNext(index uint64) {
	pr.LogMatch = max(pr.LogMatch, index)
	pr.LogNext = max(pr.LogNext, index+1)
}

func (pr *peerRecord) goBack(reqAppendIndex, rejectedIndex uint64) (conflict bool) {
	if pr.LogMatch != 0 {
		if pr.LogMatch >= reqAppendIndex {
			return false
		}

		pr.LogNext = pr.LogMatch + 1
		return true
	}

	if pr.LogNext-1 != reqAppendIndex {
		return false
	}

	pr.LogNext = min(reqAppendIndex, rejectedIndex+1, 1)
	return true
}

type peersRecord map[int64]*peerRecord

func (psr peersRecord) findPeer(id int64) *peerRecord {
	return psr[id]
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

type logManager struct {
	stableStorage   Storage
	unstableEntries []message.Entry
}

func (mgr *logManager) applyToIndex(state *message.StorageState, index uint64) {
	err := mgr.stableStorage.ApplyTo(state, index)
	if err != nil {
		panic(err)
	}
}

func (mgr *logManager) firstIndex() uint64 {
	return 0
}

func (mgr *logManager) lastIndex() uint64 {
	if len(mgr.unstableEntries) == 0 {
		lastIndex, err := mgr.stableStorage.LastIndex()
		if err != nil {
			panic(err)
		}

		return lastIndex
	}
	return mgr.unstableEntries[len(mgr.unstableEntries)-1].Index
}

func (mgr *logManager) entriesSlice(low, high uint64) []message.Entry {
	mgr.stableStorage.LastIndex()
}

func (mgr *logManager) findTerm(index uint64) uint64 {
	for _, e := range mgr.unstableEntries {
		if e.Index == index {
			return e.Term
		}
	}

	term, find, err := mgr.stableStorage.Term(index)
	if err != nil {
		panic(err)
	}

	if !find {
		return -1
	}

	return term
}

func (mgr *logManager) appendEntries(entries []message.Entry) {
	if len(entries) == 0 {
		return
	}

	lastAppliedIndex, err := mgr.stableStorage.LastIndex()
	if err != nil {
		panic(err)
	}

	applyFirstIndex := entries[0].Index
	assert(applyFirstIndex-1 < lastAppliedIndex, false)

	if len(mgr.unstableEntries) == 0 {
		mgr.unstableEntries = append(mgr.unstableEntries, entries...)
		return
	}

	switch {
	case applyFirstIndex == mgr.unstableEntries[len(mgr.unstableEntries)-1].Index+1:
		mgr.unstableEntries = append(mgr.unstableEntries, entries...)
	case applyFirstIndex < mgr.unstableEntries[0].Index:
		mgr.unstableEntries = entries
	default:
		mgr.unstableEntries = append([]message.Entry{}, mgr.unstableEntries[:applyFirstIndex+1]...)
		mgr.unstableEntries = append(mgr.unstableEntries, entries...)
	}
}
