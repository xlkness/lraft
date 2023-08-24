package entries

import (
	"lraft/message"
	"lraft/storage"
	"lraft/utils"
)

type Manager struct {
	stableStorage storage.Storage
	tempEntries   []*message.Entry
}

func NewEntriesManager(storage storage.Storage) *Manager {
	em := new(Manager)
	em.stableStorage = storage
	return em
}

func (mgr *Manager) ApplyToIndex(state *message.StorageState, index uint64) {
	lastAppliedIndex, _, err := mgr.stableStorage.LastIndex()
	if err != nil {
		panic(err)
	}

	if index <= lastAppliedIndex {
		return
	}

	shouldApplyEntries := mgr.tempEntries[:int(index-lastAppliedIndex)]
	err = mgr.stableStorage.ApplyTo(state, shouldApplyEntries)
	if err != nil {
		panic(err)
	}

	mgr.tempEntries = mgr.tempEntries[int(index-lastAppliedIndex):]
}

func (mgr *Manager) FirstIndex() uint64 {
	fi, _, err := mgr.stableStorage.FirstIndex()
	if err != nil {
		panic(err)
	}
	if fi <= 0 && len(mgr.tempEntries) > 0 {
		return mgr.tempEntries[0].Index
	}

	return fi
}

func (mgr *Manager) LastIndex() uint64 {
	if len(mgr.tempEntries) == 0 {
		lastIndex, _, err := mgr.stableStorage.LastIndex()
		if err != nil {
			panic(err)
		}

		return lastIndex
	}
	return mgr.tempEntries[len(mgr.tempEntries)-1].Index
}

func (mgr *Manager) Slice(low, high uint64) (entries []*message.Entry, snapshot storage.SnapshotReader) {

	stableLastIndex, _, err := mgr.stableStorage.LastIndex()
	if err != nil {
		panic(err)
	}
	if low == 0 {
		return nil, nil
	}

	if low <= stableLastIndex {
		var appliedEntries []*message.Entry
		var err error
		appliedEntries, snapshot, err = mgr.stableStorage.Entries(low, min(stableLastIndex, high))
		if err != nil {
			panic(err)
		}

		if snapshot != nil {

		} else {
			entries = append(entries, appliedEntries...)
		}
	}

	if len(mgr.tempEntries) == 0 {
		return entries, snapshot
	}

	if high >= mgr.tempEntries[0].Index {
		entries = append(entries, mgr.tempEntries[0:int(min(uint64(len(mgr.tempEntries)), high))]...)
	}

	return entries, snapshot
}

func (mgr *Manager) IsUpToDate(index, term uint64) bool {
	lastTerm := mgr.FindTerm(mgr.LastIndex())
	return term > lastTerm || (term == lastTerm && index >= mgr.LastIndex())
}

func (mgr *Manager) FindTerm(index uint64) uint64 {
	for _, e := range mgr.tempEntries {
		if e.Index == index {
			return e.Term
		}
	}

	term, find, _, err := mgr.stableStorage.Term(index)
	if err != nil {
		panic(err)
	}

	if !find {
		return 0
	}

	return term
}

func (mgr *Manager) LeaderAppendEntries(term uint64, entries []*message.Entry) {
	lastIndex := mgr.LastIndex()
	for i, e := range entries {
		e.Term = term
		e.Index = lastIndex + 1 + uint64(i)
	}
	mgr.appendEntries(entries)
}

func (mgr *Manager) FollowerAppendEntries(prevTerm, prevIndex uint64, entries []*message.Entry) (rejectedIndex uint64, ok bool) {
	if len(entries) == 0 {
		return mgr.LastIndex(), true
	}
	if mgr.FindTerm(prevIndex) == prevTerm {
		for i, entry := range entries {
			if mgr.FindTerm(entry.Index) != entry.Term {
				// 查找冲突的索引，一旦找到，后续的条目都可以追加
				mgr.appendEntries(entries[i:])
				return mgr.LastIndex(), true
			}
		}
		return
	}

	return mgr.LastIndex(), false
}

func (mgr *Manager) appendEntries(entries []*message.Entry) {
	if len(entries) == 0 {
		return
	}

	lastAppliedIndex, _, err := mgr.stableStorage.LastIndex()
	utils.Assert(err, nil)

	applyFirstIndex := entries[0].Index

	// 只有临时条目可以被纠正，持久化的条目不能被纠正
	utils.Assert(applyFirstIndex-1 >= lastAppliedIndex, true)

	if len(mgr.tempEntries) == 0 {
		mgr.tempEntries = append(mgr.tempEntries, entries...)
		return
	}

	switch {
	case applyFirstIndex == mgr.tempEntries[len(mgr.tempEntries)-1].Index+1:
		// 第一个索引等于当前最后一个索引+1，表示有序
		mgr.tempEntries = append(mgr.tempEntries, entries...)
	case applyFirstIndex < mgr.tempEntries[0].Index:
		// 第一个索引小于第0个临时条目索引，直接替换整个
		mgr.tempEntries = entries
	default:
		// 第一个索引处于临时条目之间，纠正
		mgr.tempEntries = append([]*message.Entry{}, mgr.tempEntries[:applyFirstIndex-lastAppliedIndex-1]...)
		mgr.tempEntries = append(mgr.tempEntries, entries...)
	}
}
