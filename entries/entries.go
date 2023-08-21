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

	err = mgr.stableStorage.ApplyTo(state, mgr.tempEntries[0:int(index-lastAppliedIndex)])
	if err != nil {
		panic(err)
	}
}

func (mgr *Manager) FirstIndex() uint64 {
	return 0
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
		return -1
	}

	return term
}

func (mgr *Manager) LeaderAppendEntries(entries []*message.Entry) {
	mgr.appendEntries(entries)
}

func (mgr *Manager) FollowerAppendEntries(prevTerm, prevIndex uint64, entries []*message.Entry) {
	if len(entries) == 0 {
		return
	}
	if mgr.FindTerm(prevIndex) == prevTerm {
		for i, entry := range entries {
			if mgr.FindTerm(entry.Index) != entry.Term {

				utils.Assert(entry.Index >= mgr.LastIndex(), true)

				mgr.appendEntries(entries[i:])
				//newLastIndex := entries[len(entries)-1].Index
				err := mgr.stableStorage.ApplyTo(nil, entries[i:])
				if err != nil {
					panic(err)
				}
				break
			}
		}
	}
}

func (mgr *Manager) appendEntries(entries []*message.Entry) {
	if len(entries) == 0 {
		return
	}

	lastAppliedIndex, _, err := mgr.stableStorage.LastIndex()
	if err != nil {
		panic(err)
	}

	applyFirstIndex := entries[0].Index
	utils.Assert(applyFirstIndex-1 < lastAppliedIndex, false)

	if len(mgr.tempEntries) == 0 {
		mgr.tempEntries = append(mgr.tempEntries, entries...)
		return
	}

	switch {
	case applyFirstIndex == mgr.tempEntries[len(mgr.tempEntries)-1].Index+1:
		mgr.tempEntries = append(mgr.tempEntries, entries...)
	case applyFirstIndex < mgr.tempEntries[0].Index:
		mgr.tempEntries = entries
	default:
		mgr.tempEntries = append([]*message.Entry{}, mgr.tempEntries[:applyFirstIndex+1]...)
		mgr.tempEntries = append(mgr.tempEntries, entries...)
	}
}
