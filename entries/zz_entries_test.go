package entries

import (
	"fmt"
	"lraft/message"
	"lraft/storage"
	"os"
	"testing"
)

var entries = []*message.Entry{
	{Term: 4, Index: 1, EntryType: 3, Data: []byte("sdfsdf")},
	{Term: 5, Index: 2, EntryType: 4, Data: []byte("123123")},
	{Term: 6, Index: 3, EntryType: 5, Data: []byte("sdfsdf23432")},
	{Term: 6, Index: 4, EntryType: 5, Data: []byte("sdfsdf23432")},
	{Term: 6, Index: 5, EntryType: 5, Data: []byte("sdfsdf23432")},
	{Term: 6, Index: 6, EntryType: 5, Data: []byte("sdfsdf23432")},
	{Term: 7, Index: 7, EntryType: 5, Data: []byte("sdfsdf23432")},
	{Term: 8, Index: 8, EntryType: 5, Data: []byte("sdfsdf23432")},
	{Term: 9, Index: 9, EntryType: 5, Data: []byte("sdfsdf23432")},
}

func testLeaderEntries() {
	os.Remove("debug.entries")
	st := storage.NewDebugMemoryStorage(1)
	em := NewEntriesManager(st)
	em.LeaderAppendEntries(1, entries)
	assert(em.LastIndex(), uint64(9))
	assert(em.FirstIndex(), uint64(1))
	assert(em.FindTerm(3), uint64(6))
	es, _ := em.Slice(1, 9)
	assert(len(es), 9)
	assert(es[0].Index, uint64(1))
	assert(es[len(es)-1].Index, uint64(9))

	em.ApplyToIndex(&message.StorageState{}, 9)
	assert(em.LastIndex(), uint64(9))
	assert(em.FirstIndex(), uint64(1))
	assert(em.FindTerm(3), uint64(6))
	es, _ = em.Slice(1, 9)
	assert(len(es), 9)
	assert(es[0].Index, uint64(1))
	assert(es[len(es)-1].Index, uint64(9))

	assert(len(em.tempEntries), 0)
}

func testLeaderEntries1() {
	os.Remove("debug.entries")
	st := storage.NewDebugMemoryStorage(1)
	em := NewEntriesManager(st)
	em.LeaderAppendEntries(1, entries)
	assert(em.LastIndex(), uint64(9))
	assert(em.FirstIndex(), uint64(1))
	assert(em.FindTerm(3), uint64(6))
	es, _ := em.Slice(1, 9)
	assert(len(es), 9)
	assert(es[0].Index, uint64(1))
	assert(es[len(es)-1].Index, uint64(9))

	em.ApplyToIndex(&message.StorageState{}, 6)
	assert(em.LastIndex(), uint64(9))
	assert(em.FirstIndex(), uint64(1))
	assert(em.FindTerm(3), uint64(6))
	es, _ = em.Slice(1, 9)
	assert(len(es), 9)
	assert(es[0].Index, uint64(1))
	assert(es[len(es)-1].Index, uint64(9))

	assert(len(em.tempEntries), 3)
}

func testAppend() {
	os.Remove("debug.entries")
	st := storage.NewDebugMemoryStorage(1)
	em := NewEntriesManager(st)
	em.appendEntries(entries)

	newEntries := []*message.Entry{
		{Term: 4, Index: 10, EntryType: 3, Data: []byte("sdfsdf")},
		{Term: 5, Index: 11, EntryType: 4, Data: []byte("123123")},
	}
	em.appendEntries(newEntries)
	assert(em.LastIndex(), uint64(11))
	assert(em.FirstIndex(), uint64(1))
	es, _ := em.Slice(1, 11)
	assert(es[0].Index, uint64(1))
	assert(es[len(es)-1].Index, uint64(11))

	newEntries = []*message.Entry{
		{Term: 11, Index: 10, EntryType: 3, Data: []byte("sdfsdf")},
		{Term: 22, Index: 11, EntryType: 4, Data: []byte("123123")},
	}
	em.appendEntries(newEntries)
	assert(em.LastIndex(), uint64(11))
	assert(em.FindTerm(10), uint64(11))
	assert(em.FindTerm(11), uint64(22))

	em.ApplyToIndex(&message.StorageState{}, 10)
	es, _ = em.Slice(1, 11)
	assert(len(es), 11)
	assert(es[0].Index, uint64(1))
	assert(es[len(es)-1].Index, uint64(11))

	assert(em.FirstIndex(), uint64(1))
	assert(em.LastIndex(), uint64(11))

	assert(len(em.tempEntries), 1)
	assert(em.tempEntries[0].Index, uint64(11))
	assert(em.tempEntries[0].Term, uint64(22))
}

func testFollowerEntries() {
	os.Remove("debug.entries")
	st := storage.NewDebugMemoryStorage(1)
	em := NewEntriesManager(st)
	em.FollowerAppendEntries(0, 0, entries)
	assert(em.LastIndex(), uint64(9))
	assert(em.FirstIndex(), uint64(1))
	assert(em.FindTerm(3), uint64(6))
	es, _ := em.Slice(1, 9)
	assert(len(es), 9)
	assert(es[0].Index, uint64(1))
	assert(es[len(es)-1].Index, uint64(9))

	em.ApplyToIndex(&message.StorageState{}, 6)
	assert(em.LastIndex(), uint64(9))
	assert(em.FirstIndex(), uint64(1))
	assert(em.FindTerm(3), uint64(6))
	es, _ = em.Slice(1, 9)
	assert(len(es), 9)
	assert(es[0].Index, uint64(1))
	assert(es[len(es)-1].Index, uint64(9))

	assert(len(em.tempEntries), 3)

	newEntries := []*message.Entry{
		{Term: 11, Index: 8, EntryType: 3, Data: []byte("sdfsdf")},
		{Term: 22, Index: 9, EntryType: 4, Data: []byte("123123")},
	}
	em.FollowerAppendEntries(9, 9, newEntries)
	assert(em.LastIndex(), uint64(9))
	assert(em.FirstIndex(), uint64(1))
	assert(em.FindTerm(8), uint64(11))
	assert(em.FindTerm(9), uint64(22))
	assert(len(em.tempEntries), 3)
}

func TestEntries(t *testing.T) {
	testLeaderEntries()
	testLeaderEntries1()
	testAppend()
	testFollowerEntries()
}

func assert(cur, expected any) {
	if cur != expected {
		panic(fmt.Errorf("cur:%v, expected:%v", cur, expected))
	}
}
