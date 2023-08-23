package storage

import (
	"fmt"
	"lraft/message"
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

func testFileRW() {
	dms := NewDebugMemoryStorage()
	err := dms.ApplyTo(&message.StorageState{Term: 111, LastAppliedIndex: 234}, entries)
	assert(err, nil)
}

func testIndexExtract() {
	dms := NewDebugMemoryStorage()
	err := dms.ApplyTo(&message.StorageState{Term: 9, LastAppliedIndex: 9}, entries)
	assert(err, nil)

	li, _, err := dms.LastIndex()
	assert(err, nil)

	fi, _, err := dms.FirstIndex()
	assert(err, nil)

	ens, _, err := dms.Entries(2, 9)
	assert(err, nil)

	term, find, _, err := dms.Term(3)
	assert(err, nil)

	assert(li, entries[len(entries)-1].Index)
	assert(fi, entries[0].Index)
	assert(len(ens), 8)
	assert(ens[0].Index, uint64(2))
	assert(ens[len(ens)-1].Index, uint64(9))
	assert(find, true)
	assert(term, uint64(6))
}

func TestDebugStorage(t *testing.T) {
	os.Remove("debug.entries")
	testFileRW()

	os.Remove("debug.entries")
	testIndexExtract()
}

func assert(cur, expected any) {
	if cur != expected {
		panic(fmt.Errorf("cur:%v, expected:%v", cur, expected))
	}
}
