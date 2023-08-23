package storage

import (
	"encoding/json"
	"fmt"
	"lraft/message"
	"os"
)

type debugMemoryStorage struct {
	id      int64
	state   *message.StorageState
	entries []*message.Entry
}

func NewDebugMemoryStorage(id int64) Storage {
	dms := new(debugMemoryStorage)
	dms.id = id
	dms.state = &message.StorageState{}
	dms.load()
	return dms
}

func (sm *debugMemoryStorage) InitialState() (*message.StorageState, error) {
	return sm.state, nil
}
func (sm *debugMemoryStorage) Entries(lowIndex, highIndex uint64) ([]*message.Entry, SnapshotReader, error) {
	return sm.entries[lowIndex-1 : highIndex], nil, nil
}
func (sm *debugMemoryStorage) Term(index uint64) (term uint64, find bool, snapshot SnapshotReader, err error) {
	if index-1 < 0 || index-1 >= uint64(len(sm.entries)) {
		return 0, false, nil, nil
	}
	return sm.entries[index-1].Term, true, nil, nil
}
func (sm *debugMemoryStorage) LastIndex() (uint64, SnapshotReader, error) {
	if len(sm.entries) == 0 {
		return 0, nil, nil
	}
	return sm.entries[len(sm.entries)-1].Index, nil, nil
}
func (sm *debugMemoryStorage) FirstIndex() (uint64, SnapshotReader, error) {
	if len(sm.entries) == 0 {
		return 0, nil, nil
	}
	return sm.entries[0].Index, nil, nil
}

func (sm *debugMemoryStorage) ApplyTo(state *message.StorageState, entries []*message.Entry) error {
	sm.state = state
	sm.entries = append(sm.entries, entries...)
	sm.saveDelta(entries)
	return nil
}

func (sm *debugMemoryStorage) load() error {
	filename := fmt.Sprintf("%d.debug.entries", sm.id)
	fd, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		panic(err)
	}
	defer fd.Close()
	fInfo, err := fd.Stat()
	if err != nil {
		panic(err)
	}
	if fInfo.Size() == 0 {
		return nil
	}
	stateBuf := make([]byte, 1024)
	_, err = fd.Read(stateBuf)
	if err != nil {
		panic(err)
	}

	for i := range stateBuf {
		if stateBuf[i] == ' ' {
			state := &message.StorageState{}
			err = json.Unmarshal(stateBuf[:i], state)
			if err != nil {
				panic(err)
			}
			sm.state = state
		}
	}

	fd.Seek(1024, 0)

	entriesBuf := make([]byte, int(fInfo.Size()-1024))
	_, err = fd.Read(entriesBuf)
	if err != nil {
		panic(err)
	}

	start := 0
	for i := range entriesBuf {
		if entriesBuf[i] == '\n' {
			entry := &message.Entry{}
			err = json.Unmarshal(entriesBuf[start:i], entry)
			if err != nil {
				panic(err)
			}

			start = i

			sm.entries = append(sm.entries, entry)
		}
	}

	return nil
}

func (sm *debugMemoryStorage) saveDelta(entries []*message.Entry) error {
	filename := fmt.Sprintf("%d.debug.entries", sm.id)
	fd, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		panic(err)
	}
	defer fd.Close()
	_, err = fd.Seek(0, 0)
	if err != nil {
		panic(err)
	}

	stateBin, err := json.Marshal(sm.state)
	if err != nil {
		panic(err)
	}

	stateLen := 1024
	newStateBin := make([]byte, stateLen)
	for i := range newStateBin {
		if i == stateLen-1 {
			break
		}
		newStateBin[i] = ' '
	}
	newStateBin[len(newStateBin)-1] = '\n'
	copy(newStateBin, stateBin)
	_, err = fd.WriteString(string(newStateBin))
	if err != nil {
		panic(err)
	}
	_, err = fd.Seek(0, 2)
	if err != nil {
		panic(err)
	}
	for _, en := range entries {
		enBin, err := json.Marshal(en)
		if err != nil {
			panic(err)
		}
		_, err = fd.WriteString(string(enBin) + "\n")
		if err != nil {
			panic(err)
		}
	}
	return nil
}
