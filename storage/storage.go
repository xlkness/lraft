package storage

import "lraft/message"

type SnapshotReader interface {
	Term() uint64
	Index() uint64
	Serialize() ([]byte, error)
}

type reader interface {
	InitialState() (*message.StorageState, error)
	Entries(lowIndex, highIndex uint64) ([]*message.Entry, SnapshotReader, error)
	Term(index uint64) (term uint64, find bool, snapshot SnapshotReader, err error)
	LastIndex() (uint64, SnapshotReader, error)
	FirstIndex() (uint64, SnapshotReader, error)
}

type writer interface {
	ApplyTo(state *message.StorageState, entries []*message.Entry) error
}

type Storage interface {
	reader
	writer
}
