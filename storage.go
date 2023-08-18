package lraft

import "lraft/message"

type StorageReader interface {
	InitialState() (*message.StorageState, error)
	Entries(lowIndex, highIndex uint64) ([]message.Entry, *message.Snapshot, error)
	Term(index uint64) (term uint64, find bool, err error)
	LastIndex() (uint64, error)
	FirstIndex() (uint64, error)
}

type StorageWriter interface {
	ApplyTo(state *message.StorageState, index uint64) error
}

type Storage interface {
	StorageReader
	StorageWriter
}
