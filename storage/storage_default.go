package storage

import "lraft/message"

type defaultStorage struct {
	state   *message.StorageState
	entries []message.Entry
}

func (sm *defaultStorage) InitialState() (*message.StorageState, error) {
	return nil, nil
}
func (sm *defaultStorage) Entries(lowIndex, highIndex uint64) ([]message.Entry, snapshotReader, error) {
	return nil, nil, nil
}
func (sm *defaultStorage) Term(index uint64) (term uint64, find bool, snapshot snapshotReader, err error) {
	return 1, false, nil, nil
}
func (sm *defaultStorage) LastIndex() (uint64, snapshotReader, error) {
	return 1, nil, nil
}
func (sm *defaultStorage) FirstIndex() (uint64, snapshotReader, error) {
	return 1, nil, nil
}
func (sm *defaultStorage) ProposeTo(state *message.StorageState, entry *message.Entry) error {
	return nil
}
func (sm *defaultStorage) ApplyTo(state *message.StorageState, index uint64) error {
	return nil
}
func (sm *defaultStorage) saveDelta(state *message.StorageState) error {
	return nil
}
