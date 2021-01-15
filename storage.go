package gocelery

import (
	"bytes"
	"encoding/hex"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// Storage for storing any kind of data
type Storage interface {
	Get(key []byte) ([]byte, error)
	Set(key, value []byte) error
	Delete(key []byte) error
	Iterate(prefix []byte, f func(key, value []byte))
}

type inMemoryStorage struct {
	lock  sync.RWMutex
	store map[string][]byte
}

func NewInMemoryStorage() Storage {
	return &inMemoryStorage{
		store: make(map[string][]byte),
	}
}

func (i *inMemoryStorage) Get(id []byte) ([]byte, error) {
	i.lock.RLock()
	defer i.lock.RUnlock()
	v, ok := i.store[hex.EncodeToString(id)]
	if !ok {
		return nil, ErrNotFound
	}

	return v, nil
}

func (i *inMemoryStorage) Set(id, value []byte) error {
	i.lock.Lock()
	defer i.lock.Unlock()
	i.store[hex.EncodeToString(id)] = value
	return nil
}

func (i *inMemoryStorage) Delete(id []byte) error {
	i.lock.Lock()
	defer i.lock.Unlock()
	delete(i.store, hex.EncodeToString(id))
	return nil
}

func (i *inMemoryStorage) Iterate(prefix []byte, f func(key, value []byte)) {
	i.lock.RLock()
	defer i.lock.RUnlock()
	for k, v := range i.store {
		key, err := hex.DecodeString(k)
		if err != nil {
			continue
		}

		if !bytes.HasPrefix(key, prefix) {
			continue
		}

		f(key, v)
	}
}

func (i *inMemoryStorage) Length() int {
	i.lock.RLock()
	defer i.lock.Unlock()
	return len(i.store)
}

// levelDBStorage implements the Storage based on levelDB
type levelDBStorage struct {
	db *leveldb.DB
}

// NewLevelDBStorage returns an levelDB implementation of CeleryBackend
func NewLevelDBStorage(db *leveldb.DB) Storage {
	return levelDBStorage{db: db}
}

func (l levelDBStorage) Get(key []byte) ([]byte, error) {
	return l.db.Get(key, nil)
}

func (l levelDBStorage) Set(key, value []byte) error {
	return l.db.Put(key, value, nil)
}

func (l levelDBStorage) Delete(key []byte) error {
	return l.db.Delete(key, nil)
}

func (l levelDBStorage) Iterate(prefix []byte, f func(key, value []byte)) {
	itr := l.db.NewIterator(util.BytesPrefix(prefix), nil)
	for itr.Next() {
		f(itr.Key(), itr.Value())
	}
}
