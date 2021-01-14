package gocelery

import (
	"container/list"
	"context"
	"encoding/hex"
	"sync"
)

type inMemoryBackend struct {
	lock  sync.RWMutex
	store map[string][]byte
}

func NewInMemoryBackend() Backend {
	return &inMemoryBackend{
		store: make(map[string][]byte),
	}
}

func (i *inMemoryBackend) Get(id []byte) ([]byte, error) {
	i.lock.RLock()
	defer i.lock.RUnlock()
	v, ok := i.store[hex.EncodeToString(id)]
	if !ok {
		return nil, ErrNotFound
	}

	return v, nil
}

func (i *inMemoryBackend) Set(id, value []byte) error {
	i.lock.Lock()
	defer i.lock.Unlock()
	i.store[hex.EncodeToString(id)] = value
	return nil
}

func (i *inMemoryBackend) Delete(id []byte) error {
	i.lock.Lock()
	defer i.lock.Unlock()
	delete(i.store, hex.EncodeToString(id))
	return nil
}

func (i *inMemoryBackend) Length() int {
	i.lock.RLock()
	defer i.lock.Unlock()
	return len(i.store)
}

type inMemoryBroker struct {
	queue      *list.List
	dispatched map[string]struct{}
	lock       sync.RWMutex
}

func NewInMemoryBroker() Broker {
	return &inMemoryBroker{
		queue:      list.New(),
		dispatched: make(map[string]struct{}),
		lock:       sync.RWMutex{},
	}
}

func (i *inMemoryBroker) Start(context.Context) {}

func (i *inMemoryBroker) Enqueue(id []byte) error {
	i.lock.Lock()
	defer i.lock.Unlock()
	i.queue.PushBack(id)
	return nil
}

func (i *inMemoryBroker) Dequeue() ([]byte, error) {
	i.lock.Lock()
	defer i.lock.Unlock()
	e := i.queue.Front()
	if e == nil {
		return nil, ErrNotFound
	}

	defer i.queue.Remove(e)
	v := e.Value.([]byte)
	i.dispatched[hex.EncodeToString(v)] = struct{}{}
	return v, nil
}

func (i *inMemoryBroker) Finished(id []byte) {
	i.lock.Lock()
	defer i.lock.Unlock()
	delete(i.dispatched, hex.EncodeToString(id))
}

func (i *inMemoryBroker) Len() int {
	i.lock.RLock()
	defer i.lock.RUnlock()
	return i.queue.Len()
}
