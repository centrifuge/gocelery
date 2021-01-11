package gocelery

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInMemoryBackend(t *testing.T) {
	runs := 100
	backend := NewInMemoryBackend()
	cn := make(chan struct {
		k []byte
		v []byte
	}, runs)
	for i := 0; i < runs; i++ {
		go func(i int) {
			key := randomBytes(32)
			value := randomBytes(32)
			assert.NoError(t, backend.Set(key, value))
			cn <- struct {
				k []byte
				v []byte
			}{k: key, v: value}
		}(i)
	}

	wg := sync.WaitGroup{}
	for i := 0; i < runs; i++ {
		set := <-cn
		wg.Add(1)
		go func(k, v []byte) {
			defer wg.Done()
			gv, err := backend.Get(k)
			assert.NoError(t, err)
			assert.Equal(t, v, gv)
			assert.NoError(t, backend.Delete(k))
		}(set.k, set.v)
	}

	wg.Wait()
	assert.Len(t, backend.(*inMemoryBackend).store, 0)
}

func TestInMemoryBroker(t *testing.T) {
	runs := 100
	broker := NewInMemoryBroker()
	cn := make(chan []byte, runs)
	for i := 0; i < runs; i++ {
		go func(i int) {
			id := randomBytes(32)
			broker.Enqueue(id)
			cn <- id
		}(i)
	}

	wg := sync.WaitGroup{}
	var ids [][]byte
	for i := 0; i < runs; i++ {
		ids = append(ids, <-cn)
		wg.Add(1)
		go func() {
			defer wg.Done()
			gv, err := broker.Dequeue()
			assert.NoError(t, err)
			broker.Finished(gv)
		}()
	}

	wg.Wait()
	assert.Equal(t, 0, broker.Len())
	assert.Len(t, ids, runs)
}
