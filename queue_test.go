package gocelery

import (
	"context"
	"encoding/hex"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestQueue(t *testing.T) {
	db, canc := getLevelDB(t, randomFilePath(t))
	defer canc()
	queue := NewQueue(NewLevelDBStorage(db), 0)
	runs := 100
	testQueueEnqueueNow(t, runs, queue)
}

func testQueueEnqueueNow(t *testing.T, runs int, queue Queue) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go queue.Start(ctx)
	for i := 0; i < runs; i++ {
		go func(i int) {
			id := randomBytes(32)
			err := queue.EnqueueNow(id)
			assert.NoError(t, err)
		}(i)
	}

	wg := sync.WaitGroup{}
	cn := make(chan []byte, runs)
	for i := 0; i < runs; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var gv []byte
			var err error
			for {
				gv, err = queue.Dequeue()
				if err != nil {
					continue
				}

				break
			}
			queue.Finished(gv)
			cn <- gv
		}()
	}

	ids := make(map[string]bool)
	for i := 0; i < runs; i++ {
		ids[hex.EncodeToString(<-cn)] = true
	}

	wg.Wait()
	assert.Equal(t, 0, queue.Len())
	assert.Len(t, ids, runs)
}

func dequeue(t *testing.T, broker Queue, count int, ack bool) [][]byte {
	// dequeue half the queue and wait for more than 2 seconds without ack
	var ids [][]byte
	idChan := make(chan []byte)
	for i := 0; i < count; i++ {
		go func(i int) {
			for {
				v, err := broker.Dequeue()
				if err != nil {
					assert.Error(t, err, ErrNotFound)
					continue
				}

				if ack {
					broker.Finished(v)
				}

				idChan <- v
				return
			}
		}(i)
	}

	for i := 0; i < count; i++ {
		ids = append(ids, <-idChan)
	}
	assert.Len(t, ids, count)
	return ids
}

func TestQueue_State(t *testing.T) {
	path := randomFilePath(t)
	db, _ := getLevelDB(t, path)
	queue := NewQueue(NewLevelDBStorage(db), 5*time.Second)
	ctx, c := context.WithCancel(context.Background())
	go queue.Start(ctx)

	runs := 100
	wg := sync.WaitGroup{}
	for i := 0; i < runs; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := randomBytes(32)
			err := queue.EnqueueNow(id)
			assert.NoError(t, err)
		}(i)
	}
	wg.Wait()
	assert.Equal(t, runs, queue.Len())

	// dequeue half the queue and wait for more than 10 seconds without ack
	dequeue(t, queue, runs/2, false)
	time.Sleep(10 * time.Second)
	assert.Equal(t, runs, queue.Len())
	c()

	// restart the queue should have all enqueued
	queue = NewQueue(NewLevelDBStorage(db), 5*time.Second)
	ctx, c = context.WithCancel(context.Background())
	go queue.Start(ctx)
	assert.Equal(t, runs, queue.Len())

	// dequeue half
	dequeue(t, queue, runs/2, false)
	assert.True(t, queue.Len() >= runs/2)
	c()

	// restart again and all the un ack should be enqueued
	queue = NewQueue(NewLevelDBStorage(db), 5*time.Second)
	ctx, c = context.WithCancel(context.Background())
	go queue.Start(ctx)
	time.Sleep(10 * time.Second)
	assert.Equal(t, runs, queue.Len())

	// dequeue and ack as well
	dequeue(t, queue, runs/2, true)
	assert.Equal(t, runs/2, queue.Len())
	c()
	db.Close()

	// restart should have only half now
	db, canc := getLevelDB(t, path)
	defer canc()
	queue = NewQueue(NewLevelDBStorage(db), 5*time.Second)
	assert.Equal(t, runs/2, queue.Len())
}
