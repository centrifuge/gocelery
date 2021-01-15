package gocelery

import (
	"context"
	cr "crypto/rand"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb"
)

func randomFilePath(t *testing.T) string {
	r := make([]byte, 32)
	n, err := cr.Read(r)
	assert.Nil(t, err)
	assert.Equal(t, n, 32)
	return fmt.Sprintf("%s_%x", "/tmp/data.leveldb_TESTING", r)
}

func getLevelDB(t *testing.T, path string) (*leveldb.DB, func()) {
	if path == "" {
		path = randomFilePath(t)
	}
	levelDB, err := leveldb.OpenFile(path, nil)
	if err != nil {
		t.Fatalf("failed to open levelDb: %v", err)
	}

	return levelDB, func() {
		levelDB.Close()
		os.Remove(path)
	}
}

func TestNewLevelDBStorage(t *testing.T) {
	db, canc := getLevelDB(t, randomFilePath(t))
	defer canc()
	storage := NewLevelDBStorage(db)
	runs := 1000
	testStorage(t, runs, storage)
}

func TestNewLevelDBBroker(t *testing.T) {
	db, canc := getLevelDB(t, randomFilePath(t))
	defer canc()
	broker := NewLevelDBBroker(db, 0)
	runs := 100
	testBroker(t, runs, broker)
}

func dequeue(t *testing.T, broker Broker, count int, ack bool) [][]byte {
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

func TestLevelDBBroker_State(t *testing.T) {
	path := randomFilePath(t)
	db, _ := getLevelDB(t, path)
	broker := NewLevelDBBroker(db, 5*time.Second)
	ctx, c := context.WithCancel(context.Background())
	go broker.Start(ctx)

	runs := 100
	wg := sync.WaitGroup{}
	for i := 0; i < runs; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := randomBytes(32)
			err := broker.Enqueue(id)
			assert.NoError(t, err)
		}(i)
	}
	wg.Wait()
	assert.Equal(t, runs, broker.Len())

	// dequeue half the queue and wait for more than 10 seconds without ack
	dequeue(t, broker, runs/2, false)
	time.Sleep(10 * time.Second)
	assert.Equal(t, runs, broker.Len())
	c()

	// restart the queue should have all enqueued
	broker = NewLevelDBBroker(db, 5*time.Second)
	ctx, c = context.WithCancel(context.Background())
	go broker.Start(ctx)
	assert.Equal(t, runs, broker.Len())

	// dequeue half
	dequeue(t, broker, runs/2, false)
	assert.True(t, broker.Len() >= runs/2)
	c()

	// restart again and all the un ack should be enqueued
	broker = NewLevelDBBroker(db, 5*time.Second)
	ctx, c = context.WithCancel(context.Background())
	go broker.Start(ctx)
	time.Sleep(10 * time.Second)
	assert.Equal(t, runs, broker.Len())

	// dequeue and ack as well
	dequeue(t, broker, runs/2, true)
	assert.Equal(t, runs/2, broker.Len())
	c()
	db.Close()

	// restart should have only half now
	db, canc := getLevelDB(t, path)
	defer canc()
	broker = NewLevelDBBroker(db, 5*time.Second)
	assert.Equal(t, runs/2, broker.Len())
}
