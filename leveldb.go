package gocelery

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

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

var (
	queuePrefix = "broker-queue-"
	ackPrefix   = "broker-ack-"
)

// levelDBBroker implements CeleryBroker backed by levelDB
type levelDBBroker struct {
	db             *leveldb.DB
	requeueTimeout time.Duration

	head int
	tail int
	lock sync.RWMutex // protect head and tail
}

// NewLevelDBBroker returns levelDB backed implementation of Broker
// We will also initialise any previous state
// if timeout is 0, then defaults to 30 minutes
func NewLevelDBBroker(db *leveldb.DB, reQueueTimeout time.Duration) Broker {
	if reQueueTimeout < 1 {
		reQueueTimeout = time.Minute * 30
	}

	brk := &levelDBBroker{
		db:             db,
		head:           0,
		tail:           0,
		requeueTimeout: reQueueTimeout,
	}

	// restore the old state from the DB
	brk.loadHeadTail()
	return brk
}

func (l *levelDBBroker) Start(ctx context.Context) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Errorf("stopping broker: %v", ctx.Err())
			return
		case <-tick.C:
			ids := l.timeouts()
			for _, v := range ids {
				err := l.Enqueue(v)
				if err != nil {
					log.Error(err)
					continue
				}
			}
		}
	}
}

// Len returns the current items in the queue
func (l *levelDBBroker) Len() int {
	l.lock.RLock()
	defer l.lock.RUnlock()
	c := l.tail - l.head
	if c < 0 {
		c = 0
	}

	return c
}

func (l *levelDBBroker) Enqueue(id []byte) error {
	l.lock.Lock()
	defer l.lock.Unlock()
	// add the message to the queue
	key := fmt.Sprintf("%s%d", queuePrefix, l.tail)
	err := l.db.Put([]byte(key), id, nil)
	if err != nil {
		return err
	}

	// delete the message from un ack
	v := hex.EncodeToString(id)
	ackKey := fmt.Sprintf("%s%s", ackPrefix, v)
	l.db.Delete([]byte(ackKey), nil)
	l.tail++
	return nil
}

func (l *levelDBBroker) EnqueueAfter(id []byte, t time.Time) error {
	d, err := t.UTC().MarshalBinary()
	if err != nil {
		return err
	}

	ackKey := fmt.Sprintf("%s%s", ackPrefix, hex.EncodeToString(id))
	return l.db.Put([]byte(ackKey), d, nil)
}

func (l *levelDBBroker) Dequeue() ([]byte, error) {
	l.lock.Lock()
	defer l.lock.Unlock()
	txn, err := l.db.OpenTransaction()
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			txn.Discard()
			return
		}

		err = txn.Commit()
		if err != nil {
			return
		}
		l.head++
	}()

	key := fmt.Sprintf("%s%d", queuePrefix, l.head)
	v, err := txn.Get([]byte(key), nil)
	if err != nil {
		return nil, err
	}

	err = txn.Delete([]byte(key), nil)
	if err != nil {
		return nil, err
	}

	d, err := time.Now().UTC().Add(l.requeueTimeout).MarshalBinary()
	if err != nil {
		return nil, err
	}

	ackKey := fmt.Sprintf("%s%s", ackPrefix, hex.EncodeToString(v))
	err = txn.Put([]byte(ackKey), d, nil)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (l *levelDBBroker) Finished(id []byte) {
	v := hex.EncodeToString(id)
	ackKey := fmt.Sprintf("%s%s", ackPrefix, v)
	err := l.db.Delete([]byte(ackKey), nil)
	if err != nil {
		log.Error(err)
		return
	}
}

func (l *levelDBBroker) timeouts() [][]byte {
	prefix := []byte(ackPrefix)
	itr := l.db.NewIterator(util.BytesPrefix(prefix), nil)
	defer itr.Release()
	var ids [][]byte
	var count int
	for itr.Next() {
		count++
		id := fetchBytes(string(itr.Key()))
		t := new(time.Time)
		err := t.UnmarshalBinary(itr.Value())
		if err != nil {
			log.Errorf("failed to unmarshall time: %v", err)
			continue
		}

		if time.Now().UTC().Before(t.UTC()) {
			continue
		}

		ids = append(ids, id)
	}
	return ids
}

func fetchNumber(s string) int {
	d := strings.Split(s, "-")
	i := d[2]
	v, _ := strconv.Atoi(i)
	return v
}

func fetchBytes(s string) []byte {
	d := strings.Split(s, "-")
	i := d[2]
	v, _ := hex.DecodeString(i)
	return v
}

// loadHeadTail loads any previous state if any available
// logs any state retrieval errors
func (l *levelDBBroker) loadHeadTail() {
	prefix := []byte(queuePrefix)
	itr := l.db.NewIterator(util.BytesPrefix(prefix), nil)
	defer itr.Release()
	var nums []int
	for itr.Next() {
		key := string(itr.Key())
		num := fetchNumber(key)
		nums = append(nums, num)
	}

	if len(nums) < 1 {
		return
	}

	sort.Ints(nums)
	head := nums[0]
	tail := head
	if len(nums) > 1 {
		tail = nums[len(nums)-1]
	}

	if head > tail {
		tail = head
	}

	if tail > 0 {
		tail++
	}

	l.head = head
	l.tail = tail
}
