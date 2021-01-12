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

// levelDBBackend implements the CeleryBackend based on levelDB
type levelDBBackend struct {
	db *leveldb.DB
}

// NewLevelDBBackend returns an levelDB implementation of CeleryBackend
func NewLevelDBBackend(db *leveldb.DB) Backend {
	return levelDBBackend{db: db}
}

func (l levelDBBackend) Get(key []byte) ([]byte, error) {
	return l.db.Get(key, nil)
}

func (l levelDBBackend) Set(key, value []byte) error {
	return l.db.Put(key, value, nil)
}

func (l levelDBBackend) Delete(key []byte) error {
	return l.db.Delete(key, nil)
}

var (
	queuePrefix = "broker-queue-"
	ackPrefix   = "broker-ack-"
)

// levelDBBroker implements CeleryBroker backed by levelDB
type levelDBBroker struct {
	db             *leveldb.DB
	head           int
	tail           int
	dispatched     map[string]time.Time
	requeueTimeout time.Duration

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
		dispatched:     make(map[string]time.Time),
	}

	// restore the old state from the DB
	brk.loadQueueState()
	brk.enqueueUnAckJobs()
	return brk
}

func (l *levelDBBroker) timeouts() [][]byte {
	l.lock.RLock()
	defer l.lock.RUnlock()
	var values [][]byte
	for v, t := range l.dispatched {
		if time.Since(t) <= l.requeueTimeout {
			continue
		}

		value, err := hex.DecodeString(v)
		if err != nil {
			log.Error(err)
			continue
		}

		values = append(values, value)
	}

	return values
}

func (l *levelDBBroker) Start(ctx context.Context) error {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			values := l.timeouts()
			for _, v := range values {
				err := l.Enqueue(v)
				if err != nil {
					log.Error(err)
					continue
				}
			}
		}
	}
}

func fetchNumber(s string) int {
	d := strings.Split(s, "-")
	i := d[2]
	v, _ := strconv.Atoi(i)
	return v
}

// loadQueueState loads any previous state if any available
// logs any state retrieval errors
func (l *levelDBBroker) loadQueueState() {
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

func (l *levelDBBroker) enqueueUnAckJobs() {
	// enqueue if there are any unacknowledged
	prefix := []byte(ackPrefix)
	itr := l.db.NewIterator(util.BytesPrefix(prefix), nil)
	defer itr.Release()
	var values [][]byte
	for itr.Next() {
		value := append([]byte{}, itr.Value()...)
		values = append(values, value)
	}

	for _, value := range values {
		value := value
		err := l.Enqueue(value)
		if err != nil {
			log.Error(err)
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
	delete(l.dispatched, v)
	l.tail++
	return nil
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

	ackKey := fmt.Sprintf("%s%s", ackPrefix, hex.EncodeToString(v))
	err = txn.Put([]byte(ackKey), v, nil)
	if err != nil {
		return nil, err
	}

	l.dispatched[hex.EncodeToString(v)] = time.Now()
	l.head++
	return v, nil
}

func (l *levelDBBroker) Finished(id []byte) {
	l.lock.Lock()
	defer l.lock.Unlock()
	v := hex.EncodeToString(id)
	ackKey := fmt.Sprintf("%s%s", ackPrefix, v)
	err := l.db.Delete([]byte(ackKey), nil)
	if err != nil {
		log.Error(err)
		return
	}

	delete(l.dispatched, v)
}
