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
)

// Queue is a message queue
type Queue interface {
	Start(ctx context.Context)
	EnqueueNow(id []byte) error
	EnqueueAfter(id []byte, t time.Time) error
	Dequeue() ([]byte, error)
	Finished(id []byte)
	Len() int
}

// levelDBBroker implements CeleryBroker backed by levelDB
type queue struct {
	storage        Storage
	requeueTimeout time.Duration
	queuePrefix    string
	queueAckPrefix string

	head int
	tail int
	lock sync.RWMutex // protect head and tail
}

// NewQueue returns an implementation of Queue
// if timeout is 0, then defaults to 30 minutes
func NewQueue(storage Storage, reQueueTimeout time.Duration) Queue {
	if reQueueTimeout < 1 {
		reQueueTimeout = time.Minute * 30
	}

	name := "gocelery"
	q := &queue{
		storage:        storage,
		head:           0,
		tail:           0,
		requeueTimeout: reQueueTimeout,
		queuePrefix:    fmt.Sprintf("%s-queue-", name),
		queueAckPrefix: fmt.Sprintf("%s-ack-", name),
	}

	// restore the old state from the DB
	q.loadHeadTail()
	return q
}

func (q *queue) Start(ctx context.Context) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Debugf("stopping queue: %v", ctx.Err())
			return
		case <-tick.C:
			ids := q.timeouts()
			for _, v := range ids {
				err := q.EnqueueNow(v)
				if err != nil {
					log.Error(err)
					continue
				}
			}
		}
	}
}

// Len returns the current items in the queue
func (q *queue) Len() int {
	q.lock.RLock()
	defer q.lock.RUnlock()
	c := q.tail - q.head
	if c < 0 {
		c = 0
	}

	return c
}

func (q *queue) EnqueueNow(id []byte) error {
	q.lock.Lock()
	defer q.lock.Unlock()

	// add the message to the queue
	key := fmt.Sprintf("%s%d", q.queuePrefix, q.tail)
	err := q.storage.Set([]byte(key), id)
	if err != nil {
		return err
	}

	// delete the message from un ack
	v := hex.EncodeToString(id)
	ackKey := fmt.Sprintf("%s%s", q.queueAckPrefix, v)
	err = q.storage.Delete([]byte(ackKey))
	if err != nil {
		return err
	}

	q.tail++
	return nil
}

func (q *queue) EnqueueAfter(id []byte, t time.Time) error {
	d, err := t.UTC().MarshalBinary()
	if err != nil {
		return err
	}

	ackKey := fmt.Sprintf("%s%s", q.queueAckPrefix, hex.EncodeToString(id))
	return q.storage.Set([]byte(ackKey), d)
}

func (q *queue) Dequeue() ([]byte, error) {
	q.lock.Lock()
	defer q.lock.Unlock()

	key := fmt.Sprintf("%s%d", q.queuePrefix, q.head)
	v, err := q.storage.Get([]byte(key))
	if err != nil {
		return nil, err
	}

	d, err := time.Now().UTC().Add(q.requeueTimeout).MarshalBinary()
	if err != nil {
		return nil, err
	}

	ackKey := fmt.Sprintf("%s%s", q.queueAckPrefix, hex.EncodeToString(v))
	err = q.storage.Set([]byte(ackKey), d)
	if err != nil {
		return nil, err
	}

	err = q.storage.Delete([]byte(key))
	if err != nil {
		return nil, err
	}
	q.head++
	return v, nil
}

func (q *queue) Finished(id []byte) {
	v := hex.EncodeToString(id)
	ackKey := fmt.Sprintf("%s%s", q.queueAckPrefix, v)
	err := q.storage.Delete([]byte(ackKey))
	if err != nil {
		log.Error(err)
		return
	}
}

func (q *queue) timeouts() [][]byte {
	prefix := []byte(q.queueAckPrefix)
	var ids [][]byte
	q.storage.Iterate(prefix, func(key, value []byte) {
		id := fetchBytes(string(key))
		t := new(time.Time)
		err := t.UnmarshalBinary(value)
		if err != nil {
			log.Errorf("failed to unmarshall time: %v", err)
			return
		}

		if time.Now().UTC().Before(t.UTC()) {
			return
		}

		ids = append(ids, id)
	})
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
func (q *queue) loadHeadTail() {
	prefix := []byte(q.queuePrefix)
	var nums []int
	q.storage.Iterate(prefix, func(key, value []byte) {
		num := fetchNumber(string(key))
		nums = append(nums, num)
	})

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

	q.head = head
	q.tail = tail
}
