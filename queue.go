package gocelery

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	logging "github.com/ipfs/go-log"
)

var (
	// ErrNotFound when the a requested resource is not found
	ErrNotFound = errors.New("not found")
)

const (
	// MaxValidTime signifies how long is valid by default
	// Max set to 12 hrs
	MaxValidTime = 12 * time.Hour

	// defaultBackOff with current tries for back off
	defaultBackOff = time.Second * 5

	// maxBackOffTime to backoff at a delay
	maxBackoffTime = time.Minute * 5
)

func init() {
	gob.Register(Job{})
}

var log = logging.Logger("gocelery")

func getBackOffTime(tries uint) time.Duration {
	d := defaultBackOff * time.Duration(tries)
	if d.Minutes() > maxBackoffTime.Minutes() {
		return maxBackoffTime
	}

	return d
}

type enqueueJob struct {
	job *Job
	err chan error
}

type Queue struct {
	enqueue     chan enqueueJob
	work        chan<- work
	result      <-chan *Job
	storage     Storage
	broker      Broker
	runners     map[string]Runner
	runnerFuncs map[string]RunnerFunc
	workers     []*worker
	finished    chan *Job
	register    chan register
	close       func()
}

func NewQueue(workerCount int, backend Storage, broker Broker) *Queue {
	if workerCount == 0 {
		workerCount = 1
	}

	enqueue := make(chan enqueueJob)
	work := make(chan work)
	result := make(chan *Job)
	finished := make(chan *Job)
	register := make(chan register)
	var workers []*worker
	for i := 0; i < workerCount; i++ {
		workers = append(workers, newWorker(work, result))
	}

	return &Queue{
		enqueue:     enqueue,
		work:        work,
		result:      result,
		storage:     backend,
		broker:      broker,
		runners:     make(map[string]Runner),
		runnerFuncs: make(map[string]RunnerFunc),
		workers:     workers,
		finished:    finished,
		register:    register,
		close: func() {
			close(enqueue)
			close(work)
			close(result)
			close(register)
			close(finished)
		},
	}
}

func (q *Queue) OnFinished() <-chan *Job {
	return q.finished
}

func (q *Queue) Enqueue(job *Job) error {
	ej := enqueueJob{
		job: job,
		err: make(chan error),
	}
	q.enqueue <- ej
	return <-ej.err
}

func (q *Queue) Start(ctx context.Context) {
	go q.broker.Start(ctx)

	for _, w := range q.workers {
		go w.start(ctx)
	}

	tick := time.NewTicker(1 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Errorf("stopping queue: %v", ctx.Err())
			q.close()
			return
		case ej := <-q.enqueue:
			err := q.enqueueJob(ej.job)
			go func() {
				ej.err <- err
			}()
		case job := <-q.result:
			err := q.processJob(job)
			if err != nil {
				log.Errorf("failed to process job[%s]: %v", job.HexID(), err)
			}
		case r := <-q.register:
			if r.runner == nil {
				q.runnerFuncs[r.name] = r.runnerFunc
			} else {
				q.runners[r.name] = r.runner
			}
			go func() {
				r.resp <- true
			}()
		case <-tick.C:
			q.pushWork()
		}
	}
}

func (q *Queue) RegisterRunner(name string, runner Runner) bool {
	r := register{
		name:   name,
		runner: runner,
		resp:   make(chan bool),
	}
	q.register <- r
	return <-r.resp
}

func (q *Queue) RegisterRunnerFunc(name string, runnerFunc RunnerFunc) bool {
	r := register{
		name:       name,
		runnerFunc: runnerFunc,
		resp:       make(chan bool),
	}
	q.register <- r
	return <-r.resp
}

func (q *Queue) processJob(job *Job) (err error) {
	if job.IsCompleted() {
		log.Debugf("job[%s] completed successfully...", job.HexID())
		return q.jobFinished(job)
	}

	if !job.IsValid() {
		log.Debugf("job[%s] expired...", job.HexID())
		return q.jobFinished(job)
	}

	task := job.LastTask()
	// task ran and failed, then increase the delay
	if task.DidRan() && task.Error != "" {
		task.Delay = time.Now().UTC().Add(getBackOffTime(task.Tries))
	}

	// re enqueue job
	return q.enqueueJob(job)
}

func (q *Queue) jobFinished(job *Job) error {
	q.broker.Finished(job.ID)
	go func() {
		q.finished <- job
	}()
	return q.updateJobData(job)
}

func (q *Queue) pushWork() {
	id, err := q.broker.Dequeue()
	if err != nil {
		return
	}

	res, err := q.storage.Get(q.fetchKey(id))
	if err != nil {
		log.Errorf("failed to get job from storage: %v", err)
		return
	}

	dec := gob.NewDecoder(bytes.NewReader(res))
	job := new(Job)
	err = dec.Decode(job)
	if err != nil {
		log.Errorf("failed to decode job: %v", err)
		return
	}

	// job is not valid anymore, ack to broker
	if !job.IsValid() {
		log.Errorf("job[%s] expired...", job.HexID())
		err = q.jobFinished(job)
		if err != nil {
			log.Errorf("failed to send job finished: %v", err)
		}
		return
	}

	task := job.LastTask()
	// if task is not ready to be dispatched
	// TODO(ved): broker should take care of not sending until ready
	if !task.IsRunnable() {
		err = q.enqueueJob(job)
		if err != nil {
			log.Errorf("failed to enqueue job[%s]: %v", job.HexID(), err)
		}
		return
	}

	w := work{
		job:        job,
		runner:     q.runners[job.Runner],
		runnerFunc: q.runnerFuncs[task.RunnerFunc],
	}

	log.Debugf("dispatching job[%s]", job.HexID())
	go func() {
		q.work <- w
	}()
}

func (q *Queue) updateJobData(job *Job) error {
	key := q.fetchKey(job.ID)
	var value bytes.Buffer
	enc := gob.NewEncoder(&value)
	err := enc.Encode(job)
	if err != nil {
		return fmt.Errorf("failed to gob encode the job: %w", err)
	}

	return q.storage.Set(key, value.Bytes())
}

func (q *Queue) enqueueJob(job *Job) error {
	err := q.updateJobData(job)
	if err != nil {
		return err
	}

	err = q.broker.Enqueue(job.ID)
	if err != nil {
		return err
	}

	log.Debugf("job[%s] enqueued...", job.HexID())
	return nil
}

func (q *Queue) fetchKey(id []byte) []byte {
	return []byte(fmt.Sprintf("queue-jobs-%s", hex.EncodeToString(id)))
}

type register struct {
	name       string
	runner     Runner
	runnerFunc RunnerFunc
	resp       chan bool
}
