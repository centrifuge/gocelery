package gocelery

import (
	"bytes"
	"context"
	"encoding/gob"
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
	// MaxValidTime signifies how long a job is valid by default
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

type fetchJob struct {
	id  JobID
	job chan *Job
}

type subscribe struct {
	id    JobID
	sub   chan<- *Job
	ok    chan bool
	unsub bool
}

// Dispatcher coordinates with the workers and queue in executing the jobs.
type Dispatcher struct {
	enqueue     chan enqueueJob
	work        chan<- work
	result      <-chan *Job
	fetchJob    chan fetchJob
	storage     Storage
	queue       Queue
	runners     map[string]Runner
	runnerFuncs map[string]RunnerFunc
	workers     []*worker
	finished    chan *Job
	subscribe   chan subscribe
	subscribers map[string][]chan<- *Job
	register    chan register
}

// NewDispatcher for a new Dispatcher instance with a storage and Queue
func NewDispatcher(workerCount int, storage Storage, queue Queue) *Dispatcher {
	if workerCount == 0 {
		workerCount = 1
	}

	enqueue := make(chan enqueueJob)
	work := make(chan work)
	result := make(chan *Job)
	finished := make(chan *Job)
	register := make(chan register)
	fetchJob := make(chan fetchJob)
	subscribe := make(chan subscribe)
	var workers []*worker
	for i := 0; i < workerCount; i++ {
		workers = append(workers, newWorker(work, result))
	}

	return &Dispatcher{
		enqueue:     enqueue,
		work:        work,
		result:      result,
		storage:     storage,
		queue:       queue,
		runners:     make(map[string]Runner),
		runnerFuncs: make(map[string]RunnerFunc),
		workers:     workers,
		finished:    finished,
		register:    register,
		fetchJob:    fetchJob,
		subscribe:   subscribe,
		subscribers: make(map[string][]chan<- *Job),
	}
}

// OnFinished returns a channel that gets updates for all finished tasks
func (d *Dispatcher) OnFinished() <-chan *Job {
	return d.finished
}

// Dispatch dispatches a job and returns a result.
func (d *Dispatcher) Dispatch(job *Job) (Result, error) {
	ej := enqueueJob{
		job: job,
		err: make(chan error),
	}
	d.enqueue <- ej
	err := <-ej.err
	if err != nil {
		return Result{}, err
	}
	return Result{
		JobID:      job.ID,
		Dispatcher: d,
	}, nil
}

// Job returns the Job associated with ID.
func (d *Dispatcher) Job(id JobID) (*Job, error) {
	res := make(chan *Job)
	d.fetchJob <- fetchJob{
		id:  id,
		job: res,
	}
	job := <-res
	if job == nil {
		return nil, ErrNotFound
	}

	return job, nil
}

func (d *Dispatcher) fetchJobByID(id JobID) (*Job, error) {
	res, err := d.storage.Get(fetchKey(id))
	if err != nil {
		return nil, fmt.Errorf("failed to failed job[%x]: %w", id, err)
	}

	dec := gob.NewDecoder(bytes.NewReader(res))
	job := new(Job)
	err = dec.Decode(job)
	if err != nil {
		return nil, fmt.Errorf("failed to decode job: %w", err)
	}

	return job, nil
}

// Subscribe to a specific jobID.
// Once the job is complete, it is pushed to sub
func (d *Dispatcher) Subscribe(jobID JobID, sub chan<- *Job) bool {
	req := subscribe{
		id:    jobID,
		sub:   sub,
		ok:    make(chan bool),
		unsub: false,
	}
	d.subscribe <- req
	return <-req.ok
}

// UnSubscribe from any updates from the jobID
func (d *Dispatcher) UnSubscribe(jobID JobID, sub chan<- *Job) bool {
	req := subscribe{
		id:    jobID,
		sub:   sub,
		ok:    make(chan bool),
		unsub: true,
	}
	d.subscribe <- req
	return <-req.ok
}

// Start initiates the dispatcher functions.
func (d *Dispatcher) Start(ctx context.Context) {
	go d.queue.Start(ctx)

	for _, w := range d.workers {
		go w.start(ctx)
	}

	tick := time.NewTicker(1 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Infof("stopping queue: %v", ctx.Err())
			return
		case sub := <-d.subscribe:
			subs := d.subscribers[sub.id.Hex()]
			if !sub.unsub {
				subs = append(subs, sub.sub)
			} else {
				var ns []chan<- *Job
				for _, s := range subs {
					if s == sub.sub {
						continue
					}
					ns = append(ns, s)
				}
				subs = ns
			}
			d.subscribers[sub.id.Hex()] = subs
			go func() {
				sub.ok <- true
			}()
		case fj := <-d.fetchJob:
			job, err := d.fetchJobByID(fj.id)
			if err != nil {
				log.Error(err)
			}
			fj.job <- job
		case ej := <-d.enqueue:
			err := d.enqueueJob(ej.job)
			go func() {
				ej.err <- err
			}()
		case job := <-d.result:
			err := d.processJob(job)
			if err != nil {
				log.Errorf("failed to process job[%s]: %v", job.HexID(), err)
			}
		case r := <-d.register:
			if r.runner == nil {
				d.runnerFuncs[r.name] = r.runnerFunc
			} else {
				d.runners[r.name] = r.runner
			}
			go func() {
				r.resp <- true
			}()
		case <-tick.C:
			d.pushWork()
		}
	}
}

// RegisterRunner registers a runner
func (d *Dispatcher) RegisterRunner(name string, runner Runner) bool {
	r := register{
		name:   name,
		runner: runner,
		resp:   make(chan bool),
	}
	d.register <- r
	return <-r.resp
}

// RegisterRunnerFunc registers a runnerFunc
func (d *Dispatcher) RegisterRunnerFunc(name string, runnerFunc RunnerFunc) bool {
	r := register{
		name:       name,
		runnerFunc: runnerFunc,
		resp:       make(chan bool),
	}
	d.register <- r
	return <-r.resp
}

func (d *Dispatcher) processJob(job *Job) (err error) {
	if job.isSuccessful() {
		log.Infof("job[%s] completed successfully...", job.HexID())
		return d.jobFinished(job)
	}

	if !job.IsValid() {
		log.Infof("job[%s] expired...", job.HexID())
		return d.jobFinished(job)
	}

	task := job.LastTask()
	// task ran and failed, then increase the delay
	if task.DidRan() && task.Error != "" {
		log.Infof("job[%s]: Task %s failed with error: %s", job.HexID(), task.RunnerFunc, task.Error)
		task.Delay = time.Now().UTC().Add(getBackOffTime(task.Tries))
		log.Infof("job[%s]: will re-try at %s", job.HexID(), task.Delay.String())
	}

	// re enqueue job
	return d.enqueueJob(job)
}

func (d *Dispatcher) jobFinished(job *Job) error {
	d.queue.Finished(job.ID)
	job.FinishedAt = time.Now().UTC()
	job.Finished = true
	err := d.updateJobData(job)
	subs := d.subscribers[job.HexID()]
	subs = append(subs, d.finished)
	for _, sub := range subs {
		go func(sub chan<- *Job) {
			defer func() {
				err := recover()
				if err != nil {
					log.Error(err)
				}
			}()
			t := time.NewTimer(time.Minute)
			select {
			case sub <- job:
			case <-t.C:
				t.Stop()
			}
		}(sub)
	}
	delete(d.subscribers, job.HexID())
	return err
}

func (d *Dispatcher) pushWork() {
	var count int
	defer func() {
		if count > 0 {
			log.Infof("dispatched %d jobs", count)
		}
	}()

	max := len(d.workers) - len(d.work)
	for count < max {
		id, err := d.queue.Dequeue()
		if err != nil {
			return
		}

		job, err := d.fetchJobByID(id)
		if err != nil {
			log.Errorf("failed to fetch job: %v", err)
			continue
		}

		// job is not valid anymore, ack to queue
		if !job.IsValid() {
			log.Infof("job[%s] expired...", job.HexID())
			err = d.jobFinished(job)
			if err != nil {
				log.Errorf("failed to send job finished: %v", err)
			}
			continue
		}

		task := job.LastTask()
		w := work{job: job}
		if job.Runner != "" {
			w.runner = d.runners[job.Runner]
		} else {
			w.runnerFunc = d.runnerFuncs[task.RunnerFunc]
		}

		count++
		log.Infof("dispatching job[%s]", job.HexID())
		go func() {
			d.work <- w
		}()
	}
}

func (d *Dispatcher) updateJobData(job *Job) error {
	key := fetchKey(job.ID)
	var value bytes.Buffer
	enc := gob.NewEncoder(&value)
	err := enc.Encode(job)
	if err != nil {
		return fmt.Errorf("failed to gob encode the job: %w", err)
	}

	return d.storage.Set(key, value.Bytes())
}

func (d *Dispatcher) enqueueJob(job *Job) error {
	err := d.updateJobData(job)
	if err != nil {
		return err
	}

	delay := job.LastTask().Delay
	if delay.IsZero() {
		delay = time.Now().UTC()
	}

	err = d.queue.EnqueueAfter(job.ID, delay)
	if err != nil {
		return err
	}

	log.Infof("job[%s] enqueued...", job.HexID())
	return nil
}

func fetchKey(id JobID) []byte {
	return []byte(fmt.Sprintf("queue-jobs-%s", id.Hex()))
}

type register struct {
	name       string
	runner     Runner
	runnerFunc RunnerFunc
	resp       chan bool
}
