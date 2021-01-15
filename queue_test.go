package gocelery

import (
	"context"
	"encoding/hex"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func testBackend(t *testing.T, runs int, backend Backend) {
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
}

func testBroker(t *testing.T, runs int, broker Broker) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go broker.Start(ctx)
	for i := 0; i < runs; i++ {
		go func(i int) {
			id := randomBytes(32)
			err := broker.Enqueue(id)
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
				gv, err = broker.Dequeue()
				if err != nil {
					continue
				}

				break
			}
			broker.Finished(gv)
			cn <- gv
		}()
	}

	ids := make(map[string]bool)
	for i := 0; i < runs; i++ {
		ids[hex.EncodeToString(<-cn)] = true
	}

	wg.Wait()
	assert.Equal(t, 0, broker.Len())
	assert.Len(t, ids, runs)
}

func newQueue(t *testing.T, workers int) (*Queue, func()) {
	levelDB, closeDB := getLevelDB(t, randomFilePath(t))
	backend := NewLevelDBBackend(levelDB)
	broker := NewLevelDBBroker(levelDB, 1*time.Minute)
	queue := NewQueue(workers, backend, broker)
	return queue, closeDB
}

func TestQueue_Register(t *testing.T) {
	queue, closeF := newQueue(t, 5)
	t.Cleanup(closeF)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go queue.Start(ctx)
	assert.True(t, queue.RegisterRunnerFunc("add", add))
	assert.True(t, queue.RegisterRunner("error", singleRunner{}))
}

type jobResult struct {
	totalRuns int
	result    interface{}
	err       bool
}

func enqueueTasks(t *testing.T, queue *Queue, tasks []string, jobCount int) map[string]jobResult {
	assert.True(t, queue.RegisterRunnerFunc("add", add))
	assert.True(t, queue.RegisterRunnerFunc("error", err))
	assert.True(t, queue.RegisterRunner("singleRunner", singleRunner{}))
	assert.True(t, queue.RegisterRunner("multiRunner", multiRunner{}))

	// enqueue some jobs
	jobs := make(map[string]jobResult)
	for i := 0; i < jobCount; i++ {
		task := tasks[rand.Intn(len(tasks))]
		var args []interface{}
		var or map[string]interface{}
		var res interface{}
		var err bool
		validUntil := time.Now().UTC().Add(5 * time.Second)
		runner := ""
		runnerFunc := task
		switch task {
		case "singleRunner", "multiRunner":
			runner = task
			runnerFunc = "add"
			fallthrough
		case "add":
			a, b := rand.Intn(1000), rand.Intn(1000)
			args = []interface{}{a}
			or = map[string]interface{}{"b": b}
			res = a + b
		case "error":
			err = true
		}

		job := newJob(task, runner, runnerFunc, args, or, validUntil)
		jobs[job.HexID()] = jobResult{
			totalRuns: 1,
			result:    res,
			err:       err,
		}
		assert.NoError(t, queue.Enqueue(job))
	}

	return jobs
}

func verifyJobs(t *testing.T, queue *Queue, jobs map[string]jobResult, jobCount int) {
	assert.Len(t, jobs, jobCount)
	for i := 0; i < jobCount; i++ {
		j := <-queue.OnFinished()
		result := jobs[j.HexID()]
		delete(jobs, j.HexID())
		assert.True(t, j.LastTask().DidRan())
		if result.err {
			assert.False(t, j.IsCompleted())
			assert.False(t, j.IsValid())
			assert.NotEmpty(t, j.LastTask().Error)
			continue
		}
		assert.True(t, j.IsCompleted())
		assert.Equal(t, result.result, j.LastTask().Result)
	}
	assert.Len(t, jobs, 0)
}

// TODO(ved): this test doesn't end sometimes
func TestQueue(t *testing.T) {
	queue, closeF := newQueue(t, 5)
	t.Cleanup(closeF)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go queue.Start(ctx)
	for name, tasks := range map[string][]string{
		"RunnerFunc": {"add", "error"},
		"Runner":     {"singleRunner", "multiRunner"},
	} {
		t.Run(name, func(t *testing.T) {
			jobCount := 10
			jobs := enqueueTasks(t, queue, tasks, jobCount)
			verifyJobs(t, queue, jobs, jobCount)
		})
	}
}

func Test_getBackoffTime(t *testing.T) {
	for i := 1; i < 1000; i++ {
		d := getBackOffTime(uint(i))
		if i < 60 && d.Seconds() != (defaultBackOff*time.Duration(i)).Seconds() {
			t.Fail()
		}

		if i > 60 && d.Seconds() != maxBackoffTime.Seconds() {
			t.Fail()
		}
	}
}

func TestQueue_ValidUntil(t *testing.T) {
	queue, cancel := newQueue(t, 4)
	t.Cleanup(cancel)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go queue.Start(ctx)
	task := "error"
	assert.True(t, queue.RegisterRunnerFunc(task, err))
	job := newJob(task, "", task, nil, nil, time.Now().UTC().Add(10*time.Second))
	now := time.Now()
	assert.NoError(t, queue.Enqueue(job))
	job = <-queue.OnFinished()
	assert.True(t, time.Since(now) > 10*time.Second)
	assert.Len(t, job.Tasks, 1)
	assert.True(t, job.Tasks[0].Tries > 1)
	assert.NotEmpty(t, job.Tasks[0].Error)
}
