package gocelery

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/rand"
	"testing"
	"time"

	logging "github.com/ipfs/go-log"
	"github.com/stretchr/testify/assert"
)

func newDispatcher(t *testing.T, workers int) *Dispatcher {
	levelDB, _ := getLevelDB(t, randomFilePath(t))
	storage := NewLevelDBStorage(levelDB)
	queue := NewQueue(storage, 30*time.Minute)
	dispatcher := NewDispatcher(workers, storage, queue)
	return dispatcher
}

func newDispatcherInmemory(t *testing.T, workers int) *Dispatcher {
	storage := NewInMemoryStorage()
	queue := NewQueue(storage, 30*time.Minute)
	dispatcher := NewDispatcher(workers, storage, queue)
	return dispatcher
}

func TestDispatcher_Register_Subscribe(t *testing.T) {
	dispatcher := newDispatcher(t, 5)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go dispatcher.Start(ctx)
	assert.True(t, dispatcher.RegisterRunnerFunc("add", add))
	assert.True(t, dispatcher.RegisterRunner("error", singleRunner{}))
	id := randomBytes(32)
	cn := make(chan *Job)
	assert.True(t, dispatcher.Subscribe(id, cn))
	assert.Len(t, dispatcher.subscribers[hex.EncodeToString(id)], 1)
	assert.True(t, dispatcher.UnSubscribe(id, cn))
	assert.Len(t, dispatcher.subscribers[hex.EncodeToString(id)], 0)
}

type jobResult struct {
	totalRuns int
	result    interface{}
	err       bool
}

func enqueueTasks(t *testing.T, dispatcher *Dispatcher, tasks []string, jobCount int) map[string]jobResult {
	assert.True(t, dispatcher.RegisterRunnerFunc("add", add))
	assert.True(t, dispatcher.RegisterRunnerFunc("error", err))
	assert.True(t, dispatcher.RegisterRunner("singleRunner", singleRunner{}))
	assert.True(t, dispatcher.RegisterRunner("multiRunner", multiRunner{}))

	// enqueue some jobs
	jobs := make(map[string]jobResult)
	for i := 0; i < jobCount; i++ {
		task := tasks[rand.Intn(len(tasks))]
		var args []interface{}
		var or map[string]interface{}
		var res interface{}
		var errored bool
		validUntil := time.Now().UTC().Add(5 * time.Second)
		runner := ""
		runnerFunc := task
		switch task {
		case "multiRunner":
			errored = true
			fallthrough
		case "singleRunner":
			runner = task
			runnerFunc = "add"
			fallthrough
		case "add":
			a, b := rand.Intn(1000), rand.Intn(1000)
			args = []interface{}{a}
			or = map[string]interface{}{"b": b}
			res = a + b
		case "error":
			errored = true
		}

		job := newJob(task, runner, runnerFunc, args, or, validUntil)
		jobs[job.HexID()] = jobResult{
			totalRuns: 1,
			result:    res,
			err:       errored,
		}
		result, err := dispatcher.Dispatch(job)
		assert.NoError(t, err)
		assert.NotEmpty(t, result)
	}

	return jobs
}

func verifyJobs(t *testing.T, queue *Dispatcher, jobs map[string]jobResult, jobCount int) {
	assert.Len(t, jobs, jobCount)
	for i := 0; i < jobCount; i++ {
		j := <-queue.OnFinished()
		result := jobs[j.HexID()]
		delete(jobs, j.HexID())
		assert.True(t, j.LastTask().DidRan())
		if result.err {
			assert.False(t, j.IsSuccessful())
			assert.False(t, j.IsValid())
			assert.NotEmpty(t, j.LastTask().Error)
			continue
		}
		assert.True(t, j.IsSuccessful())
		assert.Equal(t, result.result, j.LastTask().Result)
	}
	assert.Len(t, jobs, 0)
}

func TestDispatcher(t *testing.T) {
	dispatcher := newDispatcher(t, 100)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go dispatcher.Start(ctx)
	for name, tasks := range map[string][]string{
		"RunnerFunc": {"add", "error"},
		"Runner":     {"singleRunner", "multiRunner"},
	} {
		t.Run(name, func(t *testing.T) {
			jobCount := 1000
			jobs := enqueueTasks(t, dispatcher, tasks, jobCount)
			verifyJobs(t, dispatcher, jobs, jobCount)
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

func TestDispatcher_ValidUntil(t *testing.T) {
	dispatcher := newDispatcher(t, 4)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go dispatcher.Start(ctx)
	task := "error"
	assert.True(t, dispatcher.RegisterRunnerFunc(task, err))
	job := newJob(task, "", task, nil, nil, time.Now().UTC().Add(10*time.Second))
	now := time.Now()
	result, err := dispatcher.Dispatch(job)
	assert.NoError(t, err)
	assert.NotEmpty(t, result)
	job = <-dispatcher.OnFinished()
	assert.True(t, time.Since(now) >= 10*time.Second)
	assert.Len(t, job.Tasks, 1)
	assert.True(t, job.Tasks[0].Tries > 1)
	assert.NotEmpty(t, job.Tasks[0].Error)
}

func TestResult_Await(t *testing.T) {
	dispatcher := newDispatcher(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	go dispatcher.Start(ctx)
	task := "add"
	assert.True(t, dispatcher.RegisterRunnerFunc(task, add))
	job := newJob(task, "", task, []interface{}{1}, map[string]interface{}{"b": 2},
		time.Now().UTC().Add(10*time.Second))
	result, err := dispatcher.Dispatch(job)
	assert.NoError(t, err)
	assert.NotEmpty(t, result)
	res, err := result.Await(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 3, res)
}

func BenchmarkDispatcherInMemory10Workers(b *testing.B) {
	benchmarkDispatcher(b, true)
}

func BenchmarkDispatcherLevelDB10Workers(b *testing.B) {
	benchmarkDispatcher(b, false)
}

func benchmarkDispatcher(b *testing.B, inmemory bool) {
	logging.SetAllLoggers(logging.LevelFatal)
	variants := []struct {
		name     string
		jobCount int
	}{
		{
			name:     "10Jobs",
			jobCount: 10,
		},
		{
			name:     "100Jobs",
			jobCount: 100,
		},
		{
			name:     "1000Jobs",
			jobCount: 1000,
		},
		{
			name:     "10000Jobs",
			jobCount: 10000,
		},
	}
	dispatcher := newDispatcherInmemory(&testing.T{}, 10)
	if !inmemory {
		dispatcher = newDispatcher(&testing.T{}, 10)
	}

	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(cancel)
	go dispatcher.Start(ctx)
	for _, v := range variants {
		for _, tasks := range []string{"add", "error", "singleRunner", "multiRunner"} {
			b.Run(fmt.Sprintf("%s_%s", v.name, tasks), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					jobs := enqueueTasks(new(testing.T), dispatcher, []string{tasks}, v.jobCount)
					verifyJobs(new(testing.T), dispatcher, jobs, v.jobCount)
				}
			})
		}
	}
}
