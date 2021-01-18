package gocelery

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	logging "github.com/ipfs/go-log"
	"github.com/stretchr/testify/assert"
)

func init() {
	logging.SetAllLoggers(logging.LevelFatal)
}

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

func TestDispatcher_Register(t *testing.T) {
	queue := newDispatcher(t, 5)
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

func enqueueTasks(t *testing.T, queue *Dispatcher, tasks []string, jobCount int) map[string]jobResult {
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
		assert.NoError(t, queue.Dispatch(job))
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

func TestDispatcher(t *testing.T) {
	dispatcher := newDispatcher(t, 10)
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
	assert.NoError(t, dispatcher.Dispatch(job))
	job = <-dispatcher.OnFinished()
	assert.True(t, time.Since(now) >= 10*time.Second)
	assert.Len(t, job.Tasks, 1)
	assert.True(t, job.Tasks[0].Tries > 1)
	assert.NotEmpty(t, job.Tasks[0].Error)
}

func BenchmarkDispatcherInMemory10Workers(b *testing.B) {
	benchmarkDispatcher(b, true)
}

func BenchmarkDispatcherLevelDB10Workers(b *testing.B) {
	benchmarkDispatcher(b, false)
}

func benchmarkDispatcher(b *testing.B, inmemory bool) {
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
