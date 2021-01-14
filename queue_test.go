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

// func getBackends(t *testing.T) ([]CeleryBackend, func()) {
// 	levelDB, funcC := getLevelDB(t)
// 	return []CeleryBackend{
// 		NewRedisCeleryBackend("redis://localhost:6379"),
// 		NewLevelDBBackend(levelDB),
// 	}, funcC
// }
//
// // TestGetResult is Redis specific test to get result from backend
// func TestGetResult(t *testing.T) {
// 	backend := NewRedisCeleryBackend("redis://localhost:6379")
// 	taskID := generateUUID()
//
// 	// value must be float64 for testing due to json limitation
// 	value := reflect.ValueOf(rand.Float64())
// 	resultMessage := getReflectionResultMessage(&value)
// 	defer releaseResultMessage(resultMessage)
// 	messageBytes, err := json.Marshal(resultMessage)
// 	if err != nil {
// 		t.Errorf("error marshalling result message: %v", err)
// 	}
// 	conn := backend.(*redisCeleryBackend).Get()
// 	defer conn.Close()
// 	_, err = conn.Do("SETEX", fmt.Sprintf("celery-task-meta-%s", taskID), 86400, messageBytes)
// 	if err != nil {
// 		t.Errorf("error setting result message to celery: %v", err)
// 	}
// 	// get result
// 	res, err := backend.GetResult(taskID)
// 	if err != nil {
// 		t.Errorf("error getting result from backend: %v", err)
// 	}
// 	if !reflect.DeepEqual(res, resultMessage) {
// 		t.Errorf("result message received %v is different from original %v", res, resultMessage)
// 	}
// }
//
// // TestSetResult is Redis specific test to set result to backend
// func TestSetResult(t *testing.T) {
// 	backend := NewRedisCeleryBackend("redis://localhost:6379")
// 	taskID := generateUUID()
// 	value := reflect.ValueOf(rand.Float64())
// 	result := getReflectionResultMessage(&value)
// 	releaseResultMessage(result)
// 	// set result
// 	err := backend.SetResult(taskID, result)
// 	if err != nil {
// 		t.Errorf("error setting result to backend: %v", err)
// 	}
// 	conn := backend.(*redisCeleryBackend).Get()
// 	defer conn.Close()
// 	val, err := conn.Do("GET", fmt.Sprintf("celery-task-meta-%s", taskID))
// 	if err != nil {
// 		t.Errorf("error getting data from redis: %v", err)
// 	}
// 	if val == nil {
// 		t.Errorf("result not available from redis")
// 	}
// 	var res ResultMessage
// 	err = json.Unmarshal(val.([]byte), &res)
// 	if err != nil {
// 		t.Errorf("error parsing json result")
// 	}
// 	if !reflect.DeepEqual(&res, result) {
// 		t.Errorf("result message received %v is different from original %v", &res, result)
// 	}
// }
//
// // TestSetGetResult tests set/get result feature for all backends
// func TestSetGetResult(t *testing.T) {
// 	backends, funcC := getBackends(t)
// 	defer funcC()
// 	for _, backend := range backends {
// 		taskID := generateUUID()
// 		value := reflect.ValueOf(rand.Float64())
// 		resultMessage := getReflectionResultMessage(&value)
// 		// set result
// 		err := backend.SetResult(taskID, resultMessage)
// 		if err != nil {
// 			t.Errorf("error setting result to backend: %v", err)
// 		}
// 		// get result
// 		res, err := backend.GetResult(taskID)
// 		if err != nil {
// 			t.Errorf("error getting result from backend: %v", err)
// 		}
// 		if !reflect.DeepEqual(res, resultMessage) {
// 			t.Errorf("result message received %v is different from original %v", res, resultMessage)
// 		}
//
// 		releaseResultMessage(resultMessage)
// 	}
// }

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
	ctx, cancel := context.WithCancel(context.Background())
	go queue.Start(ctx)
	return queue, func() {
		cancel()
		closeDB()
	}
}

func TestQueue_Register(t *testing.T) {
	queue, closeF := newQueue(t, 5)
	defer closeF()
	assert.True(t, queue.RegisterRunnerFunc("add", add))
	assert.True(t, queue.RegisterRunner("error", singleRunner{}))
}

func TestQueue_RunnerFunc_Basic(t *testing.T) {
	queue, closeF := newQueue(t, 5)
	defer closeF()
	assert.True(t, queue.RegisterRunnerFunc("add", add))
	assert.True(t, queue.RegisterRunnerFunc("error", err))

	// enqueue some jobs
	tasks := []string{"add", "error"}
	jobCount := 10
	type jobResult struct {
		totalRuns int
		result    interface{}
		err       bool
	}
	jobs := make(map[string]jobResult)
	for i := 0; i < jobCount; i++ {
		task := tasks[rand.Intn(len(tasks))]
		var args []interface{}
		var or map[string]interface{}
		var res interface{}
		var err bool
		var validUntil time.Time
		switch task {
		case "add":
			a, b := rand.Intn(1000), rand.Intn(1000)
			args = []interface{}{a}
			or = map[string]interface{}{"b": b}
			res = a + b
		default:
			err = true
			validUntil = time.Now().UTC().Add(5 * time.Second)
		}

		job := newJob(task, "", task, args, or, validUntil)
		jobs[job.HexID()] = jobResult{
			totalRuns: 1,
			result:    res,
			err:       err,
		}
		assert.NoError(t, queue.Enqueue(job))
	}

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
	// TODO(ved): this test runner is not stopping sometimes
}
