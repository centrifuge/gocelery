package gocelery

import (
	"context"
	"encoding/hex"
	"sync"
	"testing"

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
