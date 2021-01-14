package gocelery

import (
	"context"
	"encoding/hex"
	"errors"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

type test struct {
	work   work
	result interface{}
	err    bool
}

func dispatchWork(runs int,
	w chan work,
	runnerFunc RunnerFunc,
	runner Runner,
	runnerName string,
	args []interface{},
	or map[string]interface{},
	result interface{}, errored bool) map[string]test {
	tests := make(map[string]test)
	for i := 0; i < runs; i++ {
		t := test{
			work: work{
				job: &Job{
					ID:        randomBytes(32),
					Desc:      runnerName,
					Runner:    runnerName,
					Overrides: or,
					Tasks: []*Task{{
						RunnerFunc: runnerName,
						Args:       args,
					}},
				},
				runnerFunc: runnerFunc,
				runner:     runner,
			},
			result: result,
			err:    errored,
		}
		tests[hex.EncodeToString(t.work.job.ID)] = t
		go func(t test) {
			w <- t.work
		}(t)
	}

	return tests
}

func add(args []interface{}, overrides map[string]interface{}) (result interface{}, err error) {
	a := args[0].(int)
	b := overrides["b"].(int)
	return a + b, nil
}

func err(args []interface{}, overrides map[string]interface{}) (result interface{}, err error) {
	return nil, errors.New("this will always fail")
}

func TestWorker_RunnerFunc(t *testing.T) {
	w := make(chan work)
	result := make(chan *Job)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	workers := []*worker{newWorker(w, result), newWorker(w, result)}
	for _, worker := range workers {
		go worker.start(ctx)
	}

	runs := 100
	a, b := rand.Intn(1000), rand.Intn(1000)
	or := map[string]interface{}{"b": b}
	r := a + b
	tests := dispatchWork(runs/2, w, add, nil, "test", []interface{}{a}, or, r, false)
	testse := dispatchWork(runs/2, w, err, nil, "test", nil, nil, nil, true)
	for i := 0; i < runs; i++ {
		job := <-result
		test, ok := tests[hex.EncodeToString(job.ID)]
		if !ok {
			test = testse[hex.EncodeToString(job.ID)]
		}
		assert.Equal(t, test.result, job.Tasks[0].Result)
		assert.True(t, job.Tasks[0].DidRan())
		if test.err {
			assert.NotEmpty(t, job.Tasks[0].Error)
			continue
		}
		assert.Empty(t, job.Tasks[0].Error)
	}
}

type singleRunner struct{}

func (r singleRunner) New() Runner {
	return singleRunner{}
}

func (r singleRunner) RunnerFunc(task string) RunnerFunc {
	return add
}

func (r singleRunner) Next(task string) (next string, ok bool) {
	return "", false
}

type multiRunner struct{}

func (r multiRunner) New() Runner {
	return multiRunner{}
}

func (r multiRunner) RunnerFunc(task string) RunnerFunc {
	switch task {
	case "add":
		return add
	default:
		return err
	}
}

func (r multiRunner) Next(task string) (next string, ok bool) {
	switch task {
	case "add":
		return "error", true
	default:
		return "", false
	}
}

func TestWorker_Runner(t *testing.T) {
	w := make(chan work)
	result := make(chan *Job)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	workers := []*worker{newWorker(w, result), newWorker(w, result), newWorker(w, result)}
	for _, worker := range workers {
		go worker.start(ctx)
	}

	runs := 1000
	a, b := rand.Intn(1000), rand.Intn(1000)
	or := map[string]interface{}{"b": b}
	r := a + b
	tests := dispatchWork(runs, w, nil, singleRunner{}, "test", []interface{}{a}, or, r, false)
	testse := dispatchWork(runs, w, nil, multiRunner{}, "add", []interface{}{a}, or, r, false)
	testse1 := dispatchWork(runs, w, nil, multiRunner{}, "error", []interface{}{a}, or, nil, true)
	for i := 0; i < runs*3; i++ {
		job := <-result
		var tt test
		var ok bool
		for _, ts := range []map[string]test{tests, testse, testse1} {
			tt, ok = ts[hex.EncodeToString(job.ID)]
			if ok {
				break
			}
		}
		task := job.Tasks[0]
		assert.True(t, task.DidRan())
		if tt.err {
			assert.NotEmpty(t, task.Error)
			continue
		}
		assert.Equal(t, tt.result, task.Result)
		assert.Empty(t, task.Error)
	}
	assert.Len(t, result, 0)
}
