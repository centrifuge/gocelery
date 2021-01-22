package examples

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/centrifuge/gocelery/v2"
)

func add(args []interface{}, or map[string]interface{}) (interface{}, error) {
	return args[0].(int) + args[1].(int), nil
}

func err(args []interface{}, overrides map[string]interface{}) (result interface{}, err error) {
	return nil, errors.New("this will always fail")
}

type runner int

func (r runner) New() gocelery.Runner {
	return r
}

func (r runner) RunnerFunc(task string) gocelery.RunnerFunc {
	return func(args []interface{}, overrides map[string]interface{}) (result interface{}, err error) {
		args[1] = int(r)
		return add(args, overrides)
	}
}

func (r runner) Next(task string) (next string, ok bool) {
	return "", false
}

// ExampleDispatcher_Dispatch_runnerFunc runs a single `add` task with default validity.
func ExampleDispatcher_Dispatch_runnerFunc() {
	storage := gocelery.NewInMemoryStorage()
	queue := gocelery.NewQueue(storage, 10*time.Minute)
	dispatcher := gocelery.NewDispatcher(10, storage, queue)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go dispatcher.Start(ctx)
	dispatcher.RegisterRunnerFunc("add", add)

	job := gocelery.NewRunnerFuncJob("Add job", "add", []interface{}{1, 2}, nil, time.Now())
	res, err := dispatcher.Dispatch(job)
	if err != nil {
		panic(err)
	}

	result, err := res.Await(ctx)
	fmt.Println(result, err)
	// Output: 3 <nil>
}

// ExampleDispatcher_Dispatch_runnerFunc_fail runs a single `error` task with a 10 second validity.
func ExampleDispatcher_Dispatch_runnerFunc_fail() {
	storage := gocelery.NewInMemoryStorage()
	queue := gocelery.NewQueue(storage, 10*time.Minute)
	dispatcher := gocelery.NewDispatcher(10, storage, queue)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go dispatcher.Start(ctx)
	dispatcher.RegisterRunnerFunc("error", err)

	job := gocelery.NewRunnerFuncJob("Fail job", "error", []interface{}{1, 2}, nil, time.Now().Add(10*time.Second))
	res, err := dispatcher.Dispatch(job)
	if err != nil {
		panic(err)
	}

	result, err := res.Await(ctx)
	fmt.Println(result, err)
	// Output: <nil> this will always fail
}

// ExampleDispatcher_Dispatch_runner runs a single runner `add` task with default validity.
func ExampleDispatcher_Dispatch_runner() {
	storage := gocelery.NewInMemoryStorage()
	queue := gocelery.NewQueue(storage, 10*time.Minute)
	dispatcher := gocelery.NewDispatcher(10, storage, queue)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go dispatcher.Start(ctx)
	dispatcher.RegisterRunner("runner", runner(4))

	job := gocelery.NewRunnerJob("Add job", "runner", "add", []interface{}{1, 2}, nil, time.Now())
	res, err := dispatcher.Dispatch(job)
	if err != nil {
		panic(err)
	}

	result, err := res.Await(ctx)
	fmt.Println(result, err)

	res = gocelery.Result{
		JobID:      job.ID,
		Dispatcher: dispatcher,
	}
	result, err = res.Await(ctx)
	fmt.Println(result, err)
	// Output:
	// 5 <nil>
	// 5 <nil>
}
