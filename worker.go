package gocelery

import (
	"context"
	"time"
)

type work struct {
	job        *Job
	runnerFunc RunnerFunc
	runner     Runner
}

type worker struct {
	work   <-chan work // channel to receive work
	result chan<- *Job // channel to send job result
}

func newWorker(work <-chan work, result chan<- *Job) *worker {
	return &worker{
		work:   work,
		result: result,
	}
}

func processWork(work work) *Job {
	job := work.job
	task := job.Tasks[len(job.Tasks)-1] // fetch the last task

	runnerFunc := work.runnerFunc
	var runner Runner
	if runnerFunc == nil {
		runner = work.runner.New()
		runnerFunc = runner.RunnerFunc(task.RunnerFunc)
	}

	res, err := runnerFunc(task.Args, job.Overrides)
	task.Result = res
	task.Tries++
	if err != nil {
		task.Error = err.Error()
		return job
	}

	if runner == nil {
		return job
	}

	// task completed, fetch the next task
	next, ok := runner.Next(task.RunnerFunc)
	if !ok {
		// no new task, return
		return job
	}

	job.Tasks = append(job.Tasks, newTask(next, task.Args, time.Now()))
	return job
}

func (w *worker) start(ctx context.Context) {
	for {
		select {
		case work := <-w.work:
			job := processWork(work)
			w.result <- job
		case <-ctx.Done():
			return
		}
	}
}
