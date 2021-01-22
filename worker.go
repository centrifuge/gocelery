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
	task := job.LastTask() // fetch the last task

	runnerFunc := work.runnerFunc
	var runner Runner
	if runnerFunc == nil {
		runner = work.runner.New()
		runnerFunc = runner.RunnerFunc(task.RunnerFunc)
	}

	res, err := runnerFunc(task.Args, job.Overrides)
	task.Result = res
	task.Error = ""
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
		case work, ok := <-w.work:
			if !ok {
				log.Debugf("stopping worker: %v", "work chan closed")
				return
			}
			job := processWork(work)
			w.result <- job
		case <-ctx.Done():
			log.Debugf("stopping worker: %v", ctx.Err())
			return
		}
	}
}
