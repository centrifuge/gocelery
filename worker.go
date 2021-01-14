package gocelery

import (
	"context"
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

	if work.runnerFunc != nil {
		task.Result, task.Error = work.runnerFunc(task.Args, job.Overrides)
		return job
	}

	runner := work.runner.New()
	runnerFunc := runner.RunnerFunc(task.Runner)
	task.Result, task.Error = runnerFunc(task.Args, job.Overrides)
	if task.Error != nil {
		return job
	}

	// task completed, fetch the next task
	next, ok := runner.Next(task.Runner)
	if !ok {
		// no new task, return
		return job
	}

	job.Tasks = append(job.Tasks, &Task{Runner: next, Args: task.Args})
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
