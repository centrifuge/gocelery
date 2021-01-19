package gocelery

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"
)

// Result is the result of a Job
type Result struct {
	JobID      []byte
	Dispatcher *Dispatcher
}

// Await waits for the job to be finished and return the result.
func (r Result) Await(ctx context.Context) (res interface{}, err error) {
	// subscribe for finish
	sub := make(chan *Job)
	r.Dispatcher.Subscribe(r.JobID, sub)
	defer r.Dispatcher.UnSubscribe(r.JobID, sub)

	// fetch job
	job, err := r.Dispatcher.Job(r.JobID)
	if err != nil {
		return nil, err
	}

	if !job.HasCompleted() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case j := <-sub:
			job = j
		}
	}

	task := job.LastTask()
	if task.Error != "" {
		return task.Result, errors.New(task.Error)
	}

	return task.Result, nil
}

// Task represents a single task in a Job
type Task struct {
	RunnerFunc string        `json:"runnerFuncs"` // name of the runnerFuncs
	Args       []interface{} `json:"args"`
	Result     interface{}   `json:"result"`
	Error      string        `json:"error"`
	Tries      uint          `json:"tries"`
	Delay      time.Time     `json:"delay"`
}

func newTask(runnerFunc string, args []interface{}, delay time.Time) *Task {
	return &Task{
		RunnerFunc: runnerFunc,
		Args:       args,
		Delay:      delay.UTC(),
	}
}

// DidRan returns if the task was run at least once.
func (t Task) DidRan() bool {
	return t.Tries > 0
}

// IsSuccessful if a task is successful.
func (t Task) IsSuccessful() bool {
	return t.DidRan() && t.Error == ""
}

// Job represents a single prefix job
// a job can contain multiple sub-tasks
type Job struct {
	ID         []byte                 `json:"JobID"`
	Desc       string                 `json:"desc"`
	Runner     string                 `json:"runner"`    // name of the Runner
	Overrides  map[string]interface{} `json:"overrides"` // a prefix of overrides
	Tasks      []*Task                `json:"tasks"`     // if this is a TaskChain then sub tasks and their results are here
	ValidUntil time.Time              `json:"valid_until"`
	FinishedAt time.Time              `json:"finished_at"`
}

// NewRunnerFuncJob creates a new job with task as the runnerFunc
func NewRunnerFuncJob(description, task string,
	args []interface{}, overrides map[string]interface{},
	validUntil time.Time) *Job {
	return newJob(description, "", task, args, overrides, validUntil)
}

// NewRunnerJob creates a new job with runner and its first task
func NewRunnerJob(description, runner, task string,
	args []interface{}, overrides map[string]interface{},
	validUntil time.Time) *Job {
	return newJob(description, runner, task, args, overrides, validUntil)
}

func newJob(
	description, runner, runnerFunc string,
	args []interface{}, overrides map[string]interface{},
	validUntil time.Time) *Job {
	if validUntil.IsZero() || time.Now().UTC().After(validUntil) {
		validUntil = time.Now().UTC().Add(MaxValidTime)
	}

	return &Job{
		ID:        randomBytes(32),
		Desc:      description,
		Runner:    runner,
		Overrides: overrides,
		Tasks: []*Task{{
			RunnerFunc: runnerFunc,
			Args:       args,
			Delay:      time.Now().UTC(),
		}},
		ValidUntil: validUntil.UTC(),
	}
}

// IsValid returns of the job is still valid.
func (j Job) IsValid() bool {
	return time.Now().UTC().Before(j.ValidUntil.UTC())
}

// IsSuccessful returns true if the job completed successfully
func (j Job) IsSuccessful() bool {
	if !j.HasCompleted() {
		return false
	}

	return j.isSuccessful()
}

func (j Job) isSuccessful() bool {
	res := true
	for _, t := range j.Tasks {
		res = res && t.IsSuccessful()
	}

	return res
}

// HasCompleted checks if the job has finished running.
func (j Job) HasCompleted() bool {
	// has expired already
	if time.Now().UTC().After(j.ValidUntil) {
		return true
	}

	return !j.FinishedAt.IsZero()
}

// LastTask returns the last task of the Job
func (j Job) LastTask() *Task {
	if len(j.Tasks) == 0 {
		return nil
	}

	return j.Tasks[len(j.Tasks)-1]
}

// HexID returns a hex encoded string of 32 byte jobID
func (j Job) HexID() string {
	return hex.EncodeToString(j.ID)
}

// RunnerFunc is the func that is called to execute the Job
type RunnerFunc func(args []interface{}, overrides map[string]interface{}) (result interface{}, err error)

// Runner instance to run a stateful job
type Runner interface {
	New() Runner
	RunnerFunc(task string) RunnerFunc
	Next(task string) (next string, ok bool) // next task after the task
}

func randomBytes(len int) []byte {
	r := make([]byte, len)
	_, err := rand.Read(r)
	if err != nil {
		panic(err)
	}

	return r
}
