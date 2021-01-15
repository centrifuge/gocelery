package gocelery

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"
)

// Storage for storing any kind of data
type Storage interface {
	Get(key []byte) ([]byte, error)
	Set(key, value []byte) error
	Delete(key []byte) error
	Iterate(prefix []byte, f func(key, value []byte))
}

// Broker for queueing messages
type Broker interface {
	Start(ctx context.Context)
	Enqueue(id []byte) error
	Dequeue() ([]byte, error)
	Finished(id []byte)
	Len() int
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

func (t Task) IsSuccessful() bool {
	return t.DidRan() && t.Error == ""
}

func (t Task) IsRunnable() bool {
	return time.Now().UTC().After(t.Delay.UTC())
}

// Job represents a single prefix job
// a job can contain multiple sub-tasks
type Job struct {
	ID         []byte                 `json:"id"`
	Desc       string                 `json:"desc"`
	Runner     string                 `json:"runner"`    // name of the Runner
	Overrides  map[string]interface{} `json:"overrides"` // a prefix of overrides
	Tasks      []*Task                `json:"tasks"`     // if this is a TaskChain then sub tasks and their results are here
	ValidUntil time.Time              `json:"valid_until"`
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

func (j Job) IsValid() bool {
	return time.Now().UTC().Before(j.ValidUntil.UTC())
}

func (j Job) IsCompleted() bool {
	res := true
	for _, t := range j.Tasks {
		res = res && t.IsSuccessful()
	}

	return res
}

// LastTask returns the last task of the Job
func (j Job) LastTask() *Task {
	if len(j.Tasks) == 0 {
		return nil
	}

	return j.Tasks[len(j.Tasks)-1]
}

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
