package gocelery

import (
	"context"
	"crypto/rand"
	"errors"
)

var (
	// ErrNotFound when the a requested resource is not found
	ErrNotFound = errors.New("not found")
)

// Backend for storing any kind of data
type Backend interface {
	Get(key []byte) ([]byte, error)
	Set(key, value []byte) error
	Delete(key []byte) error
}

// Broker for queueing messages
type Broker interface {
	Start(ctx context.Context) error
	Enqueue(id []byte) error
	Dequeue() ([]byte, error)
	Finished(id []byte)
	Len() int
}

// Task represents a single task in a Job
type Task struct {
	Runner string        `json:"runner"`
	Args   []interface{} `json:"args"`
	Result interface{}   `json:"result"`
	Error  error         `json:"error"`
	Tries  int           `json:"tries"`
}

// DidRan returns if the task was run at least once.
func (t Task) DidRan() bool {
	return t.Result != nil || t.Error != nil
}

// Job represents a single prefix job
// a job can contain multiple sub-tasks
type Job struct {
	ID          []byte                 `json:"id"`
	Desc        string                 `json:"desc"`
	Runner      string                 `json:"runner"` // name of the singleRunner
	Preferences Preferences            `json:"preferences"`
	Overrides   map[string]interface{} `json:"overrides"` // a prefix of overrides
	Tasks       []*Task                `json:"tasks"`     // if this is a TaskChain then sub tasks and their results are here
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
