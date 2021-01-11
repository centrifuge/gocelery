package gocelery

import (
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
	Enqueue(id []byte)
	Dequeue() ([]byte, error)
	Finished(id []byte)
	Len() int
}

// Task represents a single task in a Job
type Task struct {
	Runner string        `json:"runner"` // name of the runner
	Args   []interface{} `json:"args"`
	Tries  int           `json:"tries"`
}

// Job represents a single queue job
// a job can contain multiple sub-tasks
type Job struct {
	ID          []byte                 `json:"id"`
	Name        string                 `json:"name"`
	Preferences Preferences            `json:"preferences"`
	Overrides   map[string]interface{} `json:"overrides"` // a queue of overrides
	Tasks       []Task                 `json:"tasks"`     // if this is a TaskChain then sub tasks and their results are here
	Finished    bool                   `json:"finished"`
}

func randomBytes(len int) []byte {
	r := make([]byte, len)
	_, err := rand.Read(r)
	if err != nil {
		panic(err)
	}

	return r
}
