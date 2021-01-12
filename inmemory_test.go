package gocelery

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInMemoryBackend(t *testing.T) {
	runs := 1000
	backend := NewInMemoryBackend()
	testBackend(t, runs, backend)
	assert.Len(t, backend.(*inMemoryBackend).store, 0)
}

func TestInMemoryBroker(t *testing.T) {
	runs := 1000
	broker := NewInMemoryBroker()
	testBroker(t, runs, broker)
}
