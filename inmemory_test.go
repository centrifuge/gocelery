package gocelery

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInMemoryStaorage(t *testing.T) {
	runs := 1000
	storage := NewInMemoryStorage()
	testStorage(t, runs, storage)
	assert.Len(t, storage.(*inMemoryStorage).store, 0)
}

func TestInMemoryBroker(t *testing.T) {
	runs := 1000
	broker := NewInMemoryBroker()
	testBroker(t, runs, broker)
}
