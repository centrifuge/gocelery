package gocelery

import (
	"encoding/json"
	"fmt"

	"github.com/syndtr/goleveldb/leveldb"
)

// levelDBBackend implements the CeleryBackend based on levelDB
type levelDBBackend struct {
	db *leveldb.DB
}

// NewLevelDBBackend returns an levelDB implementation of CeleryBackend
func NewLevelDBBackend(db *leveldb.DB) CeleryBackend {
	return levelDBBackend{db: db}
}

// getKey appends "celery-leveldb-result-{key}"
// so that it doesn't clash with other keys
func getKey(key string) []byte {
	key = "celery-leveldb-result-" + key
	return []byte(key)
}

// GetResult returns the result stored at key
func (ldb levelDBBackend) GetResult(key string) (*ResultMessage, error) {
	data, err := ldb.db.Get(getKey(key), nil)
	if err != nil {
		return nil, err
	}

	res := new(ResultMessage)
	err = json.Unmarshal(data, res)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal result: %v", err)
	}

	return res, nil
}

// SetResult stores the result at key taskID
func (ldb levelDBBackend) SetResult(taskID string, result *ResultMessage) error {
	if result == nil {
		return fmt.Errorf("empty result")
	}

	data, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("failed to marshall result: %v", err)
	}

	return ldb.db.Put(getKey(taskID), data, nil)
}
