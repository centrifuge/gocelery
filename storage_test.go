package gocelery

import (
	cr "crypto/rand"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb"
)

func TestInMemoryStorage(t *testing.T) {
	runs := 1000
	storage := NewInMemoryStorage()
	testStorage(t, runs, storage)
	assert.Len(t, storage.(*inMemoryStorage).store, 0)
}

func TestNewLevelDBStorage(t *testing.T) {
	db, canc := getLevelDB(t, randomFilePath(t))
	defer canc()
	storage := NewLevelDBStorage(db)
	runs := 1000
	testStorage(t, runs, storage)
}

func randomFilePath(t *testing.T) string {
	r := make([]byte, 32)
	n, err := cr.Read(r)
	assert.Nil(t, err)
	assert.Equal(t, n, 32)
	return fmt.Sprintf("%s_%x", "/tmp/data.leveldb_TESTING", r)
}

func getLevelDB(t *testing.T, path string) (*leveldb.DB, func()) {
	if path == "" {
		path = randomFilePath(t)
	}
	levelDB, err := leveldb.OpenFile(path, nil)
	if err != nil {
		t.Fatalf("failed to open levelDb: %v", err)
	}

	return levelDB, func() {
		levelDB.Close()
		os.Remove(path)
	}
}

func testStorage(t *testing.T, runs int, backend Storage) {
	prefix := []byte("test-prefix")
	var wg sync.WaitGroup
	for i := 0; i < runs; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := randomBytes(32)
			value := randomBytes(32)
			assert.NoError(t, backend.Set(append(prefix, key...), value))
		}(i)
	}

	wg.Wait()
	keys, values := make([][]byte, 0, runs), make([][]byte, 0, runs)
	backend.Iterate(prefix, func(key, value []byte) {
		keys = append(keys, append([]byte{}, key...))
		values = append(values, append([]byte{}, value...))
	})

	assert.Len(t, keys, runs)
	assert.Len(t, values, runs)
	for i, k := range keys {
		k := k
		gv, err := backend.Get(k)
		assert.NoError(t, err)
		assert.Equal(t, values[i], gv)
		assert.NoError(t, backend.Delete(k))
	}
}
