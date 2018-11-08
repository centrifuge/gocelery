package gocelery

import (
	"bytes"
	"testing"
)

func TestLevelDB_getKey(t *testing.T) {
	key := "some key"
	expected := []byte("celery-leveldb-result-" + key)
	got := getKey(key)
	if !bytes.Equal(expected, got) {
		t.Fatalf("expected %s; got %s", string(expected), string(got))
	}
}
