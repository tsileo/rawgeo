package index

import (
	"bytes"
	"errors"
	"io"
	"os"
	"sync"

	"github.com/cznic/kv"
)

var ErrNotFound = errors.New("key does not exist")

type KeyValue struct {
	Key   string
	Value string
}

type DB struct {
	db   *kv.DB
	path string
	mu   *sync.Mutex
}

// New creates a new database.
func New(path string) (*DB, error) {
	createOpen := kv.Open
	if _, err := os.Stat(path); os.IsNotExist(err) {
		createOpen = kv.Create
	}
	kvdb, err := createOpen(path, &kv.Options{})
	if err != nil {
		return nil, err
	}
	return &DB{
		db:   kvdb,
		path: path,
		mu:   new(sync.Mutex),
	}, nil
}

func (db *DB) Close() error {
	return db.db.Close()
}

func (db *DB) Destroy() error {
	if db.path != "" {
		db.Close()
		return os.RemoveAll(db.path)
	}
	return nil
}

func (db *DB) Put(key, value string) (*KeyValue, error) {
	if err := db.db.Set([]byte(key), []byte(value)); err != nil {
		return nil, err
	}
	return &KeyValue{
		Key:   key,
		Value: value,
	}, nil
}

func (db *DB) Get(key string) (*KeyValue, error) {
	bkey := []byte(key)
	val, err := db.db.Get(nil, bkey)
	if err != nil {
		return nil, err
	}
	return &KeyValue{
		Key:   key,
		Value: string(val),
	}, nil
}

// Return a lexicographical range
func (db *DB) Keys(start, end string, limit int) ([]*KeyValue, error) {
	res := []*KeyValue{}
	enum, _, err := db.db.Seek([]byte(start))
	if err != nil {
		return nil, err
	}
	endBytes := []byte(end)
	i := 0
	for {
		k, _, err := enum.Next()
		if err == io.EOF {
			break
		}
		if bytes.Compare(k, endBytes) > 0 || (limit != 0 && i > limit) {
			return res, nil
		}
		kv, err := db.Get(string(k))
		if err != nil {
			return nil, err
		}
		res = append(res, kv)
		i++
	}
	return res, nil
}
