package averagedb

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

type IOProvider interface {
	Stream(namespace string, id string) (io.ReadWriter, error)
	ListStreamIDs(namespace string) ([]string, error)
}

type flushQueueEntry struct {
	memtable *memtable
	wal      *writeAheadLog
}

type lsmDB struct {
	memtable *memtable      // current memtable
	wal      *writeAheadLog // current wal
	walID    int            // current wal ID

	memtableMaxBytes int               // max bytes before flushing memtable
	flushQueue       []flushQueueEntry // ordered queue of memtables to flush (oldest first)
	flushLock        sync.RWMutex      // flush queue lock
	writeLock        sync.Mutex        // write lock

	ioProvider IOProvider // configurable io provider
}

const DefaultMemtableMaxBytes = 1 << 20 // 1 MiB

func LSMDB(ioProvider IOProvider, memtableMaxBytes int) (*lsmDB, error) {
	db := lsmDB{
		walID:            0,
		flushQueue:       nil,
		writeLock:        sync.Mutex{},
		flushLock:        sync.RWMutex{},
		ioProvider:       ioProvider,
		memtableMaxBytes: memtableMaxBytes,
	}
	if memtableMaxBytes == 0 {
		db.memtableMaxBytes = DefaultMemtableMaxBytes
	}

	err := db.newMemtableAndWal()
	if err != nil {
		return nil, err
	}

	return &db, nil
}

// In place update new memtable and WAL.
// Nothing is updated if a new WAL cannot be created.
func (db *lsmDB) newMemtableAndWal() error {
	stream, err := db.ioProvider.Stream("wal", fmt.Sprint(db.walID))
	if err != nil {
		return err
	}

	db.memtable = newMemtable()
	db.wal = newWriteAheadLog(stream)
	return nil
}

// Put data to the wal then update the memtable.
// Throws error when data exceeds max memtable bytes.
// Throws error when write ahead log cannot be rotated.
func (db *lsmDB) Put(key string, value string) error {
	if key == "" {
		return errors.New("key required")
	}

	operation := &writeOperation{
		Key:       key,
		Value:     value,
		Delete:    false,
		Timestamp: time.Now().UnixNano(),
	}
	return db.put(operation)
}

// Return value associated to key.
// Return not ok if key not in db.
// Throws error on failure to read from storage.
func (db *lsmDB) Get(key string) (string, bool, error) {
	if key == "" {
		return "", false, errors.New("key required")
	}

	// acquire flush read lock before reading data
	db.flushLock.RLock()
	defer db.flushLock.RUnlock()

	// check active memtable
	if operation, ok := db.memtable.get(key); ok {
		if operation.Delete {
			return "", false, nil
		}
		return operation.Value, true, nil
	}

	// check flush queue from newest to oldest
	for i := len(db.flushQueue) - 1; i >= 0; i-- {
		if operation, ok := db.flushQueue[i].memtable.get(key); ok {
			if operation.Delete {
				return "", false, nil
			}
			return operation.Value, true, nil
		}
	}

	return "", false, nil
}

// Delete data from db.
// Data is only tombstoned and will be deleted on next compaction.
func (db *lsmDB) Delete(key string) error {
	if key == "" {
		return errors.New("key required")
	}

	operation := &writeOperation{
		Key:       key,
		Value:     "",
		Delete:    true,
		Timestamp: time.Now().UnixNano(),
	}
	return db.put(operation)
}

// common put for write operations
func (db *lsmDB) put(operation *writeOperation) error {
	if operation.size() > db.memtableMaxBytes {
		return errors.New(fmt.Sprintf(
			`data too large. data is {%v} bytes. Max size is {%v} bytes`,
			operation.size(),
			db.memtableMaxBytes,
		))
	}

	if db.memtable.computeNewSize(operation) > db.memtableMaxBytes {
		// lock all flush queue
		db.flushLock.Lock()

		// memtable is full, swap it out
		db.flushQueue = append(db.flushQueue, flushQueueEntry{
			memtable: db.memtable,
			wal:      db.wal,
		})
		db.walID = db.walID + 1
		err := db.newMemtableAndWal()

		// release flush queue lock
		db.flushLock.Unlock()

		if err != nil {
			return err
		}
	}

	// acquire write lock to serialize puts and deletes
	db.writeLock.Lock()
	defer db.writeLock.Unlock()

	db.wal.write(operation)
	db.memtable.put(operation)
	return nil
}
