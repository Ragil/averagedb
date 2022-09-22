package averagedb

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync"
)

type IOProvider interface {
	Stream(id string) (io.ReadWriter, error)
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
	flushQueue       []flushQueueEntry // queue of memtables to flush
	memLock          sync.RWMutex      // memtable lock

	ioProvider IOProvider // configurable io provider
}

const defaultMemtableMaxBytes = 4096

func LSMDB(ioProvider IOProvider, memtableMaxBytes int) (*lsmDB, error) {
	db := lsmDB{
		walID:            0,
		flushQueue:       nil,
		memLock:          sync.RWMutex{},
		ioProvider:       ioProvider,
		memtableMaxBytes: memtableMaxBytes,
	}
	if memtableMaxBytes == 0 {
		db.memtableMaxBytes = defaultMemtableMaxBytes
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
	stream, err := db.ioProvider.Stream(filepath.Join("wal", fmt.Sprint(db.walID)))
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
	if value == "" {
		return errors.New("value required")
	}

	if len(key)+len(value) > db.memtableMaxBytes {
		return errors.New(fmt.Sprintf(
			`data too large. data is {%v} bytes. Max size is {%v} bytes`,
			len(key)+len(value),
			db.memtableMaxBytes,
		))
	}

	if db.memtable.computeNewSize(key, value) > db.memtableMaxBytes {
		// lock all access before flushing
		db.memLock.Lock()

		// memtable is full, swap it out
		db.flushQueue = append(db.flushQueue, flushQueueEntry{
			memtable: db.memtable,
			wal:      db.wal,
		})
		db.walID = db.walID + 1
		err := db.newMemtableAndWal()

		// release write lock
		db.memLock.Unlock()

		if err != nil {
			return err
		}
	}

	// acquire memtable read lock before writing data
	db.memLock.RLock()
	defer db.memLock.RUnlock()

	db.wal.write(key, value)
	db.memtable.put(key, value)
	return nil
}

// Return value associated to key.
// Return not ok if key not in db.
// Throws error on failure to read from storage.
func (db *lsmDB) Get(key string) (string, bool, error) {
	if key == "" {
		return "", false, errors.New("key required")
	}

	// acquire memtable read lock before reading data
	db.memLock.RLock()
	defer db.memLock.RUnlock()

	// check active memtable
	if value, ok := db.memtable.get(key); ok {
		return value, true, nil
	}

	// check flush queue
	for _, flushQueueEntry := range db.flushQueue {
		if value, ok := flushQueueEntry.memtable.get(key); ok {
			return value, true, nil
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

	// acquire memtable read lock before deleting data
	db.memLock.RLock()
	defer db.memLock.RUnlock()

	db.wal.delete(key)
	db.memtable.delete(key)
	return nil
}
