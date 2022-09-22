package averagedb

type memtable struct {
	data map[string]string
	size int // size in bytes
}

func newMemtable() *memtable {
	return &memtable{
		data: map[string]string{},
		size: 0,
	}
}

// Compute expected size of memtable when new key,value are inserted or updated.
func (mem *memtable) computeNewSize(key string, value string) int {
	currentSize := mem.size
	if existingValue, ok := mem.data[key]; ok {
		currentSize -= len(key) + len(existingValue)
	}
	return currentSize + len(key) + len(value)
}

// Insert data into memtable
// Not safe to use across go routines
func (mem *memtable) put(key string, value string) {
	mem.size = mem.computeNewSize(key, value)
	mem.data[key] = value
}

// Retrieve data from memtable.
// If the data doesn't exist, return nil
// Key is required
func (mem *memtable) get(key string) (string, bool) {
	value, ok := mem.data[key]
	return value, ok
}

// delete data from memtable.
// If key doesn't exist, no-op.
func (mem *memtable) delete(key string) {
	if value, ok := mem.data[key]; ok {
		delete(mem.data, key)
		mem.size -= len(key) + len(value)
	}
}
