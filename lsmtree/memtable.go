package averagedb

type memtable struct {
	data map[string]*writeOperation
	size int // size in bytes
}

func newMemtable() *memtable {
	return &memtable{
		data: map[string]*writeOperation{},
		size: 0,
	}
}

// Compute expected size of memtable when new key,value are inserted or updated.
func (mem *memtable) computeNewSize(op *writeOperation) int {
	currentSize := mem.size
	if existingValue, ok := mem.data[op.Key]; ok {
		currentSize -= existingValue.size()
	}
	return currentSize + op.size()
}

// Insert data into memtable
// Not safe to use across go routines
func (mem *memtable) put(op *writeOperation) {
	mem.size = mem.computeNewSize(op)
	mem.data[op.Key] = op
}

// Retrieve data from memtable.
// If the data doesn't exist, return nil
// Key is required
func (mem *memtable) get(key string) (*writeOperation, bool) {
	value, ok := mem.data[key]
	return value, ok
}
