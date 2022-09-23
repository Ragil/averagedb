package averagedb

// everything persisted should be here

// persisted to wal and stored in memtable
type writeOperation struct {
	Key       string
	Value     string
	Delete    bool  // delete operation
	Timestamp int64 // current millisecond
}

func (op *writeOperation) size() int {
	return len(op.Key) + len(op.Value)
}
