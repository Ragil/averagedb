package averagedb

import (
	"encoding/gob"
	"io"
)

type writeAheadLog struct {
	stream  io.ReadWriter
	encoder gob.Encoder
}

type writeAheadLogDecoder struct {
	gob.Decoder
}

type logEntry struct {
	Key     string
	Value   string
	Deleted bool
}

func newWriteAheadLog(stream io.ReadWriter) *writeAheadLog {
	return &writeAheadLog{
		stream:  stream,
		encoder: *gob.NewEncoder(stream),
	}
}

// write a single entry atomically to the log
func (log *writeAheadLog) write(entry *logEntry) error {
	return log.encoder.Encode(entry)
}

func (log *writeAheadLog) decoder() *writeAheadLogDecoder {
	return &writeAheadLogDecoder{
		Decoder: *gob.NewDecoder(log.stream),
	}
}

// read one log entry from the log
func (decoder *writeAheadLogDecoder) read() (*logEntry, error) {
	var entry logEntry
	err := decoder.Decode(&entry)
	return &entry, err
}
