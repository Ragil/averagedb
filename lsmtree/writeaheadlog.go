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
	Compression string
	Key         string
	Value       string
	Deleted     bool
}

func newWriteAheadLog(stream io.ReadWriter) *writeAheadLog {
	return &writeAheadLog{
		stream:  stream,
		encoder: *gob.NewEncoder(stream),
	}
}

// write a single entry atomically to the log
func (log *writeAheadLog) write(key string, value string) error {
	return log.encoder.Encode(logEntry{"", key, value, false})
}

func (log *writeAheadLog) delete(key string) error {
	return log.encoder.Encode(logEntry{"", key, "", true})
}

func (log *writeAheadLog) decoder() *writeAheadLogDecoder {
	return &writeAheadLogDecoder{
		Decoder: *gob.NewDecoder(log.stream),
	}
}

// read one log entry from the log
func (decoder *writeAheadLogDecoder) read() (string, string, error) {
	var entry logEntry
	err := decoder.Decode(&entry)
	return entry.Key, entry.Value, err
}
