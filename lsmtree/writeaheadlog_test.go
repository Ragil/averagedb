package averagedb

import (
	"bytes"
	"testing"
)

func TestWriteSingleLog(t *testing.T) {
	var stream bytes.Buffer
	wal := newWriteAheadLog(&stream)
	err := wal.write(&writeOperation{Key: "hi", Value: "value"})

	if err != nil {
		t.Fatalf(`unexpected error {%v}`, err)
	}
	if stream.Len() == 0 {
		t.Fatalf(`should have written something to buffer`)
	}
}

func TestReadEndOfStream(t *testing.T) {
	var stream bytes.Buffer
	wal := newWriteAheadLog(&stream)
	decoder := wal.decoder()
	_, err := decoder.read()

	if err == nil {
		t.Fatalf(`error expected`)
	}
}

func TestWriteReadSingleLog(t *testing.T) {
	var stream bytes.Buffer
	wal := newWriteAheadLog(&stream)
	decoder := wal.decoder()

	wal.write(&writeOperation{Key: "hi", Value: "value"})
	entry, err := decoder.read()

	if err != nil {
		t.Fatalf(`unexpected error {%v}`, err)
	}
	if entry.Key != "hi" && entry.Value != "value" {
		t.Fatalf(`expected key={hi},value={value} but got key={%v}, value={%v}`, entry.Key, entry.Value)
	}
}

func TestWriteReadMultipleLogs(t *testing.T) {
	var stream bytes.Buffer
	wal := newWriteAheadLog(&stream)
	decoder := wal.decoder()

	wal.write(&writeOperation{Key: "hi", Value: "value"})
	decoder.read()
	wal.write(&writeOperation{Key: "hi2", Value: "value2"})
	entry, err := decoder.read()

	if err != nil {
		t.Fatalf(`unexpected error {%v}`, err)
	}
	if entry.Key != "hi2" && entry.Value != "value2" {
		t.Fatalf(`expected key={hi2},value={value2} but got key={%v}, value={%v}`, entry.Key, entry.Value)
	}
}
