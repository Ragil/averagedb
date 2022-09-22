package averagedb

import (
	"bytes"
	"testing"
)

func TestWriteSingleLog(t *testing.T) {
	var stream bytes.Buffer
	wal := newWriteAheadLog(&stream)
	err := wal.write("hi", "value")

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
	_, _, err := decoder.read()

	if err == nil {
		t.Fatalf(`error expected`)
	}
}

func TestWriteReadSingleLog(t *testing.T) {
	var stream bytes.Buffer
	wal := newWriteAheadLog(&stream)
	decoder := wal.decoder()

	wal.write("hi", "value")
	key, value, err := decoder.read()

	if err != nil {
		t.Fatalf(`unexpected error {%v}`, err)
	}
	if key != "hi" && value != "value" {
		t.Fatalf(`expected key={hi},value={value} but got key={%v}, value={%v}`, key, value)
	}
}

func TestWriteReadMultipleLogs(t *testing.T) {
	var stream bytes.Buffer
	wal := newWriteAheadLog(&stream)
	decoder := wal.decoder()

	wal.write("hi", "value")
	decoder.read()
	wal.write("hi2", "value2")
	key, value, err := decoder.read()

	if err != nil {
		t.Fatalf(`unexpected error {%v}`, err)
	}
	if key != "hi2" && value != "value2" {
		t.Fatalf(`expected key={hi2},value={value2} but got key={%v}, value={%v}`, key, value)
	}
}
