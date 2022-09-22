package averagedb

import (
	"bytes"
	"io"
	"testing"

	"golang.org/x/exp/maps"
)

type memoryIOProvider struct {
	data map[string]*bytes.Buffer
}

func (ioProvider *memoryIOProvider) Stream(id string) (io.ReadWriter, error) {
	ioProvider.data[id] = &bytes.Buffer{}
	return ioProvider.data[id], nil
}

func TestLSMDBReadValuesInFlushQueue(t *testing.T) {
	ioProvider := &memoryIOProvider{
		data: map[string]*bytes.Buffer{},
	}
	db, _ := LSMDB(ioProvider, 10)

	db.Put("hi", "value")
	db.Put("yo", "yoyo")

	value, _, _ := db.Get("hi")
	if value != "value" {
		t.Fatalf(`expected value {value} but found {%v}`, value)
	}

	value, _, _ = db.Get("yo")
	if value != "yoyo" {
		t.Fatalf(`expected value {yoyo} but found {%v}`, value)
	}
}

func TestLSMDBReadValuesThatDoNotExist(t *testing.T) {
	ioProvider := &memoryIOProvider{
		data: map[string]*bytes.Buffer{},
	}
	db, _ := LSMDB(ioProvider, 10)

	db.Put("hi", "value")
	value, ok, err := db.Get("not exist")

	if err != nil {
		t.Fatalf(`unexpected error {%v}`, err)
	}
	if ok {
		t.Fatalf(`expected not found but something found {%v}`, value)
	}
}

func TestLSMDBWriteValuesToWal(t *testing.T) {
	ioProvider := &memoryIOProvider{
		data: map[string]*bytes.Buffer{},
	}
	db, _ := LSMDB(ioProvider, 10)

	db.Put("hi", "value")

	keys := maps.Keys(ioProvider.data)
	if len(keys) != 1 {
		t.Fatalf(`expected 1 stream created but found {%v}`, len(keys))
	}

	bytesBuffer := ioProvider.data[keys[0]]
	if bytesBuffer.Len() == 0 {
		t.Fatalf(`buffer expected data but found 0 bytes`)
	}
}

func TestLSMDBSendToFlushQueueWhenMemtableFull(t *testing.T) {
	ioProvider := &memoryIOProvider{
		data: map[string]*bytes.Buffer{},
	}
	db, _ := LSMDB(ioProvider, 10)

	db.Put("hi", "value")
	db.Put("yo", "value")
	db.Put("ye", "value")
	db.Put("zz", "value")

	keys := maps.Keys(ioProvider.data)
	if len(keys) != 4 {
		t.Fatalf(`expected 4 streams created but found {%v} for maxMemtableBytes {%v}`, len(keys), db.memtableMaxBytes)
	}
}
