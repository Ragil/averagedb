package averagedb

import (
	"bytes"
	"io"
	"testing"

	"golang.org/x/exp/maps"
)

type memoryIOProvider struct {
	data map[string]map[string]*bytes.Buffer
}

func (ioProvider *memoryIOProvider) ListStreamIDs(namespace string) ([]string, error) {
	return maps.Keys(ioProvider.data[namespace]), nil
}
func (ioProvider *memoryIOProvider) Stream(namespace string, id string) (io.ReadWriter, error) {
	if _, ok := ioProvider.data[namespace]; !ok {
		ioProvider.data[namespace] = map[string]*bytes.Buffer{}
	}
	ioProvider.data[namespace][id] = &bytes.Buffer{}
	return ioProvider.data[namespace][id], nil
}

func TestLSMDBReadValuesInFlushQueue(t *testing.T) {
	ioProvider := &memoryIOProvider{
		data: map[string]map[string]*bytes.Buffer{},
	}
	db, _ := LSMDB(ioProvider, 10)

	db.Put("yo", "oldest")
	db.Put("hi", "flush")
	db.Put("yo", "newest")

	value, _, _ := db.Get("hi")
	if value != "flush" {
		t.Fatalf(`expected value {value} but found {%v}`, value)
	}

	value, _, _ = db.Get("yo")
	if value != "newest" {
		t.Fatalf(`expected value {newest} but found {%v}`, value)
	}
}

func TestLSMDBReadDeletedValuesInFlushQueue(t *testing.T) {
	ioProvider := &memoryIOProvider{
		data: map[string]map[string]*bytes.Buffer{},
	}
	db, _ := LSMDB(ioProvider, 10)

	db.Put("yo", "oldest")
	db.Put("hi", "flush")
	db.Delete("yo")

	value, ok, _ := db.Get("yo")
	if ok {
		t.Fatalf(`expected item deleted but found {%v}`, value)
	}
}

func TestLSMDBReadValuesThatDoNotExist(t *testing.T) {
	ioProvider := &memoryIOProvider{
		data: map[string]map[string]*bytes.Buffer{},
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
		data: map[string]map[string]*bytes.Buffer{},
	}
	db, _ := LSMDB(ioProvider, 10)

	db.Put("hi", "value")

	keys := maps.Keys(ioProvider.data["wal"])
	if len(keys) != 1 {
		t.Fatalf(`expected 1 stream created but found {%v}`, len(keys))
	}

	bytesBuffer := ioProvider.data["wal"][keys[0]]
	if bytesBuffer.Len() == 0 {
		t.Fatalf(`buffer expected data but found 0 bytes`)
	}
}

func TestLSMDBSendToFlushQueueWhenMemtableFull(t *testing.T) {
	ioProvider := &memoryIOProvider{
		data: map[string]map[string]*bytes.Buffer{},
	}
	db, _ := LSMDB(ioProvider, 10)

	db.Put("hi", "value")
	db.Put("yo", "value")
	db.Put("ye", "value")
	db.Put("zz", "value")

	keys := maps.Keys(ioProvider.data["wal"])
	if len(keys) != 4 {
		t.Fatalf(`expected 4 streams created but found {%v} for maxMemtableBytes {%v}`, len(keys), db.memtableMaxBytes)
	}
}
