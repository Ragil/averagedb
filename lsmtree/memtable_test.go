package averagedb

import (
	"testing"
)

func TestPutUpdatesSize(t *testing.T) {
	mem := newMemtable()
	mem.put(&writeOperation{Key: "hi", Value: "value"})

	if mem.size != 7 {
		t.Fatalf("expected size {7} but got {%v}", mem.size)
	}
}

func TestPutExistingEntryUpdatesSize(t *testing.T) {
	mem := newMemtable()
	mem.put(&writeOperation{Key: "hi", Value: "value"})
	mem.put(&writeOperation{Key: "hi", Value: "value2"})

	if mem.size != 8 {
		t.Fatalf("expected size {8} but got {%v}", mem.size)
	}
}

func TestDeleteMissingKey(t *testing.T) {
	mem := newMemtable()
	mem.put(&writeOperation{Key: "hi", Delete: true})

	operation, _ := mem.get("hi")
	if operation == nil || !operation.Delete {
		t.Fatalf("delete operation should be remembered")
	}
}

func TestDeleteUpdatesSize(t *testing.T) {
	mem := newMemtable()
	mem.put(&writeOperation{Key: "hi", Value: "value"})
	mem.put(&writeOperation{Key: "hi", Delete: true})

	if mem.size != 2 {
		t.Fatalf("expected size {2} but got {%v}", mem.size)
	}
}

func TestGetSingleKey(t *testing.T) {
	mem := newMemtable()

	mem.put(&writeOperation{Key: "hi", Value: "value"})
	operation, _ := mem.get("hi")

	if operation == nil || operation.Value != "value" {
		t.Fatalf(`expected operation for {hi=value} but got {%v}`, operation)
	}
}

func TestGetKeyThatDoesNotExist(t *testing.T) {
	mem := newMemtable()
	_, ok := mem.get("hi")

	if ok {
		t.Fatalf(`expected not ok, but ok`)
	}
}
