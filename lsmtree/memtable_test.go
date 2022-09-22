package averagedb

import (
	"testing"
)

func TestPutUpdatesSize(t *testing.T) {
	mem := newMemtable()
	mem.put("hi", "value")

	if mem.size != 7 {
		t.Fatalf("expected size {7} but got {%v}", mem.size)
	}
}

func TestPutExistingEntryUpdatesSize(t *testing.T) {
	mem := newMemtable()
	mem.put("hi", "value")
	mem.put("hi", "value2")

	if mem.size != 8 {
		t.Fatalf("expected size {8} but got {%v}", mem.size)
	}
}

func TestDeleteMissingKey(t *testing.T) {
	mem := newMemtable()
	mem.delete("hi")
}

func TestDeleteUpdatesSize(t *testing.T) {
	mem := newMemtable()
	mem.put("hi", "value")
	mem.delete("hi")

	if mem.size != 0 {
		t.Fatalf("expected size {0} but got {%v}", mem.size)
	}
}

func TestGetSingleKey(t *testing.T) {
	mem := newMemtable()

	mem.put("hi", "value")
	value, _ := mem.get("hi")

	if value != "value" {
		t.Fatalf(`expected {value} but got {%v}`, value)
	}
}

func TestGetKeyThatDoesNotExist(t *testing.T) {
	mem := newMemtable()
	_, ok := mem.get("hi")

	if ok {
		t.Fatalf(`expected not ok, but ok`)
	}
}
