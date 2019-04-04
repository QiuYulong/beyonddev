package skiplist

import (
	"testing"
)

func TestNodeLevel(t *testing.T) {
	n := node{}
	if n.level() != 0 {
		t.Errorf("NodeLevel got %v; want 0", n.level())
	}
	n.nexts = []*node{nil, nil, nil}
	if n.level() != 3 {
		t.Errorf("NodeLevel got %v; want 3", n.level())
	}
}

type value int

func (v value) CompareTo(other interface{}) int {
	return int(v) - int(other.(value))
}

func TestLen(t *testing.T) {
	sl := NewSkipList()
	// test empty
	length := sl.Len()
	if length != 0 {
		t.Errorf("Len got %v; want 0", length)
	}
	// test non-empty
	old := sl.Put(value(1), false)
	if old != nil {
		t.Errorf("Len got %v; want nil", old)
	}
	length = sl.Len()
	if length != 1 {
		t.Errorf("Len got %v; want 1", length)
	}
}

func TestPut(t *testing.T) {
	sl := NewSkipList()
	var values []value
	for i := 0; i < 100; i++ {
		values = append(values, value(i))
	}
	for _, v := range values {
		old := sl.Put(v, true)
		if old != nil {
			t.Errorf("Put got %v; want nil", old)
		}
	}
	for _, v := range values {
		old := sl.Put(v, true)
		if old == nil {
			t.Errorf("Put got nil; want %v", v)
		}
		if old.CompareTo(v) != 0 {
			t.Errorf("Put got %v; want %v", old, v)
		}
	}
}

func TestRemove(t *testing.T) {
	sl := NewSkipList()
	var values []value
	for i := 0; i < 100; i++ {
		values = append(values, value(i))
	}
	for _, v := range values {
		sl.Put(v, true)
	}
	for _, v := range values {
		ve := sl.Remove(v)
		if ve == nil || ve.CompareTo(v) != 0 {
			t.Errorf("Remove got %v; want %v", ve, v)
		}
	}
	ve := sl.Remove(value(99999)) // test non-exist.
	if ve != nil {
		t.Errorf("Remove got %v; want nil", ve)
	}
}

func TestIterator(t *testing.T) {
	sl := NewSkipList()
	var values []value
	for i := 0; i < 100; i++ {
		values = append(values, value(i))
	}
	for _, v := range values {
		sl.Put(v, true)
	}
	// test 0...100.
	it := sl.Iterator(nil, true, 0, ^uint64(0))
	count := 0
	for {
		ve := it()
		if ve == nil {
			break
		}
		count++
	}
	if count != len(values) {
		t.Errorf("Iterator got %v; want %v", count, len(values))
	}
	ve := it() // continue to call after done.
	if ve != nil {
		t.Errorf("Iterator got %v; want nil", ve)
	}
	// test offset & limit.
	ve = sl.Iterator(value(0), true, 50, 1)()
	if ve.CompareTo(value(50)) != 0 {
		t.Errorf("Iterator got %v; want %v", ve, 50)
	}
	// test smallest value.
	ve = sl.Iterator(value(-1), true, 0, 0)()
	if ve != nil {
		t.Errorf("Iterator got %v; want nil", ve)
	}
	// test backward traverse.
	ve = sl.Iterator(value(100), false, 50, 1)()
	if ve.CompareTo(value(49)) != 0 {
		t.Errorf("Iterator got %v; want 49", ve)
	}
}
