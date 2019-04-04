package ds

import (
	"bytes"
	"strconv"
	"testing"
)

type keyvalue struct {
	key   []byte
	value []byte
}

var emptyData = []keyvalue{
	{nil, nil},
	{[]byte{}, nil},
}
var testData = []keyvalue{
	{[]byte{1}, nil},
	{[]byte{1, 2, 3, 4}, []byte{1, 2, 3, 4, 5, 6}},
	{[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, nil},
	{[]byte(strconv.Itoa(12345)), []byte(strconv.Itoa(67890))},
	{[]byte("abc"), []byte("xyz")},
}

func TestLen(t *testing.T) {
	sm := NewSortedMap()
	// test initial length 0.
	length, err := sm.Len()
	if err != nil {
		t.Errorf("Len got %v; want nil", err)
	}
	if length != 0 {
		t.Errorf("Len got %v; want %v", length, uint64(len(testData)))
	}
	// test positive cases.
	for _, kv := range testData {
		sm.Put(kv.key, kv.value)
	}
	length, err = sm.Len()
	if err != nil {
		t.Errorf("Len got %v; want nil", err)
	}
	if length != uint64(len(testData)) {
		t.Errorf("Len got %v; want %v", length, uint64(len(testData)))
	}
}

func TestPut(t *testing.T) {
	sm := NewSortedMap()
	// test negative cases: nil & empty.
	for _, kv := range emptyData {
		_, err := sm.Put(kv.key, kv.value)
		if err == nil {
			t.Errorf("Put got nil; want error")
		}
	}
	// test positive cases.
	for _, kv := range testData {
		v, err := sm.Put(kv.key, kv.value)
		if err != nil {
			t.Errorf("Put got error %v; want nil", err)
		}
		if v != nil {
			t.Errorf("Put got %v; want nil", v)
		}
	}
	v, err := sm.Put(testData[0].key, testData[0].value)
	if err != nil {
		t.Errorf("Put got %v; want nil", err)
	}
	if bytes.Compare(v, testData[0].value) != 0 {
		t.Errorf("Put got %v; want %v", v, testData[0].value)
	}
}

func TestRemove(t *testing.T) {
	sm := NewSortedMap()
	// test negative cases: nil & empty.
	for _, kv := range emptyData {
		_, err := sm.Remove(kv.key)
		if err == nil {
			t.Errorf("Put got nil; want error")
		}
	}
	// test positive cases.
	for _, kv := range testData {
		sm.Put(kv.key, kv.value)
		v, err := sm.Remove(kv.key)
		if err != nil {
			t.Errorf("Remove got %v; want nil", err)
		}
		if bytes.Compare(v, kv.value) != 0 {
			t.Errorf("Remove got %v; want %v", v, kv.value)
		}
	}
	v, err := sm.Remove([]byte{255})
	if err != nil {
		t.Errorf("Remove got %v; want nil", err)
	}
	if v != nil {
		t.Errorf("Remove got %v; want nil", v)
	}
}

func TestGet(t *testing.T) {
	sm := NewSortedMap()
	for _, kv := range testData {
		sm.Put(kv.key, kv.value)
	}
	// test negative cases: nil & empty
	for _, kv := range emptyData {
		_, err := sm.Get(kv.key)
		if err == nil {
			t.Errorf("Get got nil; want error")
		}
	}
	// test positive cases.
	for _, c := range append(testData,
		keyvalue{[]byte{255, 255, 255}, nil},
		keyvalue{[]byte{1, 2}, nil},
	) {
		v, err := sm.Get(c.key)
		if err != nil {
			t.Errorf("Get got %v; want nil", err)
		}
		if bytes.Compare(v, c.value) != 0 {
			t.Errorf("Get got %v; want %v", v, c.value)
		}
	}
}

func TestPRStream(t *testing.T) {
	sm := NewSortedMap()
	// test valid input.
	ch := make(chan [3][]byte, 1000) // buffer
	go func() {
		for _, kv := range testData {
			ch <- [3][]byte{[]byte{OPPUT}, kv.key, kv.value}
		}
		close(ch)
	}()
	err := sm.PRStream(ch)
	if err != nil {
		t.Errorf("PutStream got %v; want nil", err)
	}
	length, _ := sm.Len()
	if length != uint64(len(testData)) {
		t.Errorf("PutStream got %v; want %v", length, len(testData))
	}
	// test invalid input for put.
	ch = make(chan [3][]byte)
	go func() {
		ch <- [3][]byte{[]byte{OPPUT}, nil, nil}
		close(ch)
	}()
	err = sm.PRStream(ch)
	if err == nil {
		t.Errorf("PutStream got nil; want error")
	}
	// test invalid input for remove.
	ch = make(chan [3][]byte)
	go func() {
		ch <- [3][]byte{[]byte{OPREMOVE}, nil, nil}
		close(ch)
	}()
	err = sm.PRStream(ch)
	if err == nil {
		t.Errorf("PutStream got nil; want error")
	}
	// test invalid operation.
	ch = make(chan [3][]byte)
	go func() {
		ch <- [3][]byte{[]byte{255}, nil, nil}
		close(ch)
	}()
	err = sm.PRStream(ch)
	if err == nil {
		t.Errorf("PutStream got nil; want error")
	}
}

func TestIterator(t *testing.T) {
	sm := NewSortedMap()
	for _, kv := range testData {
		sm.Put(kv.key, kv.value)
	}
	it, err := sm.Iterator(nil, true, 0, ^uint64(0))
	if err != nil {
		t.Errorf("Iterator got %v; want nil", err)
	}
	count := 0
	for {
		k, v, err := it()
		if k == nil && v == nil && err == nil {
			break // done
		}
		if err != nil {
			t.Errorf("Iterator got %v; want nil", err)
			break
		}
		count++
	}
	if count != len(testData) {
		t.Errorf("Iterator got %v; want %v", count, len(testData))
	}
	k, v, err := it() // continue to call it() after done.
	if !(k == nil && v == nil && err == nil) {
		t.Errorf("Iterator got (%v, %v, %v); want (nil, nil, nil)", k, v, err)
	}
}

func TestTransaction(t *testing.T) {
	sm := NewSortedMap()
	err := sm.Transaction(nil)
	if err == nil {
		t.Errorf("Transaction got nil; want error")
	}
	err = sm.Transaction([][3][]byte{})
	if err == nil {
		t.Errorf("Transaction got nil; want error")
	}
	err = sm.Transaction([][3][]byte{[3][]byte{[]byte{OPPUT}, []byte{1}, nil}})
	if err != nil {
		t.Errorf("Transaction got %v; want nil", err)
	}
	err = sm.Transaction([][3][]byte{[3][]byte{[]byte{OPREMOVE}, []byte{1}, nil}})
	if err != nil {
		t.Errorf("Transaction got %v; want nil", err)
	}
	err = sm.Transaction([][3][]byte{[3][]byte{[]byte{255}, []byte{1}, nil}})
	if err == nil {
		t.Errorf("Transaction got nil; want error")
	}
}

func BenchmarkPut(b *testing.B) {
	sm := NewSortedMap()
	for i := 0; i < b.N; i++ {
		sm.Put([]byte(strconv.Itoa(i)), nil)
	}
}
