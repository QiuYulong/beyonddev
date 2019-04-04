package ds

import (
	"beyond/pkg/algo/skiplist"
	"bytes"
	"errors"
	"fmt"
	"sync"
)

// operation enum.
const (
	//OPPUT operation Put
	OPPUT byte = iota
	// OPREMOVE operation Remove
	OPREMOVE
)

/*
sorted map is a map structure where skiplist algorithm is used underneath for CRUD.
*/

// sortedMapEntity is a <key,value> pair used by sortedmap internally.
// the comparison of key is to compare two byte array by lexicographically order.
// it implements the skiplist.IValue interface.
type sortedMapEntity struct {
	key   []byte
	value []byte
}

// CompareTo implements skiplist.Value interface.
// return 0 if e==other, -1 if e<other, else 1.
func (s *sortedMapEntity) CompareTo(other interface{}) int {
	return bytes.Compare(s.key, other.(*sortedMapEntity).key)
}

// String implements fmt.Stringer interface.
func (s *sortedMapEntity) String() string {
	return fmt.Sprintf("(%v,%v)", s.key, s.value)
}

// SortedMap is a map data struct whose entities are sorted by key.
// it is implemented based on SkipList algorithm.
// it implements the ISortedMap interface.
type SortedMap struct {
	sl    *skiplist.SkipList
	rwmux sync.RWMutex
}

// NewSortedMap create one ready to use SortedMap.
// sorted map is a collection of ordered key-value pairs.
// type of key, value is []byte. so it can be used for any data type that can be serialized to []byte.
// for example string type, usigned integer type(convert usigned integer to []byte firstly), []byte type, etc.
// order is compared by key's byte slices lexicographically.
// key must not be nil. put nil key into sorted map will cause error.
// value can be nil. so it can be used as SortedSet (a map without nil value).
func NewSortedMap() *SortedMap {
	var sm SortedMap
	sm.sl = skiplist.NewSkipList()
	return &sm
}

// Len returns the total number of entities in SortedMap.
func (s *SortedMap) Len() (uint64, error) {
	return s.sl.Len(), nil
}

// Put or replace(if exists) key-value in sorted map.
// if exists, replace and return previous value. else return nil.
func (s *SortedMap) Put(key []byte, value []byte) ([]byte, error) {
	s.rwmux.Lock()
	defer s.rwmux.Unlock()
	return s.put(key, value, true)
}

func (s *SortedMap) put(key []byte, value []byte, replace bool) ([]byte, error) {
	if key == nil || len(key) == 0 {
		return nil, errors.New("sorted map put failed, key must not be empty")
	}
	oldval := s.sl.Put(&sortedMapEntity{key: key, value: value}, replace)
	if valEntity, ok := oldval.(*sortedMapEntity); ok {
		return valEntity.value, nil
	}
	return nil, nil
}

// Remove removes key-value from sorted map by key.
// if not exists, do nothing and return nil.
// if exists, remove then return value.
func (s *SortedMap) Remove(key []byte) ([]byte, error) {
	if key == nil || len(key) == 0 {
		return nil, errors.New("sorted map Remove failed, key must not be empty")
	}
	s.rwmux.Lock()
	defer s.rwmux.Unlock()
	return s.remove(key)
}

// remove without lock. so it can be used by transaction.
func (s *SortedMap) remove(key []byte) ([]byte, error) {
	val := s.sl.Remove(&sortedMapEntity{key: key})
	if val == nil {
		return nil, nil
	}
	return val.(*sortedMapEntity).value, nil
	// if assertion fail, let it panic, don't hide it. normally, this failing should never happen.
	// if happen, that means invalid data is inserted from somewhere.
}

// Get returns the value whose key is equal to given key. nil if not exists.
// edge case: value is nil. (so nil result means either a nil value or not exists).
func (s *SortedMap) Get(key []byte) ([]byte, error) {
	if key == nil || len(key) == 0 {
		return nil, errors.New("sorted map Get failed, key must not be empty")
	}
	val := s.sl.Iterator(&sortedMapEntity{key: key}, true, 0, 1)()
	if val == nil {
		return nil, nil
	}
	valEntity := val.(*sortedMapEntity)
	if bytes.Compare(key, valEntity.key) == 0 {
		return valEntity.value, nil
	}
	return nil, nil
}

// PRStream put the data in channel into sortedmap.
// the sender of channel need to close the channel when no more data.
func (s *SortedMap) PRStream(ch chan [3][]byte) error {
	for okv := range ch {
		switch op := okv[0][0]; op {
		case OPPUT:
			if _, err := s.Put(okv[1], okv[2]); err != nil {
				return err
			}
		case OPREMOVE:
			if _, err := s.Remove(okv[1]); err != nil {
				return err
			}
		default:
			return errors.New("unknown operation")
		}
	}
	return nil
}

// Iterator returns one iterator of sorted map.
// fromKey: the position where the iterator start, the first value is the value cloest to fromKey in the traversal side.
//          (traversal side: bigger or equal side if forward is true, smaller or equal side if forward is false)
// forward: if true, traverse from smallest to biggest; else from biggest to smallest.
// offset: skip number of offset key-values before output.
// limit: output total number of limit key-values.
// iter function returns tuple (key, value, nil) per call if next exists.
// returns tuple (nil, nil, nil) if next NOT exists.
func (s *SortedMap) Iterator(fromKey []byte, forward bool, offset uint64, limit uint64) (func() ([]byte, []byte, error), error) {
	s.rwmux.RLock()
	it := s.sl.Iterator(&sortedMapEntity{key: fromKey}, forward, offset, limit)
	s.rwmux.RUnlock()
	done := false
	return func() ([]byte, []byte, error) {
		if done {
			return nil, nil, nil
		}
		ve := it()
		if ve == nil {
			done = true
			return nil, nil, nil
		}
		veEntity := ve.(*sortedMapEntity) // if assertion fail, let it panic. don't hide it. failing should never happen.
		return veEntity.key, veEntity.value, nil
	}, nil
}

// Transaction of operations. Transaction is executed atomically.
func (s *SortedMap) Transaction(ops [][3][]byte) error {
	if ops == nil || len(ops) == 0 {
		return errors.New("Transaction ops must not be empty")
	}
	s.rwmux.Lock()
	defer s.rwmux.Unlock()
	for _, op := range ops {
		switch opName := op[0][0]; opName {
		case OPPUT:
			s.put(op[1], op[2], true)
		case OPREMOVE:
			s.remove(op[1])
		default:
			return fmt.Errorf("Transaction Unknown operation '%v'", opName)
		}
	}
	return nil
}
