package beyond

import (
	"beyond/pkg/ds"
	"bytes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strconv"
	"sync"
)

// Beyond singleton.
type Beyond struct {
	smmap map[string]*ds.SortedMap
	mutex sync.Mutex
}

var beyondInstance *Beyond
var once sync.Once

// GetInstance get the singleton of beyond.
func GetInstance() *Beyond {
	once.Do(func() {
		beyondInstance = &Beyond{}
		beyondInstance.smmap = make(map[string]*ds.SortedMap)
	})
	return beyondInstance
}

// GetStatus returns status of Beyond.
func (b *Beyond) GetStatus() string {
	var msg bytes.Buffer
	msg.WriteString("[SortedMap]\n")
	for n, m := range b.smmap {
		length, err := m.Len()
		if err != nil {
			length = 0
		}
		msg.WriteString(n + " : length=" + strconv.FormatUint(length, 10) + "\n")
	}
	return msg.String()
}

// CreateSortedMap create sorted map with given name.
func (b *Beyond) CreateSortedMap(name string) error {
	if _, exists := b.smmap[name]; exists {
		return status.Errorf(codes.AlreadyExists, "sorted map '%v' already exists", name)
	}
	b.smmap[name] = ds.NewSortedMap()
	return nil
}

// DropSortedMap drop sorted map with given name.
func (b *Beyond) DropSortedMap(name string) error {
	if _, exists := b.smmap[name]; exists {
		delete(b.smmap, name)
		return nil
	}
	return status.Errorf(codes.NotFound, "sorted map '%v' not found", name)
}

// GetSortedMap get sorted map with given name.
func (b *Beyond) GetSortedMap(name string) (*ds.SortedMap, error) {
	if sm, exists := b.smmap[name]; exists {
		return sm, nil
	}
	return nil, status.Errorf(codes.NotFound, "sorted map '%v' not found", name)
}
