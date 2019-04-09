package beyond

import (
	"beyond/pkg/ds"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CreateSortedMap create sorted map with given name.
func (b *Beyond) CreateSortedMap(name string) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if _, exists := b.smmap[name]; exists {
		return status.Errorf(codes.AlreadyExists, "sorted map '%v' already exists", name)
	}
	b.smmap[name] = ds.NewSortedMap()
	return nil
}

// DropSortedMap drop sorted map with given name.
func (b *Beyond) DropSortedMap(name string) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
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
