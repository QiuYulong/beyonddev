package beyond

import (
	"beyond/pkg/ds"
	"bytes"
	"strconv"
	"sync"
)

// Beyond singleton.
type Beyond struct {
	smmap map[string]*ds.SortedMap
	mutex sync.Mutex
}

var defaultBeyond *Beyond

// init beyondInstance.
func init() {
	defaultBeyond = NewBeyond()
}

// GetBeyond get the singleton of beyond.
func GetBeyond() *Beyond {
	return defaultBeyond
}

// NewBeyond returns new instance of Beyond.
func NewBeyond() *Beyond {
	return &Beyond{
		smmap: make(map[string]*ds.SortedMap),
	}
}

// GetStatus returns status of Beyond.
func (b *Beyond) GetStatus() string {
	var msg bytes.Buffer
	msg.WriteString("[SortedMap]\n")
	for n, m := range b.smmap {
		length := m.Len()
		msg.WriteString(n + " : length=" + strconv.FormatUint(length, 10) + "\n")
	}
	return msg.String()
}
