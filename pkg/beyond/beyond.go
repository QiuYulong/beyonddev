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
		length := m.Len()
		msg.WriteString(n + " : length=" + strconv.FormatUint(length, 10) + "\n")
	}
	return msg.String()
}
