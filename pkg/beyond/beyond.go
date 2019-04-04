package beyond

import (
	"sync"
)

type beyond struct {
}

var beyondInstance *beyond
var once sync.Once

// GetInstance get the singleton of beyond.
func GetInstance() *beyond {
	once.Do(func(){
		beyondInstance = &beyond{}
	})
	return beyondInstance
}