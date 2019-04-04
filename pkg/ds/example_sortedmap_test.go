package ds

import "fmt"

func ExampleString() {
	e := sortedMapEntity{
		[]byte{1, 2, 3, 4},
		[]byte{5, 6, 7, 8},
	}
	fmt.Println(e.String())
	// Output:
	// ([1 2 3 4],[5 6 7 8])
}
