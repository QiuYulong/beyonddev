package skiplist

func ExamplePrint() {
	sl := NewSkipList()
	var values []value
	for i := 0; i < 10; i++ {
		values = append(values, value(i))
	}
	for _, v := range values {
		sl.Put(v, true)
	}
	sl.Print()
	// Output:
	// L2: 8  , total 1
	// L1: 3  4  7  8  , total 4
	// L0: 0  1  2  3  4  5  6  7  8  9  , total 10
}

func ExamplePrintValues() {
	sl := NewSkipList()
	var values []value
	for i := 0; i < 10; i++ {
		values = append(values, value(i))
	}
	for _, v := range values {
		sl.Put(v, true)
	}
	sl.PrintValues(true, 0, ^uint64(0))
	// Output:
	// 0  1  2  3  4  5  6  7  8  9
}
