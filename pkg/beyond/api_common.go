package beyond

// List list all data structures' name & type in beyond.
func (b *Beyond) List() [][2]string {
	var ds [][2]string
	// list sorted maps.
	for name := range b.smmap {
		ds = append(ds, [2]string{name, "SortedMap"})
	}
	return ds
}
