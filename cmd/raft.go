package main

import (
	"fmt"
	"sort"
)

type uint64Slice []int

func (p uint64Slice) Len() int           { return len(p) }
func (p uint64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func main() {
	mis := make(uint64Slice, 0)
	for i := 1; i < 10; i++ {
		mis = append(mis, i)
	}
	sort.Sort(sort.Reverse(mis))

	fmt.Printf("%+v\n", mis)
}
