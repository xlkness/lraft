package lraft

import "fmt"

func assert(current, expected any) {
	if current != expected {
		panic(fmt.Errorf("assert current:%v, expected:%v", current, expected))
	}
}
