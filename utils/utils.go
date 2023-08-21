package utils

import "fmt"

func Assert(current, expected any) {
	if current != expected {
		panic(fmt.Errorf("assert current:%v, expected:%v", current, expected))
	}
}
