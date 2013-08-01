package main

import (
	"container/ring"
)

func addToRing(r *ring.Ring, s string) *ring.Ring {
	n := ring.New(1)
	n.Value = s
	return n.Link(r)
}

func removeFromRing(r *ring.Ring, s string) *ring.Ring {
	newRing := ring.New(0)
	r.Do(func(v interface{}) {
		if v != s {
			newRing = addToRing(newRing, v.(string))
		}
	})
	return newRing
}
