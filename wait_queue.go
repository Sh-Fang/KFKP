package kfkp

import "sync"

type WaitQueue struct {
	mu       sync.Mutex
	capacity int
	waiters  []chan struct{}
}

// newWaitQueue create a new wait queue
func newWaitQueue() *WaitQueue {
	return &WaitQueue{
		waiters: make([]chan struct{}, 0),
	}
}
