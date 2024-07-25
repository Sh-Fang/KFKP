package kfkp

import "sync"

type WaitQueue struct {
	mu      sync.Mutex
	waiters []chan struct{}
}

// newWaitQueue create a new wait queue
func newWaitQueue() *WaitQueue {
	return &WaitQueue{
		waiters: make([]chan struct{}, 0),
	}
}

// wait make the current goroutine wait until NotifyOne is called.
func (wq *WaitQueue) wait() {
	ch := make(chan struct{})

	wq.mu.Lock()
	wq.waiters = append(wq.waiters, ch)
	wq.mu.Unlock()

	<-ch
}

// notifyOne wake up one waiter
func (wq *WaitQueue) notifyOne() {
	wq.mu.Lock()
	defer wq.mu.Unlock()

	if len(wq.waiters) > 0 {
		ch := wq.waiters[0]

		wq.waiters = wq.waiters[1:]

		close(ch)
	}
}
