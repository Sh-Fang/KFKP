package kfkp

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

var (
	kafkaTopics map[string]struct{}
	wq          = newWaitQueue()
)

type poolInfo struct {
	initCapacity  int32
	maxCapacity   int32
	maxIdle       int32
	brokerAddress string
	topic         string
	requiredAcks  kafka.RequiredAcks
	async         bool

	running int32
}

type Pool struct {
	poolInfo
	producers []*producer
	mu        sync.Mutex
	isLocked  bool
}

func (p *Pool) createProducer() (*producer, error) {

	pd := &producer{}

	// uid, err := generateSonyflakeID()
	// if err != nil {
	// 	return nil, err
	// }

	// pd.producerID = uid

	pd.writer = &kafka.Writer{
		Addr:         kafka.TCP(p.brokerAddress),
		Topic:        p.topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: p.requiredAcks,
		Async:        p.async,
	}

	return pd, nil
}

func (p *Pool) addProducer() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	pd, err := p.createProducer()
	if err != nil {
		return err
	}

	p.producers = append(p.producers, pd)

	return nil
}

func (p *Pool) initialize() error {
	// initialize slice
	p.producers = make([]*producer, 0)

	// get all existed topic
	var err error
	kafkaTopics, err = getKafkaTopics(p.brokerAddress)
	if err != nil {
		return err
	}

	// the pool can be created only if the topic exists
	_, exists := kafkaTopics[p.topic]
	if !exists {
		return fmt.Errorf("topic: %s , has not been created", p.topic)
	}

	// add initial producers
	var i int32
	for i = 0; i < p.poolInfo.initCapacity; i++ {
		err := p.addProducer()
		if err != nil {
			return err
		}
	}

	return nil
}

func NewPool(opts ...Option) (*Pool, error) {
	// default poolInfo
	poolInfo := &poolInfo{
		initCapacity:  10,
		maxCapacity:   100,
		maxIdle:       50,
		brokerAddress: "localhost:9092",
		topic:         "bus_1",
		requiredAcks:  kafka.RequireNone,
		async:         false,
	}

	// if there are any options, ignore the default options and apply those options
	for _, opt := range opts {
		opt(poolInfo)
	}

	// according to the poolInfo, create the pool
	p := &Pool{poolInfo: *poolInfo}

	err := p.initialize()
	if err != nil {
		return nil, err
	}

	return p, nil
}

func (p *Pool) GetConn() (*producer, error) {
	p.mu.Lock()

retry:
	if len(p.producers) <= 0 {
		// if no available producer, wait
		ch := make(chan struct{})

		wq.mu.Lock()
		wq.waiters = append(wq.waiters, ch)
		wq.mu.Unlock()

		p.mu.Unlock() // release the lock

		// wait for a connection to be available
		select {
		case <-ch:
			// if been woken up by NotifyOne, return a Connection
			p.mu.Lock() // re-acquire the lock

			// double check
			if len(p.producers) > 0 {
				pd := p.producers[0]
				p.producers = p.producers[1:]
				p.running++

				p.mu.Unlock()
				return pd, nil

			} else {
				goto retry
			}
		case <-time.After(100 * time.Millisecond):
			// if timeout, remove current channel from waiters, and create a new producer to return
			wq.mu.Lock()

			// remove current channel from waiters.
			// TODO: using O(1) way with hash or O(log n) way with binary-search way
			for i, waiter := range wq.waiters {
				if waiter == ch {
					wq.waiters = append(wq.waiters[:i], wq.waiters[i+1:]...)
					break
				}
			}

			wq.mu.Unlock()

			pd, err := p.createProducer()
			if err != nil {
				return nil, err
			}

			return pd, nil
		}

	} else {
		pd := p.producers[0]
		p.producers = p.producers[1:]

		p.running++

		p.mu.Unlock()

		return pd, nil
	}
}

func (p *Pool) PutConn(pd *producer) error {
	wq.mu.Lock()
	defer wq.mu.Unlock()

	if len(wq.waiters) > 0 {
		// if there are waiting producers, wake up one
		ch := wq.waiters[0]
		wq.waiters = wq.waiters[1:]

		p.mu.Lock()
		p.producers = append(p.producers, pd) // add the producer to the pool
		p.mu.Unlock()

		close(ch) // wake up one

		return nil
	}

	p.mu.Lock()
	p.producers = append(p.producers, pd)
	p.mu.Unlock()

	return nil
}

func (p *Pool) ClosePool() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// close each producer instance
	for _, pd := range p.producers {
		err := pd.closeProducer()
		if err != nil {
			return err
		}
	}

	// waiting for gc process
	p.producers = nil

	return nil
}

func (p *Pool) addRunning(delta int) {
	atomic.AddInt32(&p.running, int32(delta))
}

func (p *Pool) GetRunning() int {
	return int(atomic.LoadInt32(&p.running))
}
