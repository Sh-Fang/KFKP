package kfkp

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/segmentio/kafka-go"
)

var (
	kafkaTopics map[string]struct{}
)

type poolInfo struct {
	initCapacity  int32
	maxCapacity   int32
	maxIdle       int32
	BrokerAddress string
	Topic         string

	running int32
	waiting int32
	idling  int32
}

type Pool struct {
	poolInfo
	producers []*producer
	mu        sync.Mutex
}

func (p *Pool) addProducer() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	pd := &producer{}

	uid, err := generateSonyflakeID()
	if err != nil {
		return err
	}

	pd.producerID = uid

	pd.writer = &kafka.Writer{
		Addr:         kafka.TCP(p.BrokerAddress),
		Topic:        p.Topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		// Async:        true,
	}

	p.producers = append(p.producers, pd)
	p.poolInfo.idling++

	return nil
}

func (p *Pool) initialize() error {
	// get all existed topic
	var err error
	kafkaTopics, err = getKafkaTopics(p.BrokerAddress)
	if err != nil {
		return err
	}

	// the pool can be created only if the topic exists
	_, exists := kafkaTopics[p.Topic]
	if !exists {
		return fmt.Errorf("topic: %s , has not been created", p.Topic)
	}

	// initialize the producers slice
	p.producers = make([]*producer, 0)

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
		initCapacity: 10,

		maxCapacity: 100,
		maxIdle:     50,

		BrokerAddress: "localhost:9092",
		Topic:         "bus_1",
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
	defer p.mu.Unlock()

	pd := p.producers[0]
	p.producers = p.producers[1:]
	p.poolInfo.idling--

	return pd, nil
}

func (p *Pool) PutConn(pd *producer) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.producers = append(p.producers, pd)
	p.poolInfo.idling++

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

func (p *Pool) GetRunning() int {
	return int(atomic.LoadInt32(&p.running))
}

func (p *Pool) GetIdling() int {
	return int(atomic.LoadInt32(&p.idling))
}

func (p *Pool) GetWaiting() int {
	return int(atomic.LoadInt32(&p.waiting))
}
