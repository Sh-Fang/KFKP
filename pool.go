package kfkp

import (
	"log"
	"sync"
	"sync/atomic"

	"github.com/segmentio/kafka-go"
)

var (
	topic2id map[string]struct{}
)

type poolInfo struct {
	maxCapacity  int32
	initCapacity int32

	running int32
	waiting int32

	idle    int32
	maxIdle int32

	kafkaBrokerAddress string
}

type producer struct {
	producerID int
	writer     *kafka.Writer
}

type Pool struct {
	poolInfo
	producers []*producer
	mu        sync.Mutex
}

func getAllKafkaTopic(brokerAddress string, m map[string]struct{}) {
	conn, err := kafka.Dial("tcp", brokerAddress)
	if err != nil {
		panic(err.Error())
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		panic(err.Error())
	}

	for _, p := range partitions {
		m[p.Topic] = struct{}{}
	}
}

func createProducer() (*producer, error) {
	p := &producer{}
	p.producerID = 1

	p.writer = &kafka.Writer{
		Addr:         kafka.TCP("localhost:9092"),
		Topic:        "bus_1",
		Balancer:     &kafka.LeastBytes{}, // 指定分区的balancer模式为最小字节分布
		RequiredAcks: kafka.RequireOne,    // ack模式
	}

	return p, nil

}

func (p *Pool) initialize() {
	p.mu.Lock()
	defer p.mu.Unlock()

	getAllKafkaTopic(p.kafkaBrokerAddress, topic2id)

	p.producers = make([]*producer, p.poolInfo.initCapacity)

	var i int32
	for i = 0; i < p.poolInfo.initCapacity; i++ {
		producer, err := createProducer()

		if err != nil {
			log.Printf("Failed to create connection: %v\n", err)
			continue
		}

		p.producers = append(p.producers, producer)
		p.poolInfo.idle++
	}

}

func NewPool() *Pool {
	p := &Pool{poolInfo: poolInfo{
		maxCapacity:  100,
		initCapacity: 10,
		maxIdle:      50,
	}}

	p.initialize()

	return p
}

func (p *Pool) GetConn() {

}

func (p *Pool) PutConn() {

}

func (p *Pool) GetRunning() int {
	return int(atomic.LoadInt32(&p.running))
}

func (p *Pool) GetIdle() int {
	return int(atomic.LoadInt32(&p.idle))
}

func (p *Pool) GetWaiting() int {
	return int(atomic.LoadInt32(&p.waiting))
}
