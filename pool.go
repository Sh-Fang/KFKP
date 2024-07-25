package kfkp

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/segmentio/kafka-go"
)

var (
	kafkaTopics map[string]struct{}
)

type poolInfo struct {
	maxCapacity  int32
	initCapacity int32
	maxIdle      int32

	running int32
	waiting int32
	idling  int32

	kafkaBrokerAddress string
	kafkaTopic         string
}

type producer struct {
	producerID string
	writer     *kafka.Writer
}

type Pool struct {
	poolInfo
	producers []*producer
	mu        sync.Mutex
}

func createProducer(brokerAddress string, topic string) (*producer, error) {
	pd := &producer{}

	uid, err := generateSonyflakeID()
	if err != nil {
		return nil, err
	}

	pd.producerID = uid

	pd.writer = &kafka.Writer{
		Addr:         kafka.TCP(brokerAddress),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{}, // 指定分区的balancer模式为最小字节分布
		RequiredAcks: kafka.RequireOne,    // ack模式
	}

	return pd, nil

}

func (pd *producer) SendMessage(msg []byte) error {
	err := pd.writer.WriteMessages(context.Background(), kafka.Message{
		Value: msg,
	})

	if err != nil {
		return err
	}

	return nil
}

func (p *Pool) initialize() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var err error
	kafkaTopics, err = getKafkaTopics(p.kafkaBrokerAddress)
	if err != nil {
		return err
	}

	_, exists := kafkaTopics[p.kafkaTopic]
	if !exists {
		return fmt.Errorf("topic: %s , has not been created", p.kafkaTopic)
	}

	p.producers = make([]*producer, p.poolInfo.initCapacity)

	var i int32
	for i = 0; i < p.poolInfo.initCapacity; i++ {
		producer, err := createProducer(p.kafkaBrokerAddress, p.kafkaTopic)
		if err != nil {
			return err
		}

		p.producers = append(p.producers, producer)
		p.poolInfo.idling++
	}

	return nil
}

func NewPool() (*Pool, error) {
	p := &Pool{poolInfo: poolInfo{
		maxCapacity:  100,
		initCapacity: 10,
		maxIdle:      50,

		kafkaBrokerAddress: "localhost:9092",
		kafkaTopic:         "bus_1",
	}}

	err := p.initialize()
	if err != nil {
		return nil, err
	}

	return p, nil
}

func (p *Pool) GetConn() {

}

func (p *Pool) PutConn() {

}

func (p *Pool) GetRunning() int {
	return int(atomic.LoadInt32(&p.running))
}

func (p *Pool) GetIdle() int {
	return int(atomic.LoadInt32(&p.idling))
}

func (p *Pool) GetWaiting() int {
	return int(atomic.LoadInt32(&p.waiting))
}
