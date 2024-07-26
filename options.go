package kfkp

import (
	"time"

	"github.com/segmentio/kafka-go"
)

// Option represents the optional function.
type Option func(*poolInfo)

// WithInitCapacity sets up the init capacity of the pool.
func WithInitCapacity(size int) Option {
	return func(p *poolInfo) {
		p.initCapacity = int32(size)
	}
}

// WithMaxCapacity sets up the max capacity of the pool.
func WithMaxCapacity(size int) Option {
	return func(p *poolInfo) {
		p.maxCapacity = int32(size)
	}
}

// WithMaxIdle sets up the max idle of the pool.
func WithMaxIdle(size int) Option {
	return func(p *poolInfo) {
		p.maxIdle = int32(size)
	}
}

// WithBrokerAddress sets up the broker address of the pool.
func WithBrokerAddress(brokerAddress string) Option {
	return func(p *poolInfo) {
		p.brokerAddress = brokerAddress
	}
}

// WithTopic sets up the topic of the pool.
func WithTopic(topic string) Option {
	return func(p *poolInfo) {
		p.topic = topic
	}
}

// WithRequiredAcks sets up the required acks of the pool.
func WithRequiredAcks(requiredAcks kafka.RequiredAcks) Option {
	return func(p *poolInfo) {
		p.requiredAcks = requiredAcks
	}
}

// WithAsync sets up the async of the pool.
func WithAsync(async bool) Option {
	return func(p *poolInfo) {
		p.async = async
	}
}

// WithClearUpInterval sets up the clear up interval of the pool.
func WithClearUpInterval(d time.Duration) Option {
	return func(p *poolInfo) {
		p.clearUpInterval = d
	}
}

// WithConnLifetime sets up the conn lifetime of the pool.
func WithConnLifetime(d time.Duration) Option {
	return func(p *poolInfo) {
		p.connLifetime = d
	}
}
