package kfkp

import "github.com/segmentio/kafka-go"

type Option func(*poolInfo)

func WithInitCapacity(size int) Option {
	return func(p *poolInfo) {
		p.initCapacity = int32(size)
	}
}

func WithMaxCapacity(size int) Option {
	return func(p *poolInfo) {
		p.maxCapacity = int32(size)
	}
}

func WithMaxIdle(size int) Option {
	return func(p *poolInfo) {
		p.maxIdle = int32(size)
	}
}

func WithBrokerAddress(brokerAddress string) Option {
	return func(p *poolInfo) {
		p.brokerAddress = brokerAddress
	}
}

func WithTopic(topic string) Option {
	return func(p *poolInfo) {
		p.topic = topic
	}
}

func WithRequiredAcks(requiredAcks kafka.RequiredAcks) Option {
	return func(p *poolInfo) {
		p.requiredAcks = requiredAcks
	}
}

func WithAsync(async bool) Option {
	return func(p *poolInfo) {
		p.async = async
	}
}
