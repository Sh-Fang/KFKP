package kfkp

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
		p.BrokerAddress = brokerAddress
	}
}

func WithTopic(topic string) Option {
	return func(p *poolInfo) {
		p.Topic = topic
	}
}
