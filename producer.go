package kfkp

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

type producer struct {
	// producerID string
	writer   *kafka.Writer
	lastUsed time.Time
}

func (pd *producer) SendMessage(key, value []byte) error {
	err := pd.writer.WriteMessages(context.Background(), kafka.Message{
		Key:   key,
		Value: value,
	})

	if err != nil {
		return err
	}

	return nil
}

func (pd *producer) closeProducer() error {
	err := pd.writer.Close()
	if err != nil {
		return err
	}
	return nil
}
