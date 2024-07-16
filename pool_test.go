package kfkp

import (
	"context"
	"fmt"
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestGetAllKafkaTopic(t *testing.T) {
	testMap := make(map[string]struct{})
	getAllKafkaTopic("localhost:9092", testMap)

	for k := range testMap {
		fmt.Println(k)
	}
}

func TestCreateProducer(t *testing.T) {
	p, err := createProducer()

	if err != nil {
		fmt.Print("err")
	}

	err = p.writer.WriteMessages(context.Background(), kafka.Message{
		Value: []byte("123"),
	})

	if err != nil {
		fmt.Println(err)
	}
}
