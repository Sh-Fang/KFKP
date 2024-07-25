package kfkp

import (
	"fmt"
	"log"
	"testing"
)

func TestGetKafkaTopics(t *testing.T) {
	var testMap map[string]struct{}
	var err error
	testMap, err = getKafkaTopics("localhost:9092")
	if err != nil {
		fmt.Println(err)
	}

	for k := range testMap {
		fmt.Println(k)
	}
}

func TestCreateProducerAndSendMessage(t *testing.T) {
	pd, err := createProducer("localhost:9092", "bus_1")
	if err != nil {
		fmt.Println(err)
	}

	err = pd.SendMessage([]byte("123"))

	if err != nil {
		fmt.Println(err)
	}
}

func TestNewPool(t *testing.T) {
	pool, err := NewPool()
	if err != nil {
		log.Fatal(err)
	}

	idle := pool.GetIdle()

	fmt.Println(idle)
}
