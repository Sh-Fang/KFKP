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

func TestNewPool(t *testing.T) {
	pool, err := NewPool()
	if err != nil {
		log.Fatal(err)
	}

	pd, err := pool.GetConn()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(pool.GetIdling())

	pd.SendMessage([]byte("123"), []byte("hello"))

	err = pool.PutConn(pd)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(pool.GetIdling())

	err = pool.ClosePool()
	if err != nil {
		log.Fatal(err)
	}
}
