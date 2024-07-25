package kfkp

import (
	"fmt"
	"log"
	"strconv"
	"sync"
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
	// using default config
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

	// using consumer config
	pool, err = NewPool(
		WithInitCapacity(100),
		WithMaxCapacity(1000),
		WithMaxIdle(100),
		WithBrokerAddress("localhost:9092"),
		WithTopic("bus_1"),
	)
	if err != nil {
		log.Fatal(err)
	}

	pd, err = pool.GetConn()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(pool.GetIdling())

	pd.SendMessage([]byte("312"), []byte("hello"))

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

func TestConcurrentGetAndPut(t *testing.T) {
	// using consumer config
	pool, err := NewPool(
		WithInitCapacity(100),
		WithMaxCapacity(100),
		WithMaxIdle(20),
		WithBrokerAddress("localhost:9092"),
		WithTopic("bus_1"),
	)
	if err != nil {
		log.Fatal(err)
	}

	wg := sync.WaitGroup{}

	for i := 0; i < 100; i++ {
		go func(i int) {
			wg.Add(1)
			defer wg.Done()

			pd, err := pool.GetConn()
			if err != nil {
				log.Fatal(err)
			}

			err = pd.SendMessage([]byte("123"), []byte(strconv.Itoa(i)))
			if err != nil {
				log.Fatal(err)
			}

			// time.Sleep(100 * time.Millisecond)

			err = pool.PutConn(pd)
			if err != nil {
				log.Fatal(err)
			}

		}(i)
	}

	wg.Wait()

	err = pool.ClosePool()
	if err != nil {
		log.Fatal(err)
	}

}
