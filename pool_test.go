package kfkp

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"testing"

	"github.com/segmentio/kafka-go"
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

func TestConcurrentGetAndPut(t *testing.T) {
	// using consumer config
	pool, err := NewPool(
		WithInitCapacity(10),
		WithMaxCapacity(100),
		WithMaxIdle(10),
		WithBrokerAddress("localhost:9092"),
		WithTopic("bus_1"),
		WithRequiredAcks(kafka.RequireNone),
		WithAsync(false),
	)
	if err != nil {
		log.Fatal(err)
	}

	wg := sync.WaitGroup{}

	for i := 0; i < 10000; i++ {
		go func() {
			wg.Add(1)
			defer wg.Done()

			pd, err := pool.GetConn()
			if err != nil {
				return
			}

			if pd == nil {
				return
			}

			err = pd.SendMessage([]byte("123"), []byte(strconv.Itoa(i)))
			if err != nil {
				return
			}

			// time.Sleep(10 * time.Millisecond)

			err = pool.PutConn(pd)
			if err != nil {
				return
			}

		}()
	}

	wg.Wait()

	err = pool.ClosePool()
	if err != nil {
		log.Fatal(err)
	}

}
