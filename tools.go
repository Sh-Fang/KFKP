package kfkp

import (
	"errors"
	"strconv"

	"github.com/segmentio/kafka-go"
	"github.com/sony/sonyflake"
)

func getKafkaTopics(brokerAddress string) (map[string]struct{}, error) {
	m := make(map[string]struct{})

	conn, err := kafka.Dial("tcp", brokerAddress)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return nil, err
	}

	for _, p := range partitions {
		m[p.Topic] = struct{}{}
	}

	return m, nil
}

func generateSonyflakeID() (string, error) {
	var st sonyflake.Settings

	sf, err := sonyflake.New(st)
	if err != nil {
		return "", errors.New("sonyflake creation failed")
	}

	uid, err := sf.NextID()
	if err != nil {
		return "", errors.New("uid creation failed")
	}

	return strconv.FormatUint(uid, 10), nil
}
