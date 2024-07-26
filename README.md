# Introduction

Library `kfkp` implements a kafka producer pool, which is a high-performance Go package designed to efficiently manage Kafka producer connections.

# Features
- Dynamic Scaling: Automatically manages the creation, acquisition, release, and destruction of Kafka producer connections, with support for dynamic scaling based on demand.

- Synchronization Mechanisms: Utilizes synchronization mechanisms to handle connection contention and timeout scenarios, ensuring efficient and reliable connection management.

- Configuration Options: Implements the Options pattern to configure pool parameters, allowing fine-tuning of the connection pool behavior according to application needs.

- Timeout Queue Mechanism: Supports a timeout waiting queue mechanism to handle connection requests and timeouts effectively.

- Connection Cleanup: Periodically cleans up long-unused connections to maintain optimal performance and resource utilization.

# How to install
```GO
go get -u install "github.com/Sh-Fang/kfkp"
```

# How to use
```GO
package main

import (
	"log"
	"strconv"

	"github.com/Sh-Fang/kfkp"
	"github.com/segmentio/kafka-go"
)

func main() {
	pool, err := kfkp.NewPool(
		kfkp.WithInitCapacity(10),
		kfkp.WithMaxCapacity(100),
		kfkp.WithMaxIdle(10),
		kfkp.WithBrokerAddress("localhost:9092"),
		kfkp.WithTopic("topic"),
		kfkp.WithRequiredAcks(kafka.RequireNone),
		kfkp.WithAsync(false),
	)
	if err != nil {
		log.Fatal(err)
	}

	pd, err := pool.GetConn()
	if err != nil {
		return
	}

	err = pd.SendMessage([]byte("123"), []byte(strconv.Itoa(111)))
	if err != nil {
		return
	}

	err = pool.PutConn(pd)
	if err != nil {
		return
	}

	err = pool.ClosePool()
	if err != nil {
		log.Fatal(err)
	}
}
```

## Customize pool

You can invoke the NewPool method to instantiate a pool with options, as follows:

```GO
pool, err := kfkp.NewPool(
		kfkp.WithInitCapacity(10),
		kfkp.WithMaxCapacity(100),
		kfkp.WithMaxIdle(10),
		kfkp.WithBrokerAddress("localhost:9092"),
		kfkp.WithTopic("topic"),
		kfkp.WithRequiredAcks(kafka.RequireNone),
		kfkp.WithAsync(false),
	)
```

## Get Connection

Connection can be getting from pool by calling `pool.GetConn()`

```GO
pd, err := pool.GetConn()
```

## Put Connection

Connection can be putting back to pool by calling `pool.PutConn(pd)`

```GO
err = pool.PutConn(pd)
```

## Close pool

You can close the connection pool and release all the resources.

```GO
err = pool.ClosePool()
```

# License
The source code in kfkp is available under the [MIT License](https://github.com/Sh-Fang/kfkp/blob/main/LICENSE).