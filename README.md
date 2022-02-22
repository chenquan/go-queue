# go-queue

> Kafka, Beanstalkd Pub/Sub framework. Reference: https://github.com/zeromicro/go-queue

## beanstalkd

High available beanstalkd.

### consumer example

```go
package main

import (
	"context"
	"github.com/chenquan/go-queue/beanstalkd"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

func main() {
	var c service.ServiceConf
	conf.MustLoad("config.yaml", &c)

	consumer := beanstalkd.NewConsumer(beanstalkd.Conf{
		ServiceConf: c,
		Beanstalks: []beanstalkd.Beanstalk{
			{
				Endpoint: "localhost:11300",
				Tube:     "tube",
			},
			{
				Endpoint: "localhost:11300",
				Tube:     "tube",
			},
		},
		Redis: redis.RedisConf{
			Host: "localhost:6379",
			Type: redis.NodeType,
		},
	}, beanstalkd.WithHandle(func(ctx context.Context, body []byte) {
		logx.WithContext(ctx).Info(string(body))

	}))
	defer consumer.Stop()
	consumer.Start()

}


```

### producer example

```go
package main

import (
	"context"
	"fmt"
	"github.com/chenquan/go-queue/beanstalkd"
	"strconv"
	"time"
)

func main() {
	producer := beanstalkd.NewProducer([]beanstalkd.Beanstalk{
		{
			Endpoint: "localhost:11300",
			Tube:     "tube",
		},
		{
			Endpoint: "localhost:11300",
			Tube:     "tube",
		},
	})
	for i := 1000; i < 1005; i++ {
		_, err := producer.Delay(context.Background(), []byte(strconv.Itoa(i)), time.Second*5)
		if err != nil {
			fmt.Println(err)
		}
	}
}


```

## kafka

Kafka Pub/Sub framework

### consumer example

config.json

```yaml
Name: kq
Brokers:
  - 127.0.0.1:19092
  - 127.0.0.1:19092
  - 127.0.0.1:19092
Group: adhoc
Topic: kq
Offset: first
Consumers: 1
```

example code

```go
package main

import (
	"context"
	"fmt"
	"github.com/chenquan/go-queue/kafka"
	"github.com/zeromicro/go-zero/core/logx"

	"github.com/zeromicro/go-zero/core/conf"
)

func main() {
	var c kafka.Conf
	conf.MustLoad("config.yaml", &c)

	q := kafka.MustNewQueue(c, kafka.WithHandle(func(ctx context.Context, k, v []byte) error {
		logx.WithContext(ctx).Info(fmt.Sprintf("=> %s\n", v))
		return nil
	}))
	defer q.Stop()
	q.Start()
}


```

### producer example

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/chenquan/go-queue/kafka"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/zeromicro/go-zero/core/cmdline"
)

type message struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	Payload string `json:"message"`
}

func main() {
	pusher := kafka.NewPusher([]string{
		"127.0.0.1:19092",
		"127.0.0.1:19092",
		"127.0.0.1:19092",
	}, "kq")

	ticker := time.NewTicker(time.Millisecond)
	for round := 0; round < 3; round++ {
		<-ticker.C

		count := rand.Intn(100)
		m := message{
			Key:     strconv.FormatInt(time.Now().UnixNano(), 10),
			Value:   fmt.Sprintf("%d,%d", round, count),
			Payload: fmt.Sprintf("%d,%d", round, count),
		}
		body, err := json.Marshal(m)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(string(body))
		if err := pusher.Push(context.Background(), body); err != nil {
			log.Fatal(err)
		}
	}

	cmdline.EnterToContinue()
}


```

## pq

Pulsar Pub/Sub framework

### consumer example

config.json

```yaml
Name: pq
Brokers:
  - 127.0.0.1:6650
Topic: pq
Conns: 2
Processors: 2
SubscriptionName: pq
```

consumer code

```go
package main

import (
	"context"
	"fmt"
	"github.com/chenquan/go-queue/pulsar"
	"github.com/chenquan/go-queue/queue"
	"github.com/zeromicro/go-zero/core/logx"

	"github.com/zeromicro/go-zero/core/conf"
)

func main() {
	var c pulsar.Conf
	conf.MustLoad("config.yaml", &c)

	q := pulsar.MustNewQueue(c, queue.WithHandle(func(ctx context.Context, k, v []byte) error {
		logx.WithContext(ctx).Info(fmt.Sprintf("=> %s\n", v))
		return nil
	}))
	defer q.Stop()
	q.Start()
}

```

producer code

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/chenquan/go-queue/pulsar"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/zeromicro/go-zero/core/cmdline"
)

type message struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	Payload string `json:"message"`
}

func main() {
	pusher := pulsar.NewPusher([]string{
		"127.0.0.1:19092",
		"127.0.0.1:19092",
		"127.0.0.1:19092",
	}, "kq")

	ticker := time.NewTicker(time.Millisecond)
	for round := 0; round < 3; round++ {
		<-ticker.C

		count := rand.Intn(100)
		m := message{
			Key:     strconv.FormatInt(time.Now().UnixNano(), 10),
			Value:   fmt.Sprintf("%d,%d", round, count),
			Payload: fmt.Sprintf("%d,%d", round, count),
		}
		body, err := json.Marshal(m)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(string(body))

		if _, err := pusher.Push(context.Background(), []byte(strconv.FormatInt(time.Now().UnixNano(), 10)), body); err != nil {
			log.Fatal(err)
		}
	}

	cmdline.EnterToContinue()
}

```