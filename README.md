# go-queue

> Kafka, Beanstalkd, Pulsar Pub/Sub framework. Reference: https://github.com/zeromicro/go-queue

## installation

```shell
go get -u github.com/chenquan/go-queue
```

## beanstalkd

High available beanstalkd.

### consumer example

config.yaml

```yaml
Name: beanstalkd
Telemetry:
  Name: beanstalkd
  Endpoint: http://localhost:14268/api/traces
  Sampler: 1.0
  Natcher: jaeger
```

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

	c.MustSetUp()

	consumer := beanstalkd.NewConsumer(beanstalkd.Conf{
		Beanstalkd: beanstalkd.Beanstalkd{
			Endpoints: []string{
				"localhost:11300",
				"localhost:11300",
			},
			Tube: "tube",
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
	producer := beanstalkd.NewProducer(
		beanstalkd.Beanstalkd{
			Tube: "tube",
			Endpoints: []string{
				"localhost:11300",
				"127.0.0.1:11300",
			},
		},
	)

	for i := 1; i < 1005; i++ {
		//_, err := producer.Delay(context.Background(), []byte(strconv.Itoa(i)), time.Second*5)
		//if err != nil {
		//	fmt.Println(err)
		//}
		_, err := producer.Push(context.Background(), nil, []byte(strconv.Itoa(i)), beanstalkd.WithDuration(time.Second*5))
		if err != nil {
			fmt.Println(err)
		}
	}
}
```

## kafka

Kafka Pub/Sub framework

### consumer example

config.yaml

```yaml
Name: kafka
Brokers:
  - 127.0.0.1:19092
  - 127.0.0.1:19092
  - 127.0.0.1:19092
Group: kafka
Topic: kafka
Offset: first
Consumers: 1

Telemetry:
  Name: kq
  Endpoint: http://localhost:14268/api/traces
  Sampler: 1.0
  Natcher: jaeger
```

example code

```go
package main

import (
	"context"
	"fmt"
	"github.com/chenquan/go-queue/kafka"
	"github.com/chenquan/go-queue/queue"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/service"

	"github.com/zeromicro/go-zero/core/conf"
)

func main() {
	var c struct {
		kafka.Conf
		service.ServiceConf
	}

	conf.MustLoad("config.yaml", &c)
	c.MustSetUp()

	q := kafka.MustNewQueue(c.Conf, queue.WithHandle(func(ctx context.Context, k, v []byte) error {
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
	}, "kafka")

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

## pulsar

Pulsar Pub/Sub framework

### consumer example

config.yaml

```yaml
Name: pulsar
Brokers:
  - 127.0.0.1:6650
Topic: pulsar
Conns: 2
Processors: 2
SubscriptionName: pulsar

Telemetry:
  Name: pulsar
  Endpoint: http://localhost:14268/api/traces
  Sampler: 1.0
  Natcher: jaeger
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
	"github.com/zeromicro/go-zero/core/service"

	"github.com/zeromicro/go-zero/core/conf"
)

func main() {
	var c struct {
		pulsar.Conf
		service.ServiceConf
	}
	conf.MustLoad("config.yaml", &c)
	c.MustSetUp()

	q := pulsar.MustNewQueue(c.Conf, queue.WithHandle(func(ctx context.Context, k, v []byte) error {
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
	}, "pulsar")

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