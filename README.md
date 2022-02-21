# go-queue

> Kafka, Beanstalkd Pub/Sub framework. reference: https://github.com/zeromicro/go-queue

## dq

High available beanstalkd.

### consumer example

```go
package main

import (
	"context"
	"github.com/chenquan/go-queue/dq"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

func main() {
	var c service.ServiceConf
	conf.MustLoad("config.yaml", &c)

	consumer := dq.NewConsumer(dq.Conf{
		ServiceConf: c,
		Beanstalks: []dq.Beanstalk{
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
	}, dq.WithHandle(func(ctx context.Context, body []byte) {
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
	"strconv"
	"time"

	"github.com/chenquan/go-queue/dq"
)

func main() {
	producer := dq.NewProducer([]dq.Beanstalk{
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

## kq

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
	"github.com/zeromicro/go-zero/core/logx"

	"github.com/chenquan/go-queue/kq"
	"github.com/zeromicro/go-zero/core/conf"
)

func main() {
	var c kq.Conf
	conf.MustLoad("config.yaml", &c)

	q := kq.MustNewQueue(c, kq.WithHandle(func(ctx context.Context, k, v []byte) error {
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
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/chenquan/go-queue/kq"
	"github.com/zeromicro/go-zero/core/cmdline"
)

type message struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	Payload string `json:"message"`
}

func main() {
	pusher := kq.NewPusher([]string{
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