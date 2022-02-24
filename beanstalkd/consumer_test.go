package beanstalkd

import (
	"context"
	"fmt"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"strconv"
	"testing"
	"time"
)

func TestBeanstalkd(t *testing.T) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*20)
	c := make(chan struct{})

	go startConsumer(ctx, c)
	startProducer()

	for i := 1; i < 10; i++ {
		<-c
	}

	cancelFunc()
}

func startConsumer(ctx context.Context, c chan struct{}) {
	consumer := NewConsumer(Conf{
		Beanstalkd: Beanstalkd{
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
	}, WithHandle(func(ctx context.Context, body []byte) {
		c <- struct{}{}
		fmt.Println(body)
	}))
	go func() {
		select {
		case <-ctx.Done():
			consumer.Stop()
		}
	}()
	consumer.Start()
}

func startProducer() {
	producer := NewProducer(
		Beanstalkd{
			Tube: "tube",
			Endpoints: []string{
				"localhost:11300",
				"127.0.0.1:11300",
			},
		},
	)

	for i := 0; i < 10; i++ {

		ids, err := producer.Push(context.Background(), nil, []byte(strconv.Itoa(i)), WithDuration(time.Second))
		if err != nil {
			fmt.Println(err)
		}
		if i == 2 {
			_ = producer.Revoke(context.Background(), ids.(string))
		}
	}
}
