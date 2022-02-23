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
